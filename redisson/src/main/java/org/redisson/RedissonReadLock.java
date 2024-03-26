/**
 * Copyright (c) 2013-2022 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonReadLock extends RedissonLock implements RLock {

    protected RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }
    
    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        //lock {lockName}:serverId:threadId:rwlock_timeout
        return suffixName(getRawName(), getLockName(threadId)) + ":rwlock_timeout";
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return commandExecutor.syncedEval(getRawName(), LongCodec.INSTANCE, command,
                                //获取锁的mode值（读锁还是写锁 read或者write）
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                       //如果mode不存在
                                "if (mode == false) then " +
                                       //设置一个mode = read
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                                       //设置锁+可重用次数+1
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                        //设置一个 {lockName}:serverId:threadId:rwlock_timeout:1 1
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +
                                        //设置过期时间
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +
                                        //设置过期时间
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                                "end; " +
                                        //如果当前mode是读锁，或者是（写锁，但是持有写锁的线程是本次加锁线程）
                                        //注意这里判断的是ARGV[3]=serverId+threadId:write 是写锁的线程
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                                        //增加锁的可重入次数，可能是第一个线程id 次数+1，也可能是新的线程Id 次数+1，就是所有线程的可重入次数都在这里了
                                        // 返回ind = 此时的重入次数
                                        // ARGV[2]需要注意，不同线程加锁，这个值都是不一样的
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                  "local key = KEYS[2] .. ':' .. ind;" +
                                        //设置一个key = {lockName}:serverId:threadId:rwlock_timeout:重入次数 1
                                  "redis.call('set', key, 1); " +
                                        //设置过期时间
                                  "redis.call('pexpire', key, ARGV[1]); " +
                                        //重新设置锁的过期时间
                                  "local remainTime = redis.call('pttl', KEYS[1]); " +
                                  "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                                  "return nil; " +
                                "end;" +
                                "return redis.call('pttl', KEYS[1]);",
                        //如果线程A先加读锁，线程B来加读锁 读读不互斥
                        //如果线程A先加写锁，线程B来加读锁 不同线程间写读互斥
                        //如果线程A先加写锁，线程A再来加读锁，那么相同线程可以加锁成功，相同线程写读不互斥
                             //可以重入，只要加了写锁，就代表别的线程肯定加不了锁了，那本线程想重新加写锁也好，加读锁也好，都不限制

                        //KEYS[1]=锁名称 KEYS[2]={lockName}:serverId:threadId:rwlock_timeout
                        //ARGV[1]=leaseTime ARGV[2]=serverId+threadId ARGV[3]=serverId+threadId:write
                        Arrays.<Object>asList(getRawName(), getReadWriteTimeoutNamePrefix(threadId)),
                        unit.toMillis(leaseTime), getLockName(threadId), getWriteLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return commandExecutor.syncedEval(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                //获取当前持有锁的mode
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                        //mode为空，说明没有线程持有锁，那么向外发送pub消息
                    "redis.call(ARGV[3], KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                        //存在锁，但是持有锁的线程非否包含本线程
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                "if (lockExists == 0) then " +
                        //不包含 return 0
                    "return nil;" +
                "end; " +
                    //包含，那么就-1，减去可重入次数
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " + 
                "if (counter == 0) then " +
                        //如果该线程对应的重入次数=0了，那么就值删除hset里面的单个线程key
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                        //删除{lockName}:serverId:threadId:rwlock_timeout：重入次数 key，也就是删除表示第几次加锁的一个key，这个key主要用来记录超时时间
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +

                        //如果当前锁还被其他线程持有着。  =1 相当于hset里面只有mode了
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    "local maxRemainTime = -3; " +
                        //拿到里面所有的key也就是线程id
                    "local keys = redis.call('hkeys', KEYS[1]); " + 
                    "for n, key in ipairs(keys) do " +
                        //拿到各个线程对应的重入次数
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                        "if type(counter) == 'number' then " + 
                            "for i=counter, 1, -1 do " +
                                //获取重入次数那次加锁设置的过期时间
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " +
                                //获取一个最大的过期时间
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" + 
                            "end; " + 
                        "end; " + 
                    "end; " +

                        //如果过期时间还没到，那么就延期锁过期时间
                    "if maxRemainTime > 0 then " +
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "return 0; " +
                    "end;" + 

                        //如果锁的模式是写锁，那么return
                    "if mode == 'write' then " + 
                        "return 0;" + 
                    "end; " +
                "end; " +

                        //到了这里，说明锁的过期时间到了，hset里面没有线程了，或者说里面线程的最大过期时间到了
                "redis.call('del', KEYS[1]); " +
                        //向外pub
                "redis.call(ARGV[3], KEYS[2], ARGV[1]); " +
                "return 1; ",
                //KEYS[1]=lockName KEYS[2]=pub channelName KEYS[3] = {lockName}:serverId:threadId:rwlock_timeout KEYS[4]={lockName}
                Arrays.<Object>asList(getRawName(), getChannelName(), timeoutPrefix, keyPrefix),
                //ARGV[1]=0 ARGV[2]=serverId + threadId ARGV[3]=pub命令
                LockPubSub.UNLOCK_MESSAGE, getLockName(threadId), getSubscribeService().getPublishCommand());
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }
    
    @Override
    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);
        
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
                "if (counter ~= false) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        "local keys = redis.call('hkeys', KEYS[1]); " + 
                        "for n, key in ipairs(keys) do " + 
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                            "if type(counter) == 'number' then " + 
                                "for i=counter, 1, -1 do " + 
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " + 
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                    
                    "return 1; " +
                "end; " +
                "return 0;",
            Arrays.<Object>asList(getRawName(), keyPrefix),
            internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.syncedEvalWithRetry(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call(ARGV[2], KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.asList(getRawName(), getChannelName()),
                LockPubSub.UNLOCK_MESSAGE, getSubscribeService().getPublishCommand());
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "read".equals(res);
    }

}
