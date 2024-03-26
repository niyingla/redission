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
import java.util.concurrent.CompletableFuture;
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
public class RedissonWriteLock extends RedissonLock implements RLock {

    protected RedissonWriteLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }

    @Override
    protected String getLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return commandExecutor.syncedEval(getRawName(), LongCodec.INSTANCE, command,
                            //获取锁里面的mode
                            "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                            //如果mode不存在，说明第一次加写锁
                            "if (mode == false) then " +
                                    //设置写锁模式=write
                                  "redis.call('hset', KEYS[1], 'mode', 'write'); " +
                                    //设置锁持有线程
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                    //加过期时间
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                              "end; " +
                               //如果mode当前是写锁，那说明之前是加的写锁
                              "if (mode == 'write') then " +
                                    //判断当前持有锁的线程是不是本线程
                                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                                    //锁可重入次数+1
                                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                    //重新设置锁过期时间
                                      "local currentExpire = redis.call('pttl', KEYS[1]); " +
                                      "redis.call('pexpire', KEYS[1], currentExpire + ARGV[1]); " +
                                      "return nil; " +
                                  "end; " +
                                "end;" +
                                "return redis.call('pttl', KEYS[1]);",
                        //比如说线程A加写锁，线程B加写锁，互斥
                        //比如说线程A加写锁，线程A再来加写锁，可以重入，只要加了写锁，就代表别的线程肯定加不了锁了，那本线程想重新加写锁也好，加读锁也好，都不限制
                        //比如说线程A加读锁，线程A再来加写锁，互斥，加锁不成功，只要加了读锁，后面再想加写锁就加不了了，因为读读不互斥，可能好几个线程都加了读锁，没办法加写锁的时候判断是不是应该加写锁
                        //KEYS[1]=锁名称
                        //ARGV[1]=锁过期时间 ARGV[2]=serverId + threadId +:write
                        Arrays.<Object>asList(getRawName()),
                        unit.toMillis(leaseTime), getLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                //获取锁mode，锁模式
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                        //不存在锁，那么pub
                    "redis.call(ARGV[4], KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end;" +
                        //如果是写锁
                "if (mode == 'write') then " +
                        //是否持有锁线程是本线程，不是，返回null
                    "local lockExists = redis.call('hexists', KEYS[1], ARGV[3]); " +
                    "if (lockExists == 0) then " +
                        "return nil;" +
                    "else " +
                        //是本线程，做可重入次数--
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                            //如果减之后还有可重入次数，那么就延长锁过期时间
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "return 0; " +
                        "else " +
                            //否则不再持有锁，删除锁里面的线程持有
                            "redis.call('hdel', KEYS[1], ARGV[3]); " +
                            //如果hset的长度=1了，那么也就是里面只有mode了，那么就删除key
                            "if (redis.call('hlen', KEYS[1]) == 1) then " +
                                "redis.call('del', KEYS[1]); " +
                                "redis.call(ARGV[4], KEYS[2], ARGV[1]); " +
                            "else " +
                                // has unlocked read-locks
                                //如果还有别的线程在，就把线程模式改成读锁。这里会存在么？
                                // 会的，加写锁加的key = serverId + threadId +:write
                                // 加了写锁，同一线程加读锁，key = serverId + threadId，可以区分出来是读锁还是写锁
                                "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                            "end; " +
                            "return 1; "+
                        "end; " +
                    "end; " +
                "end; "
                + "return nil;",
        //KEYS[1] = lockName KEYS[2]=pub channelName
        Arrays.<Object>asList(getRawName(), getChannelName()),
        //ARGV[1]=0 ARGV[2]=锁过期时间 ARGV[3]=serverId + threadId +:write, ARGV[4]=pub命令
        LockPubSub.READ_UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId), getSubscribeService().getPublishCommand());
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        CompletionStage<Boolean> f = super.renewExpirationAsync(threadId);
        return f.thenCompose(r -> {
            if (!r) {
                RedissonReadLock lock = new RedissonReadLock(commandExecutor, getRawName());
                return lock.renewExpirationAsync(threadId);
            }
            return CompletableFuture.completedFuture(r);
        });
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.syncedEvalWithRetry(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
              "if (redis.call('hget', KEYS[1], 'mode') == 'write') then " +
                  "redis.call('del', KEYS[1]); " +
                  "redis.call(ARGV[2], KEYS[2], ARGV[1]); " +
                  "return 1; " +
              "end; " +
              "return 0; ",
              Arrays.asList(getRawName(), getChannelName()),
                LockPubSub.READ_UNLOCK_MESSAGE, getSubscribeService().getPublishCommand());
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "write".equals(res);
    }

}
