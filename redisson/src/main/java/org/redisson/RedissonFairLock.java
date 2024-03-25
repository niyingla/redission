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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>fair</b> locking so it guarantees an acquire order by threads.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonFairLock extends RedissonLock implements RLock {

    private final long threadWaitTime;
    private final CommandAsyncExecutor commandExecutor;
    private final String threadsQueueName;
    private final String timeoutSetName;

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name) {
        this(commandExecutor, name, 60000*5);
    }

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name, long threadWaitTime) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.threadWaitTime = threadWaitTime;
        threadsQueueName = prefixName("redisson_lock_queue", name);
        timeoutSetName = prefixName("redisson_lock_timeout", name);
    }

    @Override
    protected CompletableFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected void unsubscribe(RedissonLockEntry entry, long threadId) {
        pubSub.unsubscribe(entry, getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected CompletableFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        long wait = threadWaitTime;
        if (waitTime > 0) {
            wait = unit.toMillis(waitTime);
        }

        RFuture<Void> f = evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_VOID,
                // get the existing timeout for the thread to remove
                "local queue = redis.call('lrange', KEYS[1], 0, -1);" +
                        // find the location in the queue where the thread is
                        "local i = 1;" +
                        "while i <= #queue and queue[i] ~= ARGV[1] do " +
                        "i = i + 1;" +
                        "end;" +
                        // go to the next index which will exist after the current thread is removed
                        "i = i + 1;" +
                        // decrement the timeout for the rest of the queue after the thread being removed
                        "while i <= #queue do " +
                        "redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), queue[i]);" +
                        "i = i + 1;" +
                        "end;" +
                        // remove the thread from the queue and timeouts set
                        "redis.call('zrem', KEYS[2], ARGV[1]);" +
                        "redis.call('lrem', KEYS[1], 0, ARGV[1]);",
                Arrays.asList(threadsQueueName, timeoutSetName),
                getLockName(threadId), wait);
        return f.toCompletableFuture();
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        long wait = threadWaitTime;
        if (waitTime > 0) {
            wait = unit.toMillis(waitTime);
        }

        long currentTime = System.currentTimeMillis();
        if (command == RedisCommands.EVAL_NULL_BOOLEAN) {
            return commandExecutor.syncedEval(getRawName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    "while true do " +
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                        "if firstThreadId2 == false then " +
                            "break;" +
                        "end;" +
                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                        "if timeout <= tonumber(ARGV[3]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            "redis.call('lpop', KEYS[2]);" +
                        "else " +
                            "break;" +
                        "end;" +
                    "end;" +

                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                        "redis.call('lpop', KEYS[2]);" +
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                        "for i = 1, #keys, 1 do " +
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i]);" +
                        "end;" +

                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +
                    "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +
                    "return 1;",
                    Arrays.asList(getRawName(), threadsQueueName, timeoutSetName),
                    unit.toMillis(leaseTime), getLockName(threadId), currentTime, wait);
        }

        if (command == RedisCommands.EVAL_LONG) {
            return commandExecutor.syncedEval(getRawName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    "while true do " +
                            //list里面是否有等待线程存在
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                        "if firstThreadId2 == false then " +
                            //没有，跳出循环
                            "break;" +
                        "end;" +
                            //如果存在等待线程，就去zset里面获取这个线程的超时时间
                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                            //超时时间小于当前时间，说明这个线程已经超时了
                        "if timeout <= tonumber(ARGV[4]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            //超时了，就从队列和超时时间zset里面删除这个线程
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            //删除队列里面的线程
                            "redis.call('lpop', KEYS[2]);" +
                        "else " +
                            "break;" +
                        "end;" +
                    "end;" +

                    // check if the lock can be acquired now
                    //检查锁是否可以被当前线程获取
                    "if (redis.call('exists', KEYS[1]) == 0) " +
                            //当前锁不存在，且等待队列为空，或者等待队列的第一个线程是当前线程
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            //或者等待队列的第一个线程是当前线程
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +

                        // remove this thread from the queue and timeout set
                            //删除等待队列里面的线程（其实是当前线程获取到了锁）
                        "redis.call('lpop', KEYS[2]);" +
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                        //获取等待队列里面的所有线程，然后遍历，减少超时时间
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                        "for i = 1, #keys, 1 do " +
                            //todo
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i]);" +
                        "end;" +

                        // acquire the lock and set the TTL for the lease
                            //获取锁，设置超时时间
                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +

                    // check if the lock is already held, and this is a re-entry
                    //检查锁是否已经被持有，这是一个重入
                    "if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2],1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +

                    // the lock cannot be acquired
                    // check if the thread is already in the queue
                    //锁不能被获取，线程是否已经在队列里面
                    //如果在队列里面，说明已经加过一次锁
                    "local timeout = redis.call('zscore', KEYS[3], ARGV[2]);" +
                    "if timeout ~= false then " +
                        // the real timeout is the timeout of the prior thread
                        // in the queue, but this is approximately correct, and
                        // avoids having to traverse the queue
                            //todo
                        "return timeout - tonumber(ARGV[3]) - tonumber(ARGV[4]);" +
                    "end;" +

                    // add the thread to the queue at the end, and set its timeout in the timeout set to the timeout of
                    // the prior thread in the queue (or the timeout of the lock if the queue is empty) plus the
                    // threadWaitTime
                    //获取队列里面最后一个线程Id
                    "local lastThreadId = redis.call('lindex', KEYS[2], -1);" +
                    "local ttl;" +
                            //线程B在等待队列，超时时间是09:30:30 线程C在09:30:00来加锁，线程C的ttl是不是要等线程B释放，所以就是09:30:30 - 09:30:00 = 30s
                            //如果list里面有线程，那么就是最后一个线程的超时时间 - 当前时间
                    "if lastThreadId ~= false and lastThreadId ~= ARGV[2] then " +
                        "ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4]);" +
                    "else " +
                            //持有的线程是线程A,它的pexpire是09:30:20 ，那么线程B在09:30:10来加锁，09:30:20 - 09:30:10 = 10s
                        "ttl = redis.call('pttl', KEYS[1]);" +
                    "end;" +
                            //计算一个timeout = ttl + 300000ms + 当前时间
                    "local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4]);" +
                    "if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then " +
                        "redis.call('rpush', KEYS[2], ARGV[2]);" +
                    "end;" +
                    "return ttl;",
                    //线程A 11:01:00获取到锁，释放锁是11:01:30。5min 11:06:31
                    // 现在11:01:10 是线程B来获取锁,那线程B的等待时间就是11:01:10 + 20s + 300s
                    // 现在11:01:20 那线程C来获取锁，那线程C的等待时间就是(11:01:10 + 20s + 300s + 300s)
                    // 11:01:30。线程A已经释放锁，线程B来获取锁了，那么线程B是list的第一个，lpop。重新计算后续线程的等待时间
                    //    线程C的等待时间就 = (11:01:10 + 20s + 300s + 300s - 300s)
                    // 11：02：00 线程A才释放锁，线程B这时候才获取到锁，lpop
                    //    线程C的等待时间就 = (11:01:10 + 20s + 300s + 300s - 300s) 线程C只需要等待 11：02：00 + 270s
                    //总结每个线程的等待时间 = 前一个等待的线程的时间+ttl（和当前时间间隔） + 300000ms + 当前时间
                    // KEYS[1] = 锁名称 KEYS[2] = redisson_lock_queue_锁名称 KEYS[3] = redisson_lock_timeout_锁名称
                    // redisson_lock_queue_锁名称：是一个list,里面存储的是第一次没有获取到锁的线程，会按照锁的获取顺序一个个的rpush到list里面
                    // redisson_lock_timeout_锁名称：是一个zset，里面存储的是等待锁线程的超时时间，记录每一个线程到什么时间没获取到锁就超时了。分数score就是超时时间
                    // ARGV[1] = 锁过期时间 ARGV[2] = 服务id + 线程id ARGV[3] = 等待时间300000ms ARGV[4]=当前时间
                    Arrays.asList(getRawName(), threadsQueueName, timeoutSetName),
                    unit.toMillis(leaseTime), getLockName(threadId), wait, currentTime);
        }

        throw new IllegalArgumentException();
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[4]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                
              + "if (redis.call('exists', KEYS[1]) == 0) then " +
                        //当前锁不存在，获取等待队列的第一个线程
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                    "if nextThreadId ~= false then " +
                        //不为null，发布消息。
                        "redis.call(ARGV[5], KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " +
                    "return 1; " +
                "end;" +
                        //当前持有锁是否是本线程，不是直接返回
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                    "return nil;" +
                "end; " +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        //可重入次数-1
                "if (counter > 0) then " +
                        //如果可重入次数-1后大于0，那么就延长超时时间
                    "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                    "return 0; " +
                "end; " +

                        //否则，删除锁
                "redis.call('del', KEYS[1]); " +
                "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                "if nextThreadId ~= false then " +
                    "redis.call(ARGV[5], KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                "end; " +
                "return 1; ",
                Arrays.asList(getRawName(), threadsQueueName, timeoutSetName, getChannelName()),
                    LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId),
                    System.currentTimeMillis(), getSubscribeService().getPublishCommand());
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return deleteAsync(getRawName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        List<Object> keys = Arrays.asList(getRawName(), threadsQueueName, timeoutSetName);
        return super.sizeInMemoryAsync(keys);
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit, String param, String... keys) {
        return super.expireAsync(timeToLive, timeUnit, param, getRawName(), threadsQueueName, timeoutSetName);
    }

    @Override
    protected RFuture<Boolean> expireAtAsync(long timestamp, String param, String... keys) {
        return super.expireAtAsync(timestamp, param, getRawName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return clearExpireAsync(getRawName(), threadsQueueName, timeoutSetName);
    }

    
    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.syncedEvalWithRetry(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[2]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                + 
                
                "if (redis.call('del', KEYS[1]) == 1) then " + 
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call(ARGV[3], KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " + 
                    "return 1; " + 
                "end; " + 
                "return 0;",
                Arrays.asList(getRawName(), threadsQueueName, timeoutSetName, getChannelName()),
                LockPubSub.UNLOCK_MESSAGE, System.currentTimeMillis(),
                getSubscribeService().getPublishCommand());
    }

}