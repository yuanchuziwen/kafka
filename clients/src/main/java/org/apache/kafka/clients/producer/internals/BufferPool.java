/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 *
 * 一个在给定内存限制下保存的 ByteBuffer 池。这个类非常符合生产者的需求。特别是它具有以下特性：
 *
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 *
 * 有一个特殊的“可池化大小”，这种大小的缓冲区保存在一个空闲列表中并被回收
 *
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 *
 * 它是公平的。即所有内存都分配给等待时间最长的线程，直到它有足够的内存。这防止了当一个线程请求大块内存并需要阻塞直到多个缓冲区被释放时的饥饿或死锁。
 * </ol>
 */
public class BufferPool {

    // 等待时间传感器名称
    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";

    // 总内存
    private final long totalMemory;
    // 可池化大小
    // 这个 poolableSize 就是 batch.size 中配置的大小；即一个批次的大小
    private final int poolableSize;
    // 重入锁
    private final ReentrantLock lock;
    // 空闲缓冲区队列
    private final Deque<ByteBuffer> free;
    // 等待条件队列
    private final Deque<Condition> waiters;
    // 度量指标
    private final Metrics metrics;
    // 时间实例
    private final Time time;
    // 等待时间传感器
    private final Sensor waitTime;
    /** Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers in free * poolableSize.  */
    // 总可用内存是 nonPooledAvailableMemory 和 free 中字节缓冲区数量 * poolableSize 的总和
    private long nonPooledAvailableMemory;
    // 是否关闭
    private boolean closed;

    /*
    TODO
    理解一下 BufferPool 中的缓冲区：
    1. 一开始的时候，所有的空间都没有被用过，即 nonPooledAvailableMemory = totalMemory
    2. 当有线程第一次调用 allocate 方法时，会从 nonPooledAvailableMemory 中分配一部分空间，然后将剩余的空间放到 free 列表中
     */

    /**
     * Create a new buffer pool
     * 创建一个新的缓冲池
     *
     * @param memory The maximum amount of memory that this buffer pool can allocate
     *               这个缓冲池可以分配的最大内存量
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     *                     缓冲区大小，缓存在空闲列表中而不是释放
     * @param metrics instance of Metrics
     *                Metrics 实例
     * @param time time instance
     *             时间实例
     * @param metricGrpName logical group name for metrics
     *                      用于度量的逻辑组名称
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        // 这个构造函数只会由 KafkaProducer 调用
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        // 总内存大小就是入参 memory
        this.totalMemory = memory;
        // 一开始的时候，nonPooledAvailableMemory 就是 memory
        this.nonPooledAvailableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        // 创建一个 “bufferpool-wait-ratio” 的度量指标，用于记录缓冲池等待分配空间的时间比例
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        // 创建一个 “bufferpool-wait-time-total” 的度量指标，用于记录缓冲池等待分配空间的总时间
        MetricName totalMetricName = metrics.metricName("bufferpool-wait-time-total",
                                                   metricGrpName,
                                                   "The total time an appender waits for space allocation.");

        // 创建一个 “buffer-exhausted-records” 的度量指标，用于记录缓冲池等待分配空间的总时间
        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        MetricName bufferExhaustedRateMetricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        MetricName bufferExhaustedTotalMetricName = metrics.metricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(new Meter(bufferExhaustedRateMetricName, bufferExhaustedTotalMetricName));

        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName));
        this.closed = false;
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     * 分配给定大小的缓冲区。
     * 如果没有足够的内存并且缓冲池配置为阻塞模式，则此方法将阻塞。
     *
     * @param size The buffer size to allocate in bytes
     *             要分配的缓冲区大小（以字节为单位）
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     *                         阻塞缓冲区内存可用的最长时间（以毫秒为单位）
     * @return The buffer
     *        缓冲区
     * @throws InterruptedException If the thread is interrupted while blocked
     *                             如果线程在阻塞时被中断
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     *         如果 size 大于池控制的总内存（因此我们将永远阻塞），则抛出 IllegalArgumentException
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        // 这个方法只会在 RecordAccumulator 的 append 方法中调用
        // 如果 size 大于总内存，则抛出异常
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");
        // 分配的缓冲区
        ByteBuffer buffer = null;
        // 上锁
        this.lock.lock();
        // 如果缓冲池已关闭，则抛出异常
        if (this.closed) {
            this.lock.unlock();
            throw new KafkaException("Producer closed while allocating memory");
        }

        // 尝试分配缓冲区
        try {
            // check if we have a free buffer of the right size pooled
            // 检查是否有一个大小正确的空闲缓冲区
            // 如果本次需要 allocate 的大小等于 batch.size，且 free 列表不为空，则直接从 free 列表中取出一个 ByteBuffer
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            // 检查是否可以通过现有内存立即满足请求，或者是否需要阻塞
            // 这里先计算所有 free 队列中的空间大小
            int freeListSize = freeSize() * this.poolableSize;
            // 如果 free 队列中的空间大小 + 未分配的空间大小 >= size，则说明有足够的空间
            // 这里可以看下 nonPooledAvailableMemory 的成员变量上的注释
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request, but need to allocate the buffer
                // 我们有足够的未分配或池化的内存来立即满足请求，但需要分配缓冲区
                // 将 free 列表中的 ByteBuffer 取出来，放到 nonPooledAvailableMemory 中
                freeUp(size);
                // 削减 nonPooledAvailableMemory 的大小，代表已经分配了 size 大小的内存
                this.nonPooledAvailableMemory -= size;

                // 这个 else 说明，nonPooledAvailableMemory + freeListSize < size；即此时空间不足
            } else {
                // we are out of memory and will have to block
                // 我们内存不足，需要阻塞
                // accumulated 代表阻塞后循环中收集的内存大小
                int accumulated = 0;
                // 创建一个新的 Condition
                Condition moreMemory = this.lock.newCondition();
                try {
                    // 计算剩余的阻塞等待时间
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    // 将 moreMemory 这个 condition 添加到 waiters 队列中
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    // 一直循环，直到我们有一个缓冲区或者已经保留了足够的内存来分配一个
                    while (accumulated < size) {
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        // 触发当前线程的阻塞，此时的当前线程是 producer 线程
                        // 捋一捋：
                        // 1. producer 执行 doSend 方法，
                        // 2. doSend 方法中调用 RecordAccumulator 的 append 方法，
                        // 3. append 方法中调用 BufferPool 的 allocate 方法
                        try {
                            // 这里 await 方法返回时，可能是被唤醒并拿到了锁；也可能是单纯超时了
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            recordWaitTime(timeNs);
                        }

                        // 如果缓冲池已关闭，则抛出异常
                        if (this.closed)
                            throw new KafkaException("Producer closed while allocating memory");

                        // 如果等待时间已经过去，则抛出异常
                        if (waitingTimeElapsed) {
                            this.metrics.sensor("buffer-exhausted-records").record();
                            throw new BufferExhaustedException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                        }

                        // 更新剩余的阻塞等待时间
                        remainingTimeToBlockNs -= timeNs;

                        // check if we can satisfy this request from the free list,
                        // otherwise allocate memory
                        // 检查我们是否可以从空闲列表中满足此请求，否则分配内存
                        if (accumulated == 0 // 已累加的内存大小为 0
                                && size == this.poolableSize // size 等于 poolableSize，即要分配的内存大小等于 batch.size
                                && !this.free.isEmpty()) { // free 列表不为空
                            // just grab a buffer from the free list
                            // 从 free 列表中取出一个 ByteBuffer
                            buffer = this.free.pollFirst();
                            accumulated = size;

                            // 否则，说明上面 3 个条件至少有一个不满足
                        } else {
                            // we'll need to allocate memory, but we may only get
                            // part of what we need on this iteration
                            // 我们需要分配内存，但在这次迭代中可能只得到我们需要的一部分
                            // 这里会尝试从 free 列表中掏出来一些内存
                            freeUp(size - accumulated);
                            // got 变量代表在当前迭代中实际获得的内存大小
                            /*
                            思考一些这个 got 和 Math.min 的计算方式：
                            如果 size - accumulated > this.nonPooledAvailableMemory，即代表空间此时还不够，
                                则 got = this.nonPooledAvailableMemory，即这些剩下的空间都要在本次 while 中占走
                            否则说明空间已经足够了，此时 got = size - accumulated，
                                即只需要 size - accumulated 这么多空间，多了我也不要

                            因此，got 可以理解为本次 while 循环中实际获得的内存大小
                             */
                            int got = (int) Math.min(
                                    size - accumulated, // 还需要的内存大小
                                    this.nonPooledAvailableMemory // 当前可用的非池化内存大小
                            );
                            // 将搜刮来的大小从 nonPooledAvailableMemory 中转移到 accumulated 中
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    // 如果执行到这里，说明是正常走出的 while 循环，即 accumulated >= size
                    // Don't reclaim memory on throwable since nothing was thrown
                    // 不要在 throwable 上回收内存，因为没有抛出任何异常
                    accumulated = 0;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available memory
                    // 当这个循环无法成功终止时，不要丢失可用内存
                    this.nonPooledAvailableMemory += accumulated;
                    // 从 waiters 队列中移除 moreMemory 这个 condition
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            // 如果还有剩余的内存，则通知其他等待的线程 
            try {
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty())  // 如果 nonPooledAvailableMemory != 0 或者 free 列表不为空
                        && !this.waiters.isEmpty()) // 如果 waiters 队列不为空，即有等待的线程
                    // 通知 waiters 队列中的第一个线程；这些剩下的线程不管怎么搞，能压榨的空间也是当前线程吃剩下的
                    this.waiters.peekFirst().signal();
            } finally {
                // Another finally... otherwise find bugs complains
                // 解锁
                lock.unlock();
            }
        }

        // 如果分配的缓冲区为空，则分配缓冲区
        // 当代码走到了这里，其实已经获取到了足够多的空间（其实应该是适当大小的空间），然后需要分配缓冲区
        if (buffer == null)
            return safeAllocateByteBuffer(size);
        else
            return buffer;
    }

    // Protected for testing
    protected void recordWaitTime(long timeNs) {
        this.waitTime.record(timeNs, time.milliseconds());
    }

    /**
     * Allocate a buffer.  If buffer allocation fails (e.g. because of OOM) then return the size count back to
     * available memory and signal the next waiter if it exists.
     *
     * 分配一个缓冲区。如果缓冲区分配失败（例如由于 OOM），则将大小计数返回到可用内存，并通知下一个等待者（如果存在）。
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            // 从 byteBuffer 中分配 size 大小的内存
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            return buffer;
        } finally {
            // 如果分配失败
            if (error) {
                // 加锁
                this.lock.lock();
                try {
                    // 将 size 计数返回到可用内存，并通知下一个等待者（如果存在）
                    this.nonPooledAvailableMemory += size;
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                } finally {
                    this.lock.unlock();
                }
            }
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     * 尝试确保我们至少有请求的字节数的内存分配，通过释放池化的缓冲区（如果需要）
     */
    private void freeUp(int size) {
        // 把 free 列表中的 ByteBuffer 取出来，放到 nonPooledAvailableMemory 中
        // 直到 nonPooledAvailableMemory >= size 或者 free 列表为空
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * 将缓冲区返回到池中。
     * 如果它们是可池化大小，则将它们添加到空闲列表中，否则只是将内存标记为可用。
     *
     * @param buffer The buffer to return
     *               要返回的缓冲区
     * @param size The size of the buffer to mark as deallocated, note that this may be smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     *             要标记为已分配的缓冲区的大小，注意这可能小于 buffer.capacity，因为缓冲区可能在原地压缩期间重新分配自身
     */
    public void deallocate(ByteBuffer buffer, int size) {
        // 加锁
        lock.lock();
        try {
            // 如果空间大小等于 poolableSize，且空间大小等于 buffer.capacity()
            if (size == this.poolableSize && size == buffer.capacity()) {
                // 将 buffer 添加到 free 列表中，并清空 buffer
                // 后续 allocate 方法会直接从 free 列表中取出 buffer 实现复用
                buffer.clear();
                this.free.add(buffer);
            } else {
                // 否则，将 size 大小的空间添加到 nonPooledAvailableMemory 中
                this.nonPooledAvailableMemory += size;
            }
            // 通知 waiters 队列中的第一个线程
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     * 总空闲内存，未分配的和在空闲列表中的
     */
    public long availableMemory() {
        // 加锁
        lock.lock();
        try {
            // 返回总可用内存
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     * 获取未分配的内存（不在空闲列表中且未在使用中）
     */
    public long unallocatedMemory() {
        // 加锁
        lock.lock();
        try {
            // 返回未分配的内存
            return this.nonPooledAvailableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     * 等待内存的线程数
     */
    public int queued() {
        // 加锁
        lock.lock();
        try {
            // 返回 waiters 队列的大小
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     * 使用后将保留在空闲列表中的缓冲区大小
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     * 此池管理的总内存
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }

    /**
     * Closes the buffer pool. Memory will be prevented from being allocated, but may be deallocated. All allocations
     * awaiting available memory will be notified to abort.
     * 关闭缓冲池。
     * 将阻止分配内存，但可以释放内存。
     * 所有等待可用内存的分配将被通知中止。
     */
    public void close() {
        // 加锁
        this.lock.lock();
        // 标记当前 bufferPool 为关闭
        this.closed = true;
        try {
            // 通知所有等待的线程
            for (Condition waiter : this.waiters)
                waiter.signal();
        } finally {
            this.lock.unlock();
        }
    }
}
