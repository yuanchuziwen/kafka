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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link MemoryRecords}
 * instances to be sent to the server.
 * 这个类充当一个队列，将记录累积到 {@link MemoryRecords} 实例中，以便发送到服务器。
 *
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 * 累加器使用有限的内存量，当内存耗尽时，追加调用将被阻塞，除非显式禁用此行为。
 */
public final class RecordAccumulator {

    // 用于日志记录的Logger对象
    private final Logger log;
    // 标记累加器是否已关闭
    private volatile boolean closed;
    // 正在等待 flush 完成的线程的数量
    // 如果线程执行了 flush，一般都会阻塞等待所有 batch 发送完成
    private final AtomicInteger flushesInProgress;
    // 正在执行 append 操作的线程数量
    // append 方法内部只会对 dq 进行加锁（TopicPartition 维度），所以不同的 TopicPartition 之间是并发的
    private final AtomicInteger appendsInProgress;
    // 批次大小（字节）
    private final int batchSize;
    // 压缩类型
    private final CompressionType compression;
    // 延迟发送时间（毫秒）
    private final int lingerMs;
    // 重试退避时间（毫秒）
    private final long retryBackoffMs;
    // 传递超时时间（毫秒）
    private final int deliveryTimeoutMs;
    // 缓冲池，用于内存管理
    // 每次添加消息的时候，就先从 free 中 allocate 一块内存；
    // 如果需要的大小是 poolableSize 的值，那么对应的 ByteBuffer 就被多次复用；否则依靠垃圾收集器回收（deallocate）
    private final BufferPool free;
    // 时间实例，用于获取当前时间
    private final Time time;
    // API版本信息
    private final ApiVersions apiVersions;
    // 存储每个主题分区的批次队列
    private final ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches;
    // 存储未完成的批次，未完成是指没有被 abort 或 complete（complete 包括 ack 成功了的，以及感知异常后 batch 自行关闭了的）
    private final IncompleteBatches incomplete;

    // 以下变量仅由 sender 线程访问，因此不需要保护它们。

    // 存储已静音的主题分区
    // （如果需要保证消息的顺序性，那么在一次发送之后，就会把这个 partition 添加到 muted 集合中，保证该 partition 的 inFlight 的 batch 只有一个）
    // 得到 server 的响应之后，会将其从 muted 集合中移除
    private final Set<TopicPartition> muted;
    // 上一个发送的主题分区对应的数组索引下标
    // 由于每次排空都是 node 维度的，所以这个值单独是没有价值的，主要是为了避免每次都从头开始遍历
    private int drainIndex;
    // 事务管理器
    private final TransactionManager transactionManager;
    // 下一个批次过期的最早时间（绝对时间）
    private long nextBatchExpiryTimeMs = Long.MAX_VALUE;

    /**
     * Create a new record accumulator
     * 创建一个新的记录累加器
     *
     * @param logContext         The log context used for logging
     *                           日志上下文，用于日志记录
     * @param batchSize          The size to use when allocating {@link MemoryRecords} instances
     *                           分配 {@link MemoryRecords} 实例时使用的尺寸
     * @param compression        The compression codec for the records
     *                           记录的压缩类型
     * @param lingerMs           An artificial delay time to add before declaring a records instance that isn't full ready for
     *                           sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *                           latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     *                           延迟发送时间（毫秒），在声明一个不是满的记录实例之前添加一个延迟，以允许更多记录到达。
     *                           设置一个非零的 lingerMs 会权衡一些延迟以换取更好的吞吐量，因为更多的批处理（和更少，更大的请求）。
     * @param retryBackoffMs     An artificial delay time to retry the produce request upon receiving an error. This avoids
     *                           exhausting all retries in a short period of time.
     *                           重试退避时间（毫秒），在收到错误时重试生产请求时的人工延迟时间。
     *                           这避免了在短时间内耗尽所有重试。
     * @param deliveryTimeoutMs  The delivery timeout for the records
     *                           传递超时时间（毫秒）
     * @param metrics            The metrics
     *                           指标
     * @param time               The time instance to use
     *                           时间实例，用于获取当前时间
     * @param apiVersions        Request API versions for current connected brokers
     *                           当前连接的broker的请求API版本
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     *                           事务管理器，用于跟踪生产者ID、纪元和每个分区的序列号。
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             CompressionType compression,
                             int lingerMs,
                             long retryBackoffMs,
                             int deliveryTimeoutMs,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        // 为成员变量赋值       
        this.log = logContext.logger(RecordAccumulator.class);
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.batches = new CopyOnWriteMap<>();
        this.free = bufferPool;
        this.incomplete = new IncompleteBatches();
        this.muted = new HashSet<>();
        this.time = time;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        // 注册指标，监控 free 的阻塞等待线程数、总内存、可用内存
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        // 创建一个 "waiting-threads" 指标
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        // 创建一个 "buffer-total-bytes" 指标
        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        // 创建一个 "buffer-available-bytes" 指标
        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);
    }

    /**
     * Add a record to the accumulator, return the append result
     * 将记录添加到累加器，返回追加结果
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * 追加结果将包含未来的元数据，以及追加的批次是否已满或创建了一个新的批次的标志
     * <p>
     *
     * @param tp              The topic/partition to which this record is being sent
     *                        此记录将被发送到的主题/分区
     * @param timestamp       The timestamp of the record
     *                        记录的时间戳
     * @param key             The key for the record
     *                        record 的键
     * @param value           The value for the record
     *                        record 的值
     * @param headers         the Headers for the record
     *                        record 的 headers
     * @param callback        The user-supplied callback to execute when the request is complete
     *                        请求完成时执行的用户提供的回调
     * @param maxTimeToBlock  The maximum time in milliseconds to block for buffer memory to be available
     *                        阻塞缓冲内存可用的最长时间（毫秒）
     * @param abortOnNewBatch A boolean that indicates returning before a new batch is created and
     *                        running the partitioner's onNewBatch method before trying to append again
     *                        一个布尔值，指示在创建新批次之前返回，并在尝试再次追加之前运行分区器的 onNewBatch 方法
     * @param nowMs           The current time, in milliseconds
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     Callback callback,
                                     long maxTimeToBlock,
                                     boolean abortOnNewBatch,
                                     long nowMs) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        // 增加正在进行的追加操作计数
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // check if we have an in-progress batch
            // 尝试获取或创建指定主题分区的双端队列
            Deque<ProducerBatch> dq = getOrCreateDeque(tp);
            // 加锁，防止多线程并发访问
            synchronized (dq) {
                // 检查当前 accumulator 是否已关闭
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");
                // 尝试追加到现有批次
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
                // 如果得到的结果不是 null，说明追加成功，直接返回
                if (appendResult != null)
                    return appendResult;
            }

            // we don't have an in-progress record batch try to allocate a new batch
            // 如果没有进行中的批次，尝试分配新批次
            // Producer 的 doSend 方法会尝试调用两次 append 方法，第一次调用的时候就会传递 abortOnNewBatch 为 true
            // 此时 append 方法会返回一个 RecordAppendResult 对象，其中的 abortEvictBatch 为 true，表示需要创建新的批次
            // 然后 Producer 会调用 partitioner 的 onNewBatch 方法，然后再次调用 append 方法，此时 abortOnNewBatch 为 false
            if (abortOnNewBatch) {
                // Return a result that will cause another call to append.
                // 返回结果，触发再次调用 append
                return new RecordAppendResult(null, false, false, true);
            }

            // 计算新批次的大小并分配内存
            byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
            int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {} with remaining timeout {}ms", size, tp.topic(), tp.partition(), maxTimeToBlock);
            // 尝试从缓冲池中分配内存
            buffer = free.allocate(size, maxTimeToBlock);

            // Update the current time in case the buffer allocation blocked above.
            // 更新当前时间，万一上面的缓冲分配被阻塞
            nowMs = time.milliseconds();
            // 加锁，防止多线程并发访问
            // 说明在每一次尝试往 ProducerBatch 中追加记录时，都会加锁；
            // 锁的都是 Deque<ProducerBatch> dq，也就是每个主题分区对应的批次队列
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                // 再次检查生产者是否已关闭
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");

                // 再次尝试追加到现有批次
                // 两个 synchronized 之间可能有其他线程插入了针对该 topicPartition 的新的批次，所以这里会再 try 一次
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    return appendResult;
                }

                // 创建新的批次并追加记录
                // 基于之前 allocate 的 buffer 和 magic 数创建 MemoryRecordsBuilder
                MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
                // 基于 topicPartition、recordsBuilder 和 nowMs 创建一个新的 batch
                ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, nowMs);
                // 调用 batch.tryAppend 方法，并保证 append 成功，否则抛异常
                FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                        callback, nowMs));

                // 将新批次添加到队列和未完成批次集合中
                // 将新的 ProducerBatch 添加到队列中
                dq.addLast(batch);
                // 将新的 ProducerBatch 添加到未完成批次集合中
                incomplete.add(batch);

                // Don't deallocate this buffer in the finally block as it's being used in the record batch
                // 清空 buffer 引用，防止在 finally 块中被释放
                buffer = null;
                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, false);
            }
        } finally {
            // 如果 buffer 未被使用，释放它；说明前面抛异常
            if (buffer != null)
                free.deallocate(buffer);
            // 减少正在进行的追加操作计数
            appendsInProgress.decrementAndGet();
        }
    }

    private MemoryRecordsBuilder recordsBuilder(ByteBuffer buffer, byte maxUsableMagic) {
        // 如果开启了事务管理，但是 maxUsableMagic 小于 RecordBatch.MAGIC_VALUE_V2，那么抛出异常
        if (transactionManager != null && maxUsableMagic < RecordBatch.MAGIC_VALUE_V2) {
            throw new UnsupportedVersionException("Attempting to use idempotence with a broker which does not " +
                    "support the required message format (v2). The broker must be version 0.11 or later.");
        }
        return MemoryRecords.builder(buffer, maxUsableMagic, compression, TimestampType.CREATE_TIME, 0L);
    }

    /**
     * Try to append to a ProducerBatch.
     * 尝试追加到 ProducerBatch。
     * <p>
     * If it is full, we return null and a new batch is created. We also close the batch for record appends to free up
     * resources like compression buffers. The batch will be fully closed (ie. the record batch headers will be written
     * and memory records built) in one of the following cases (whichever comes first): right before send,
     * if it is expired, or when the producer is closed.
     * 如果它是满的，我们返回 null 并创建一个新的批次。
     * 我们还关闭批次以释放资源，如压缩缓冲区。
     * 批次将在以下情况之一（以先到者为准）完全关闭（即在发送之前，如果过期，或者当生产者关闭时）：
     * 在发送之前，如果过期，或者当生产者关闭时。
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque, long nowMs) {
        // 获取到 deque 中的最后一个 ProducerBatch
        ProducerBatch last = deque.peekLast();
        // 如果 last 不为空，那么尝试追加记录
        if (last != null) {
            // 这里调用 batch 的 tryAppend 方法
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, nowMs);
            // 如果得到的 future 为空，说明 last 已满，需要创建新的批次
            if (future == null)
                last.closeForRecordAppends();
            else
                // 将 batch.tryAppend 的结果再封装一层
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, false);
        }
        // 到了这里，说明消息是没能 append 成功的；因此返回一个 null
        return null;
    }

    /**
     * 判断指定主题分区是否被静音
     *
     * @param tp 主题分区
     * @return 如果被静音，则返回 true；否则返回 false
     */
    private boolean isMuted(TopicPartition tp) {
        return muted.contains(tp);
    }

    /**
     * 重置下一个批次过期的最早时间
     */
    public void resetNextBatchExpiryTime() {
        // 将下一个批次过期的最早时间设置为 Long.MAX_VALUE
        nextBatchExpiryTimeMs = Long.MAX_VALUE;
    }

    public void maybeUpdateNextBatchExpiryTime(ProducerBatch batch) {
        // 如果 batch 的创建时间加上传递超时时间大于 0，那么更新下一个批次过期的最早时间
        if (batch.createdMs + deliveryTimeoutMs > 0) {
            // the non-negative check is to guard us against potential overflow due to setting
            // a large value for deliveryTimeoutMs
            // 非负检查是为了防止由于为 deliveryTimeoutMs 设置大值而导致的潜在溢出
            nextBatchExpiryTimeMs = Math.min(nextBatchExpiryTimeMs, batch.createdMs + deliveryTimeoutMs);
        } else {
            log.warn("Skipping next batch expiry time update due to addition overflow: "
                    + "batch.createMs={}, deliveryTimeoutMs={}", batch.createdMs, deliveryTimeoutMs);
        }
    }

    /**
     * Get a list of batches which have been sitting in the accumulator too long and need to be expired.
     * <p>
     * 获取在累加器中停留时间过长且需要过期的批次列表。
     * 其实就是从 ConcurrentHashMap 中的每一个 entry 中的 Deque<ProducerBatch> 中移除过期的 ProducerBatch
     */
    public List<ProducerBatch> expiredBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();
        // 遍历 ConcurrentHashMap 中的每一个 entry
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            // expire the batches in the order of sending
            // 获取 entry 中的 value，即 Deque<ProducerBatch>
            Deque<ProducerBatch> deque = entry.getValue();
            // 加锁，防止多线程并发访问
            synchronized (deque) {
                // 遍历 deque 中的每一个元素，直到找到一个未过期的 ProducerBatch
                while (!deque.isEmpty()) {
                    ProducerBatch batch = deque.getFirst();
                    // 判断当前栈顶的 batch 是否已经过期了
                    if (batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now)) {
                        // 如果已经过期，那么从 deque 中移除，并将其添加到 expiredBatches 中
                        deque.poll();
                        // 中止该 batch，但是不会去触发它的 callback
                        batch.abortRecordAppends();
                        expiredBatches.add(batch);
                    } else {
                        maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
            }
        }
        return expiredBatches;
    }

    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /**
     * Re-enqueue the given record batch in the accumulator. In Sender.completeBatch method, we check
     * whether the batch has reached deliveryTimeoutMs or not. Hence we do not do the delivery timeout check here.
     * <p>
     * 重新将给定的记录批次排队到累加器中。
     * 在 Sender.completeBatch 方法中，我们检查批次是否已达到 deliveryTimeoutMs。
     * 因此，我们在这里不进行传递超时检查。
     */
    public void reenqueue(ProducerBatch batch, long now) {
        // 让 batch 感知到自身的 reenqueue 操作，进而修改内部的一些信息
        batch.reenqueued(now);
        Deque<ProducerBatch> deque = getOrCreateDeque(batch.topicPartition);
        // 加锁，防止多线程并发访问
        synchronized (deque) {
            // 将 ProducerBatch 添加到 Deque<ProducerBatch> 中
            if (transactionManager != null)
                insertInSequenceOrder(deque, batch);
            else
                // 这里会将 batch 放到 deque 的队首
                deque.addFirst(batch);
        }
    }

    /**
     * Split the big batch that has been rejected and reenqueue the split batches in to the accumulator.
     * <p>
     *     分割已被拒绝的大批次，并将分割后的批次重新排队到累加器中。
     *
     * @return the number of split batches.
     * 分割已被拒绝的大批次，并将分割后的批次重新排队到累加器中。
     */
    public int splitAndReenqueue(ProducerBatch bigBatch) {
        // Reset the estimated compression ratio to the initial value or the big batch compression ratio, whichever
        // is bigger. There are several different ways to do the reset. We chose the most conservative one to ensure
        // the split doesn't happen too often.
        // 将估计的压缩比重置为初始值或大批次的压缩比，以较大者为准。
        // 有几种不同的方法可以重置。我们选择了最保守的方法，以确保分割不会太频繁。
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression,
                Math.max(1.0f, (float) bigBatch.compressionRatio()));

        // 将大批次分割为多个小批次
        Deque<ProducerBatch> dq = bigBatch.split(this.batchSize);
        int numSplitBatches = dq.size();

        // 获取 concurrentMap 中的当前 bigBatch 对应的 TopicPartition 对应的 Deque<ProducerBatch>
        Deque<ProducerBatch> partitionDequeue = getOrCreateDeque(bigBatch.topicPartition);
        // 将多个小批次重新添加到 concurrentMap 中
        while (!dq.isEmpty()) {
            ProducerBatch batch = dq.pollLast();
            incomplete.add(batch);
            // We treat the newly split batches as if they are not even tried.
            synchronized (partitionDequeue) {
                if (transactionManager != null) {
                    // We should track the newly created batches since they already have assigned sequences.
                    transactionManager.addInFlightBatch(batch);
                    insertInSequenceOrder(partitionDequeue, batch);
                } else {
                    // 这里也是放在了队首
                    partitionDequeue.addFirst(batch);
                }
            }
        }
        return numSplitBatches;
    }

    // We will have to do extra work to ensure the queue is in order when requests are being retried and there are
    // multiple requests in flight to that partition. If the first in flight request fails to append, then all the
    // subsequent in flight requests will also fail because the sequence numbers will not be accepted.
    //
    // Further, once batches are being retried, we are reduced to a single in flight request for that partition. So when
    // the subsequent batches come back in sequence order, they will have to be placed further back in the queue.
    //
    // Note that this assumes that all the batches in the queue which have an assigned sequence also have the current
    // producer id. We will not attempt to reorder messages if the producer id has changed, we will throw an
    // IllegalStateException instead.
    private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
        // When we are requeing and have enabled idempotence, the reenqueued batch must always have a sequence.
        if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
            throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " +
                    "though idempotency is enabled.");

        if (transactionManager.nextBatchBySequence(batch.topicPartition) == null)
            throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
                    "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());

        ProducerBatch firstBatchInQueue = deque.peekFirst();
        if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
            // The incoming batch can't be inserted at the front of the queue without violating the sequence ordering.
            // This means that the incoming batch should be placed somewhere further back.
            // We need to find the right place for the incoming batch and insert it there.
            // We will only enter this branch if we have multiple inflights sent to different brokers and we need to retry
            // the inflight batches.
            //
            // Since we reenqueue exactly one batch a time and ensure that the queue is ordered by sequence always, it
            // is a simple linear scan of a subset of the in flight batches to find the right place in the queue each time.
            List<ProducerBatch> orderedBatches = new ArrayList<>();
            while (deque.peekFirst() != null && deque.peekFirst().hasSequence() && deque.peekFirst().baseSequence() < batch.baseSequence())
                orderedBatches.add(deque.pollFirst());

            log.debug("Reordered incoming batch with sequence {} for partition {}. It was placed in the queue at " +
                    "position {}", batch.baseSequence(), batch.topicPartition, orderedBatches.size());
            // Either we have reached a point where there are batches without a sequence (ie. never been drained
            // and are hence in order by default), or the batch at the front of the queue has a sequence greater
            // than the incoming batch. This is the right place to add the incoming batch.
            deque.addFirst(batch);

            // Now we have to re insert the previously queued batches in the right order.
            for (int i = orderedBatches.size() - 1; i >= 0; --i) {
                deque.addFirst(orderedBatches.get(i));
            }

            // At this point, the incoming batch has been queued in the correct place according to its sequence.
        } else {
            deque.addFirst(batch);
        }
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * 获取一个节点列表，其分区已准备好发送，以及任何不可发送的分区将准备就绪的最早时间；
     * 同时返回一个标志，表示累积的分区批次中是否有任何未知的领导者。

     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     * 一个分区准备好发送的条件是：
     * <ol>
     *     <li>该分区没有在回退其发送</li>
     *     <li>该分区没有被静音（如果
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   设置为1，则为了防止重新排序）</li>
     *     <li>以下任一条件为真</li>
     *     <ul>
     *         <li>记录集已满</li>
     *         <li>记录集在累加器中至少停留了lingerMs毫秒</li>
     *     </ul>
     */
    /**
     * 检查哪些节点准备好发送数据，以及下一次检查的延迟时间
     *
     * @param cluster 集群元数据
     * @param nowMs 当前时间戳（毫秒）
     * @return ReadyCheckResult 包含准备好的节点、下次检查延迟和未知领导者主题
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        // 存储准备好发送数据的节点
        Set<Node> readyNodes = new HashSet<>();
        // 下次检查的延迟时间，初始化为最大值
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        // 存储未知领导者的主题
        Set<String> unknownLeaderTopics = new HashSet<>();

        // 检查是否有线程正在阻塞等待 BufferPool 释放空间；
        // 如果有，说明需要尽快发送数据来释放空间
        boolean exhausted = this.free.queued() > 0;
        
        // 遍历所有主题分区的批次
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                // When producing to a large number of partitions, this path is hot and deques are often empty.
                // We check whether a batch exists first to avoid the more expensive checks whenever possible.
                // 在向大量分区发送消息时，这部分同步锁竞争比较激烈，并且双端队列通常为空。
                // 首先检查是否存在批次，以尽可能多避免后续的昂贵检查。
                ProducerBatch batch = deque.peekFirst();
                if (batch != null) {
                    TopicPartition part = entry.getKey();
                    // 这个 cluster 是调用方给的，即 sender 给的
                    // 如果 accumulator 通过 cluster 不知道 leader 是谁，那么 sender 肯定也不知道
                    // 不知道的原因可能是因为 replica 发生了 leader 切换，而 metadata 只做了部分更新
                    Node leader = cluster.leaderFor(part);
                    if (leader == null) {
                        // This is a partition for which leader is not known, but messages are available to send.
                        // Note that entries are currently not removed from batches when deque is empty.
                        // 这是一个 leader replica 未知，但有消息可发送的分区。
                        // 注意，当双端队列为空时，entries 不会从 batches 中删除。
                        unknownLeaderTopics.add(part.topic());
                    } else if (!readyNodes.contains(leader) && !isMuted(part)) {
                        // 计算批次等待时间
                        long waitedTimeMs = batch.waitedTimeMs(nowMs);
                        boolean backingOff = batch.attempts() > 0 && waitedTimeMs < retryBackoffMs;
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        
                        // 检查批次是否可发送
                        boolean full = deque.size() > 1 || batch.isFull();
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        boolean transactionCompleting = transactionManager != null && transactionManager.isCompleting();
                        boolean sendable = full
                                || expired
                                || exhausted
                                || closed
                                || flushInProgress()
                                || transactionCompleting;
                        
                        if (sendable && !backingOff) {
                            // 如果批次可发送且不在退避状态，将节点添加到准备好的集合
                            readyNodes.add(leader);
                        } else {
                            // 计算下次检查的延迟时间
                            long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            // 注意，这会产生一个保守的估计，因为一个不可发送的分区可能有一个 leader，
                            // 稍后会发现它有可发送的数据。然而，这已经足够好了，因为我们只是醒来并再次休眠
                            // 剩余的时间。
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }
        // 返回检查结果
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * Check whether there are any batches which haven't been drained
     * <p>
     * 检查是否存在任何未排空的批次
     * 当 sender 在被 close 的时候会调用这个方法来检查是否还有未排空的批次，进而判断自身是否可以关闭
     */
    public boolean hasUndrained() {
        // 遍历所有主题分区的批次队列
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            Deque<ProducerBatch> deque = entry.getValue();
            // 对每个队列进行同步，以确保线程安全
            synchronized (deque) {
                // 如果队列非空，说明存在未排空的批次
                if (!deque.isEmpty())
                    return true;
            }
        }
        // 所有队列都为空，没有未排空的批次
        return false;
    }

    private boolean shouldStopDrainBatchesForPartition(ProducerBatch first, TopicPartition tp) {
        ProducerIdAndEpoch producerIdAndEpoch = null;
        if (transactionManager != null) {
            if (!transactionManager.isSendToPartitionAllowed(tp))
                return true;

            producerIdAndEpoch = transactionManager.producerIdAndEpoch();
            if (!producerIdAndEpoch.isValid())
                // we cannot send the batch until we have refreshed the producer id
                return true;

            if (!first.hasSequence()) {
                if (transactionManager.hasInflightBatches(tp) && transactionManager.hasStaleProducerIdAndEpoch(tp)) {
                    // Don't drain any new batches while the partition has in-flight batches with a different epoch
                    // and/or producer ID. Otherwise, a batch with a new epoch and sequence number
                    // 0 could be written before earlier batches complete, which would cause out of sequence errors
                    return true;
                }

                if (transactionManager.hasUnresolvedSequence(first.topicPartition))
                    // Don't drain any new batches while the state of previous sequence numbers
                    // is unknown. The previous batches would be unknown if they were aborted
                    // on the client after being sent to the broker at least once.
                    return true;
            }

            int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);
            if (firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
                    && first.baseSequence() != firstInFlightSequence)
                // If the queued batch already has an assigned sequence, then it is being retried.
                // In this case, we wait until the next immediate batch is ready and drain that.
                // We only move on when the next in line batch is complete (either successfully or due to
                // a fatal broker error). This effectively reduces our in flight request count to 1.
                return true;
        }
        return false;
    }

    /**
     * 为指定节点清空所有数据，并将它们分组为一批批次，这些批次的大小总和不超过指定的最大大小。
     * 该方法尝试避免重复选择相同的主题分区。
     *
     * @param cluster 当前集群元数据
     * @param node    要清空的节点
     * @param maxSize 要清空的最大字节数
     * @param now     当前时间（毫秒）
     * @return 一批批次，总大小小于请求的 maxSize。
     */
    private List<ProducerBatch> drainBatchesForOneNode(Cluster cluster, Node node, int maxSize, long now) {
        int size = 0;
        // 获取指定节点的所有分区信息
        List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
        // 用于存储已准备好发送的批次
        List<ProducerBatch> ready = new ArrayList<>();
        // 下面的这个 do-while 循环，会遍历所有分区，将可以发送的批次添加到 ready 列表中
        // drainIndex 的设计能保证每次循环都从上次遍历的位置开始，直到遍历完所有分区；

        // 计算起始索引
        int start = drainIndex = drainIndex % parts.size();
        do {
            // 获取当前分区的信息
            PartitionInfo part = parts.get(drainIndex);
            // 创建 TopicPartition 对象
            TopicPartition tp = new TopicPartition(part.topic(), part.partition());
            // 更新下一次迭代的索引
            this.drainIndex = (this.drainIndex + 1) % parts.size();

            // Only proceed if the partition has no in-flight batches.
            // 如果当前分区被静音，跳过；
            // 即说明当前 producer 保证消息的顺序性，且这个分区存在 in-flight 的批次，那么就不会再往这个分区发送消息
            if (isMuted(tp))
                continue;

            // 获取指定分区的双端队列
            Deque<ProducerBatch> deque = getDeque(tp);
            // 如果双端队列为空，跳过；即说明这个分区没有消息
            if (deque == null)
                continue;

            // 加锁
            synchronized (deque) {
                // invariant: !isMuted(tp,now) && deque != null
                // 获取双端队列的第一个批次
                ProducerBatch first = deque.peekFirst();
                // 如果第一个批次为空，跳过
                if (first == null)
                    continue;

                // first != null
                // 如果第一个批次尝试次数大于 0，并且等待时间小于重试回退时间，跳过
                // 注意，这个 continue 会导致在本次发送中跳过这个分区的所有批次
                boolean backoff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackoffMs;
                // Only drain the batch if it is not during backoff period.
                if (backoff)
                    continue;

                // 如果在增加了这个批次之后，大小超过了 maxSize && 此时 ready 中已经有的准备发送的批次，那么就跳出循环
                // 注意，这里有两个判断条件；如果大小超过了，但是此时 ready 是空的，那么也可能会发送这条消息（猜测是去触发 split？）
                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                    // there is a rare case that a single batch size is larger than the request size due to
                    // compression; in this case we will still eventually send this batch in a single request
                    // 这里有一个罕见的情况，即单个批次的大小由于压缩而大于请求大小；在这种情况下，我们仍然会
                    // 在单个请求中发送这个批次
                    break;
                } else {
                    // 如果需要停止为分区清空批次，跳过（事务相关的校验）
                    if (shouldStopDrainBatchesForPartition(first, tp))
                        break;

                    // 如果事务管理器不为空，并且是事务性的，跳过
                    boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
                    // 获取生产者 ID 和 epoch
                    ProducerIdAndEpoch producerIdAndEpoch =
                            transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
                    // 获取第一个批次，这里会从 deque 中移除
                    ProducerBatch batch = deque.pollFirst();
                    // 如果生产者 ID 和 epoch 不为空，并且批次没有序列号，跳过
                    if (producerIdAndEpoch != null && !batch.hasSequence()) {
                        // If the producer id/epoch of the partition do not match the latest one
                        // of the producer, we update it and reset the sequence. This should be
                        // only done when all its in-flight batches have completed. This is guarantee
                        // in `shouldStopDrainBatchesForPartition`.
                        // 如果分区的生产者ID/epoch与生产者的最新ID/epoch不匹配，
                        // 我们更新它并重置序列号。这应该只在所有正在传输的批次都完成时进行。
                        // 这在`shouldStopDrainBatchesForPartition`中得到保证。
                        transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);

                        // If the batch already has an assigned sequence, then we should not change the producer id and
                        // sequence number, since this may introduce duplicates. In particular, the previous attempt
                        // may actually have been accepted, and if we change the producer id and sequence here, this
                        // attempt will also be accepted, causing a duplicate.
                        //
                        // Additionally, we update the next sequence number bound for the partition, and also have
                        // the transaction manager track the batch so as to ensure that sequence ordering is maintained
                        // even if we receive out of order responses.
                        // 如果批次已经分配了序列号，则不应更改生产者ID和序列号，
                        // 因为这可能会引入重复。特别是，前一个尝试实际上可能已经被接受，
                        // 如果我们在这里更改生产者ID和序列号，这个尝试也将被接受，导致重复。
                        //
                        // 此外，我们更新分区的下一个序列号绑定，并让事务管理器跟踪批次，
                        // 以确保即使在接收到乱序响应时，序列号顺序也能得到维护。
                        batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
                        // 更新序列号
                        transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
                        log.debug("Assigned producerId {} and producerEpoch {} to batch with base sequence " +
                                        "{} being sent to partition {}", producerIdAndEpoch.producerId,
                                producerIdAndEpoch.epoch, batch.baseSequence(), tp);
                        // 将批次添加到事务管理器中
                        transactionManager.addInFlightBatch(batch);
                    }
                    // 关闭当前批次
                    batch.close();
                    // 根据批次大小更新 size，即请求中的消息大小
                    size += batch.records().sizeInBytes();
                    // 将批次添加到已准备好发送的批次列表中
                    ready.add(batch);
                    // 标记批次已清空
                    batch.drained(now);
                }
            }

            // while 循环结束的条件是 drainIndex 回到了起始位置，说明遍历完所有分区
        } while (start != drainIndex);
        // 返回已准备好发送的批次列表
        return ready;
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified
     * size on a per-node basis. This method attempts to avoid choosing the same topic-node over and over.
     * 
     * 为每个节点清空所有数据，并将它们分组为一批批次，这些批次的大小总和不超过指定的最大大小。
     * 该方法尝试避免重复选择相同的主题节点。
     *
     * @param cluster The current cluster metadata
     *                当前集群元数据
     * @param nodes   The list of node to drain
     *                要清空的节点列表
     * @param maxSize The maximum number of bytes to drain
     *                要清空的最大字节数
     * @param now     The current unix time in milliseconds
     *                当前时间（毫秒）
     * @return A list of {@link ProducerBatch} for each node specified with total size less than the requested maxSize.
     *        每个节点的 ProducerBatch 列表，总大小小于请求的 maxSize。
     */
    public Map<Integer, List<ProducerBatch>> drain(Cluster cluster, Set<Node> nodes, int maxSize, long now) {
        // 如果节点列表为空，返回一个空的 Map
        if (nodes.isEmpty())
            return Collections.emptyMap();

        // 用于存储每个节点的 ProducerBatch 列表
        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
        // 遍历所有节点
        for (Node node : nodes) {
            // 为每个节点清空所有数据，并将它们分组为一批批次，这些批次的大小总和不超过指定的最大大小
            List<ProducerBatch> ready = drainBatchesForOneNode(cluster, node, maxSize, now);
            // 将每个节点的 ProducerBatch 列表添加到 Map 中
            batches.put(node.id(), ready);
        }
        // 返回所有节点的 ProducerBatch 列表
        return batches;
    }

    /**
     * The earliest absolute time a batch will expire (in milliseconds)
     * 返回最早的批次过期时间（以毫秒为单位）
     */
    public long nextExpiryTimeMs() {
        return this.nextBatchExpiryTimeMs;
    }

    /**
     * 获取指定 TopicPartition 的双端队列
     *
     * @param tp
     * @return
     */
    private Deque<ProducerBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
        // 从 concurrentHashMap 中获取指定 TopicPartition 的双端队列
        Deque<ProducerBatch> d = this.batches.get(tp);
        // 如果队列存在，直接返回
        if (d != null)
            return d;
        // 如果队列不存在，创建一个新的队列并放入 concurrentHashMap 中
        d = new ArrayDeque<>();
        // 这里考虑到多线程并发，使用 putIfAbsent 方法；
        // 如果执行期间发生并发，那么 putIfAbsent 方法会返回真正在 concurrentHashMap 中的队列，所以需要再次判断
        Deque<ProducerBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            return previous;
    }

    /**
     * Deallocate the record batch
     * 释放记录批次
     */
    public void deallocate(ProducerBatch batch) {
        incomplete.remove(batch);
        // Only deallocate the batch if it is not a split batch because split batch are allocated outside the
        // buffer pool.
        // 只有当批次不是 split batch 时，才释放它，因为 split batch 是在 buffer pool 之外分配的
        if (!batch.isSplitBatch())
            free.deallocate(batch.buffer(), batch.initialCapacity());
    }

    /**
     * Package private for unit test. Get the buffer pool remaining size in bytes.
     * 包级私有方法，用于单元测试。获取缓冲池剩余的内存大小
     */
    long bufferPoolAvailableMemory() {
        return free.availableMemory();
    }

    /**
     * Are there any threads currently waiting on a flush?
     * 检查是否有任何线程当前正在等待刷新
     * <p>
     * package private for test
     */
    boolean flushInProgress() {
        // 这个方法会在 ready 的时候被调用，一旦它返回 true，即代表有线程正在等待 flush，那么 ready 方法就会认为所有 batch 都是 sendable 的
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<ProducerBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     * <p>
     *     启动累加器中数据的刷新...这会使所有请求立即准备就绪
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     * 检查是否有任何线程当前正在追加消息
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     * 标记所有分区为可发送状态，并阻塞直到发送完成
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            // Obtain a copy of all of the incomplete ProduceRequestResult(s) at the time of the flush.
            // We must be careful not to hold a reference to the ProduceBatch(s) so that garbage
            // collection can occur on the contents.
            // The sender will remove ProducerBatch(s) from the original incomplete collection.
            // 获取所有未完成的 ProduceRequestResult 对象，并阻塞直到它们完成
            // 我们必须在持有 ProduceBatch 的引用，以确保垃圾回收能够发生
            // 发送者将从原始的 incomplete 集合中删除 ProduceBatch
            for (ProduceRequestResult result : this.incomplete.requestResults())
                result.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * Check whether there are any pending batches (whether sent or unsent).
     * 检查是否有任何未完成的批次（无论是否已发送）
     */
    public boolean hasIncomplete() {
        return !this.incomplete.isEmpty();
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     * <p>
     * 这个方法只会在 sender 被强制关闭时调用，它会中止所有未完成的批次并返回
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        // 我们使用一个 while 循环来中止所有未完成的批次，直到没有线程在尝试追加记录为止
        // 1. 避免丢失批次
        // 2. 释放内存，以防追加线程被阻塞在缓冲区满的情况
        // 这个循环应该很快就能完成
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        // 在所有线程都停止追加记录后，再进行一次 abort 操作，以防最后一个追加线程追加的批次未被中止
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     * 遍历所有未完成的批次（无论是否已发送），并中止它们
     */
    private void abortBatches() {
        abortBatches(new KafkaException("Producer is closed forcefully."));
    }

    /**
     * Abort all incomplete batches (whether they have been sent or not)
     * 中止所有未完成的批次（无论是否已发送）
     */
    void abortBatches(final RuntimeException reason) {
        // 遍历所有未完成的批次
        for (ProducerBatch batch : incomplete.copyAll()) {
            // 获取该批次所属的主题分区的双端队列
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            // 同步操作
            synchronized (dq) {
                // 中止记录追加，不会触发回调
                batch.abortRecordAppends();
                // 从双端队列中移除该批次
                dq.remove(batch);
            }
            // 中止该批次，会触发回调
            batch.abort(reason);
            // 释放该批次
            deallocate(batch);
        }
    }

    /**
     * Abort any batches which have not been drained
     * 中止所有未被清空的批次
     */
    void abortUndrainedBatches(RuntimeException reason) {
        // 遍历所有未被清空的批次
        for (ProducerBatch batch : incomplete.copyAll()) {
            // 获取该批次所属的主题分区的双端队列
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            // 是否中止
            boolean aborted = false;
            // 同步操作
            synchronized (dq) {
                // 如果事务管理器不为空且批次没有序列号，或者事务管理器为空且批次未关闭，则中止该批次
                if ((transactionManager != null && !batch.hasSequence()) || (transactionManager == null && !batch.isClosed())) {
                    aborted = true;
                    // 中止记录追加，不会触发回调
                    batch.abortRecordAppends();
                    // 从双端队列中移除该批次
                    dq.remove(batch);
                }
            }
            // 如果中止了，则中止该批次，会触发回调
            if (aborted) {
                batch.abort(reason);
                // 释放该批次
                deallocate(batch);
            }
        }
    }

    /**
     * 将指定主题分区静音，其实是添加到 muted 集合中
     *
     * @param tp
     */
    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    /**
     * 取消指定主题分区的静音，其实是从 muted 集合中删除
     *
     * @param tp
     */
    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     * 关闭当前的 RecordAccumulator 对象，并强制所有记录缓冲区被清空
     */
    public void close() {
        this.closed = true;
        // 关闭 free 对象，释放所有资源
        this.free.close();
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;
        public final boolean abortForNewBatch;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated, boolean abortForNewBatch) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.abortForNewBatch = abortForNewBatch;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }
}
