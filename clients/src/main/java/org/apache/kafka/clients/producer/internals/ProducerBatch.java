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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * A batch of records that is or will be sent.
 * 一个将要发送的记录批次
 * <p>
 * This class is not thread safe and external synchronization must be used when modifying it
 * 这个类不是线程安全的，当修改它时必须使用外部同步
 */
public final class ProducerBatch {

    // 日志记录器
    private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class);

    // 枚举类型，表示最终状态
    private enum FinalState {ABORTED, FAILED, SUCCEEDED}

    // 创建时间戳
    final long createdMs;
    // 主题分区
    final TopicPartition topicPartition;
    // 生产请求结果
    final ProduceRequestResult produceFuture;

    // 回调函数列表
    private final List<Thunk> thunks = new ArrayList<>();
    // 内存记录构建器
    private final MemoryRecordsBuilder recordsBuilder;
    // 尝试次数
    private final AtomicInteger attempts = new AtomicInteger(0);
    // 是否为拆分批次
    private final boolean isSplitBatch;
    // 最终状态
    private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);

    // 记录数量
    int recordCount;
    // 最大记录大小
    int maxRecordSize;
    // 上次尝试发送的时间戳
    // 用于计算 backoff 时间
    private long lastAttemptMs;
    // 上次追加时间
    private long lastAppendTime;
    // 排空时间戳
    private long drainedMs;
    // 是否重试
    private boolean retry;
    // 是否重新打开（事务相关）
    private boolean reopened;

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
        this(tp, recordsBuilder, createdMs, false);
    }

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs, boolean isSplitBatch) {
        this.createdMs = createdMs;
        this.lastAttemptMs = createdMs;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        // 构造一个 ProduceRequestResult 对象，记录了整个批次发送的结果
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.retry = false;
        this.isSplitBatch = isSplitBatch;
        float compressionRatioEstimation = CompressionRatioEstimator.estimation(topicPartition.topic(),
                recordsBuilder.compressionType());
        recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation);
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     * <p>
     * 将 record 添加到当前 ProducerBatch 对象中，并返回该记录集中的相对偏移量
     *
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     * 如果没有足够的空间，则返回 null
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        // 交给 MemoryRecordsBuilder 来处理
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        } else {
            this.recordsBuilder.append(timestamp, key, value, headers);
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            this.lastAppendTime = now;
            // 这个 FutureRecordMetadata 实现了 Future 接口，内部的 get 方法会触发针对 this.produceFuture 的阻塞
            // this.produceFuture 是 batch 维度的，内部是一个 CountDownLatch，只有当 batch 完成时才会 countDown
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                    timestamp,
                    key == null ? -1 : key.length,
                    value == null ? -1 : value.length,
                    Time.SYSTEM);
            // we have to keep every future returned to the users in case the batch needs to be
            // split to several new batches and resent.
            // 将回调函数和 FutureRecordMetadata 对象封装成 Thunk 对象，添加到 thunks 列表中
            thunks.add(new Thunk(callback, future));
            this.recordCount++;
            return future;
        }
    }

    /**
     * This method is only used by {@link #split(int)} when splitting a large batch to smaller ones.
     * <p>
     * 当将一个大的 batch 拆分为多个小的 batch 时，只有 {@link #split(int)} 方法会调用这个方法
     *
     * @return true if the record has been successfully appended, false otherwise.
     */
    private boolean tryAppendForSplit(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers, Thunk thunk) {
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return false;
        } else {
            // No need to get the CRC.
            this.recordsBuilder.append(timestamp, key, value, headers);
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                    timestamp,
                    key == null ? -1 : key.remaining(),
                    value == null ? -1 : value.remaining(),
                    Time.SYSTEM);
            // Chain the future to the original thunk.
            // 关联新的 FutureRecordMetadata 对象和原来的 Thunk 对象
            thunk.future.chain(future);

            this.thunks.add(thunk);
            this.recordCount++;
            return true;
        }
    }

    /**
     * Abort the batch and complete the future and callbacks.
     * <p>
     * 中止 batch，并完成 future 和回调
     *
     * @param exception The exception to use to complete the future and awaiting callbacks.
     */
    public void abort(RuntimeException exception) {
        // cas 修改 finalState 为 ABORTED
        if (!finalState.compareAndSet(null, FinalState.ABORTED))
            throw new IllegalStateException("Batch has already been completed in final state " + finalState.get());

        log.trace("Aborting batch for partition {}", topicPartition, exception);
        // 更新 ProduceRequestResult 对象的值，并且触发回调方法
        completeFutureAndFireCallbacks(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, index -> exception);
    }

    /**
     * Check if the batch has been completed (either successfully or exceptionally).
     * <p>
     * 检查 batch 是否已经完成（成功或异常）
     *
     * @return `true` if the batch has been completed, `false` otherwise.
     */
    public boolean isDone() {
        return finalState() != null;
    }

    /**
     * Complete the batch successfully.
     * <p>
     *     成功完成 batch
     *
     * @param baseOffset    The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @return true if the batch was completed as a result of this call, and false
     * if it had been completed previously
     */
    public boolean complete(long baseOffset, long logAppendTime) {
        return done(baseOffset, logAppendTime, null, null);
    }

    /**
     * Complete the batch exceptionally. The provided top-level exception will be used
     * for each record future contained in the batch.
     * <p>
     * 以异常的方式完成 batch。提供的顶级异常将用于每个包含在 batch 中的记录 future。
     *
     * @param topLevelException top-level partition error
     * @param recordExceptions  Record exception function mapping batchIndex to the respective record exception
     * @return true if the batch was completed as a result of this call, and false
     * if it had been completed previously
     */
    public boolean completeExceptionally(
            RuntimeException topLevelException,
            Function<Integer, RuntimeException> recordExceptions
    ) {
        Objects.requireNonNull(topLevelException);
        Objects.requireNonNull(recordExceptions);
        return done(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, topLevelException, recordExceptions);
    }

    /**
     * Finalize the state of a batch. Final state, once set, is immutable. This function may be called
     * once or twice on a batch. It may be called twice if
     * 1. An inflight batch expires before a response from the broker is received. The batch's final
     * state is set to FAILED. But it could succeed on the broker and second time around batch.done() may
     * try to set SUCCEEDED final state.
     * 2. If a transaction abortion happens or if the producer is closed forcefully, the final state is
     * ABORTED but again it could succeed if broker responds with a success.
     * 结束 batch 的状态，一旦设置，状态就不可变。
     * 这个方法可能会被调用一次或两次：
     * 1. 一个处于 inflight 状态的 batch 在接收到 broker 的响应之前过期时，batch 的最终状态被设置为 FAILED。
     * 但是，它可能在 broker 中成功，然后在第二次调用 batch.done() 方法时，可能尝试将最终状态设置为 SUCCEEDED。
     * 2. 如果发生事务中止或生产者被强制关闭，最终状态被设置为 ABORTED，但它可能再次成功，如果 broker 响应成功。
     * <p>
     * Attempted transitions from [FAILED | ABORTED] --> SUCCEEDED are logged.
     * Attempted transitions from one failure state to the same or a different failed state are ignored.
     * Attempted transitions from SUCCEEDED to the same or a failed state throw an exception.
     * <p>
     * 尝试从 [FAILED | ABORTED] --> SUCCEEDED 的状态转换会被记录。
     * 尝试从一种失败状态转换到相同或不同的失败状态会被忽略。
     * 尝试从 SUCCEEDED 转换到相同或失败状态会抛出异常。
     *
     * @param baseOffset        The base offset of the messages assigned by the server
     * @param logAppendTime     The log append time or -1 if CreateTime is being used
     * @param topLevelException The exception that occurred (or null if the request was successful)
     * @param recordExceptions  Record exception function mapping batchIndex to the respective record exception
     * @return true if the batch was completed successfully and false if the batch was previously aborted
     */
    private boolean done(
            long baseOffset,
            long logAppendTime,
            RuntimeException topLevelException,
            Function<Integer, RuntimeException> recordExceptions
    ) {
        // 根据 topLevelException 的值来决定最终状态
        final FinalState tryFinalState = (topLevelException == null) ? FinalState.SUCCEEDED : FinalState.FAILED;
        if (tryFinalState == FinalState.SUCCEEDED) {
            log.trace("Successfully produced messages to {} with base offset {}.", topicPartition, baseOffset);
        } else {
            log.trace("Failed to produce messages to {} with base offset {}.", topicPartition, baseOffset, topLevelException);
        }

        // 尝试将 finalState 设置为 tryFinalState，如果成功，则执行回调方法
        if (this.finalState.compareAndSet(null, tryFinalState)) {
            completeFutureAndFireCallbacks(baseOffset, logAppendTime, recordExceptions);
            return true;
        }

        // 否则，说明 cas 失败，finalState 已经被设置为其他状态
        // 如果当前 finalState 不是 SUCCEEDED，
        if (this.finalState.get() != FinalState.SUCCEEDED) {
            // 如果本次计算的结果需要变成 SUCCEEDED，则记录日志
            if (tryFinalState == FinalState.SUCCEEDED) {
                // Log if a previously unsuccessful batch succeeded later on.
                log.debug("ProduceResponse returned {} for {} after batch with base offset {} had already been {}.",
                        tryFinalState, topicPartition, baseOffset, this.finalState.get());
            } else {
                // FAILED --> FAILED and ABORTED --> FAILED transitions are ignored.
                log.debug("Ignored state transition {} -> {} for {} batch with base offset {}",
                        this.finalState.get(), tryFinalState, topicPartition, baseOffset);
            }
        } else {
            // 如果当前 finalState 是 SUCCEEDED，则抛出异常
            // A SUCCESSFUL batch must not attempt another state change.
            throw new IllegalStateException("A " + this.finalState.get() + " batch must not attempt another state change to " + tryFinalState);
        }
        return false;
    }

    private void completeFutureAndFireCallbacks(
            long baseOffset,
            long logAppendTime,
            Function<Integer, RuntimeException> recordExceptions
    ) {
        // Set the future before invoking the callbacks as we rely on its state for the `onCompletion` call
        // 在调用回调之前更新 future，因为我们依赖它的状态来调用 `onCompletion` 方法
        produceFuture.set(baseOffset, logAppendTime, recordExceptions);

        // execute callbacks
        // 执行回调
        for (int i = 0; i < thunks.size(); i++) {
            try {
                Thunk thunk = thunks.get(i);
                if (thunk.callback != null) {
                    if (recordExceptions == null) {
                        RecordMetadata metadata = thunk.future.value();
                        thunk.callback.onCompletion(metadata, null);
                    } else {
                        RuntimeException exception = recordExceptions.apply(i);
                        thunk.callback.onCompletion(null, exception);
                    }
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        // 标记批次已经完成（不一定是成功完成）
        produceFuture.done();
    }

    /**
     * 将当前 batch 拆分为多个小的 batch。
     * 
     * @param splitBatchSize 每个小 batch 的最大大小
     * @return 拆分后的小 batch 列表
     */
    public Deque<ProducerBatch> split(int splitBatchSize) {
        Deque<ProducerBatch> batches = new ArrayDeque<>();
        // 关闭当前 batch 的记录构建器，并返回其记录的 MemoryRecords 对象
        MemoryRecords memoryRecords = recordsBuilder.build();

        // 将 memoryRecords 转为迭代器
        Iterator<MutableRecordBatch> recordBatchIter = memoryRecords.batches().iterator();
        // 如果 memoryRecords 为空，则抛出异常
        if (!recordBatchIter.hasNext())
            throw new IllegalStateException("Cannot split an empty producer batch.");

        // 尝试获取一个 RecordBatch 对象，并检查其 magic 值和压缩状态
        RecordBatch recordBatch = recordBatchIter.next();
        if (recordBatch.magic() < MAGIC_VALUE_V2 && !recordBatch.isCompressed())
            throw new IllegalArgumentException("Batch splitting cannot be used with non-compressed messages " +
                    "with version v0 and v1");

        if (recordBatchIter.hasNext())
            throw new IllegalArgumentException("A producer batch should only have one record batch.");

        // 获取 thunks 迭代器
        Iterator<Thunk> thunkIter = thunks.iterator();
        // We always allocate batch size because we are already splitting a big batch.
        // And we also Retain the create time of the original batch.
        // 我们总是分配 batch size，因为我们正在拆分一个大 batch。
        // 我们还保留了原始 batch 的创建时间。
        ProducerBatch batch = null;

        // 遍历 recordBatch 中的每个 record
        for (Record record : recordBatch) {
            assert thunkIter.hasNext();
            // 找到这个 record 对应的 thunk
            Thunk thunk = thunkIter.next();
            // 如果此时 batch 为空，则创建一个新的 batch
            if (batch == null)
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);

            // A newly created batch can always host the first message.
            // 一个新创建的 batch 可以始终容纳第一个消息。
            if (!batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk)) {
                // 如果当前 batch 无法容纳 record，则将当前 batch 添加到 batches 列表中，并关闭当前 batch
                batches.add(batch);
                batch.closeForRecordAppends();
                // 创建一个新的 batch
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);
                batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk);
            }
        }

        // Close the last batch and add it to the batch list after split.
        // 关闭最后一个 batch，并在拆分后将其添加到 batch 列表中。
        if (batch != null) {
            batches.add(batch);
            batch.closeForRecordAppends();
        }

        // 更新当前这个 batch 的 future，表示当前 batch 已经完成
        produceFuture.set(ProduceResponse.INVALID_OFFSET, NO_TIMESTAMP, index -> new RecordBatchTooLargeException());
        // 标记当前 batch 完成
        produceFuture.done();

        if (hasSequence()) {
            // 如果当前 batch 有 sequence，则设置每个小 batch 的生产者状态
            int sequence = baseSequence();
            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId(), producerEpoch());
            for (ProducerBatch newBatch : batches) {
                newBatch.setProducerState(producerIdAndEpoch, sequence, isTransactional());
                sequence += newBatch.recordCount;
            }
        }
        return batches;
    }

    private ProducerBatch createBatchOffAccumulatorForRecord(Record record, int batchSize) {
        // 计算初始大小
        int initialSize = Math.max(AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                recordsBuilder.compressionType(), record.key(), record.value(), record.headers()), batchSize);
        // 分配一个 ByteBuffer 对象，用于存储 record
        // 这个 buffer 不是从 BufferPool 中获取的，而是直接在堆上分配的
        ByteBuffer buffer = ByteBuffer.allocate(initialSize);

        // Note that we intentionally do not set producer state (producerId, epoch, sequence, and isTransactional)
        // for the newly created batch. This will be set when the batch is dequeued for sending (which is consistent
        // with how normal batches are handled).
        // 注意：我们有意不设置新创建的 batch 的生产者状态（producerId、epoch、sequence 和 isTransactional）。
        // 这个状态将在 batch 被出队发送时设置（这与普通 batch 的处理方式一致）。
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic(), recordsBuilder.compressionType(),
                TimestampType.CREATE_TIME, 0L);
        return new ProducerBatch(topicPartition, builder, this.createdMs, true);
    }

    public boolean isCompressed() {
        return recordsBuilder.compressionType() != CompressionType.NONE;
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     * <p>
     * 一个回调函数和与之关联的 FutureRecordMetadata 参数。
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "ProducerBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    boolean hasReachedDeliveryTimeout(long deliveryTimeoutMs, long now) {
        return deliveryTimeoutMs <= now - this.createdMs;
    }

    public FinalState finalState() {
        return this.finalState.get();
    }

    int attempts() {
        return attempts.get();
    }

    void reenqueued(long now) {
        // 重试次数加 1
        attempts.getAndIncrement();
        // 更新 lastAttemptMs 和 lastAppendTime
        lastAttemptMs = Math.max(lastAppendTime, now);
        lastAppendTime = Math.max(lastAppendTime, now);
        // 标记为重试
        retry = true;
    }

    long queueTimeMs() {
        return drainedMs - createdMs;
    }

    long waitedTimeMs(long nowMs) {
        return Math.max(0, nowMs - lastAttemptMs);
    }

    void drained(long nowMs) {
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    boolean isSplitBatch() {
        return isSplitBatch;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     * <p>
     * 返回 batch 是否正在重试发送
     */
    public boolean inRetry() {

        return this.retry;
    }

    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    public int estimatedSizeInBytes() {
        return recordsBuilder.estimatedSizeInBytes();
    }

    public double compressionRatio() {
        return recordsBuilder.compressionRatio();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    public void resetProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}",
                this.baseSequence(), this.topicPartition, baseSequence);
        reopened = true;
        recordsBuilder.reopenAndRewriteProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     * <p>
     * 释放 append record 所需的资源（例如压缩缓冲区）。
     * 一旦调用此方法，就只能更新 RecordBatch 标头，无法再 append record 了。
     */
    public void closeForRecordAppends() {
        recordsBuilder.closeForRecordAppends();
    }

    public void close() {
        recordsBuilder.close();
        if (!recordsBuilder.isControlBatch()) {
            CompressionRatioEstimator.updateEstimation(topicPartition.topic(),
                    recordsBuilder.compressionType(),
                    (float) recordsBuilder.compressionRatio());
        }
        reopened = false;
    }

    /**
     * Abort the record builder and reset the state of the underlying buffer. This is used prior to aborting
     * the batch with {@link #abort(RuntimeException)} and ensures that no record previously appended can be
     * read. This is used in scenarios where we want to ensure a batch ultimately gets aborted, but in which
     * it is not safe to invoke the completion callbacks (e.g. because we are holding a lock, such as
     * when aborting batches in {@link RecordAccumulator}).
     * <p>
     * 中止 record builder 并重置底层 ByteBuffer 的状态。
     * 它会在调用 {@link #abort(RuntimeException)} 之前使用，并确保无法读取先前附加的任何记录。
     * 该方法的调用场景是：我们希望中止当前 batch，但是不去调用 callback 方法
     */
    public void abortRecordAppends() {
        recordsBuilder.abort();
    }

    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

    public byte magic() {
        return recordsBuilder.magic();
    }

    public long producerId() {
        return recordsBuilder.producerId();
    }

    public short producerEpoch() {
        return recordsBuilder.producerEpoch();
    }

    public int baseSequence() {
        return recordsBuilder.baseSequence();
    }

    public int lastSequence() {
        return recordsBuilder.baseSequence() + recordsBuilder.numRecords() - 1;
    }

    public boolean hasSequence() {
        return baseSequence() != RecordBatch.NO_SEQUENCE;
    }

    public boolean isTransactional() {
        return recordsBuilder.isTransactional();
    }

    public boolean sequenceHasBeenReset() {
        return reopened;
    }
}
