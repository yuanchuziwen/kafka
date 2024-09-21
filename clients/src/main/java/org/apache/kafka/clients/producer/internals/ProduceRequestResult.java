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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A class that models the future completion of a produce request for a single partition. There is one of these per
 * partition in a produce request and it is shared by all the {@link RecordMetadata} instances that are batched together
 * for the same partition in the request.
 * <p>
 *     一个类，用于模拟单个分区的生产请求的未来完成。
 *     在生产请求中，每个分区都有一个这样的请求，它由请求中为同一分区批量处理的所有 {@link RecordMetadata} 实例共享。
 */
public class ProduceRequestResult {

    // 用于管理这一批次的结果，producer 针对 send 结果的 get 都会变成这个 latch 的 await
    private final CountDownLatch latch = new CountDownLatch(1);
    private final TopicPartition topicPartition;

    private volatile Long baseOffset = null;
    private volatile long logAppendTime = RecordBatch.NO_TIMESTAMP;
    private volatile Function<Integer, RuntimeException> errorsByIndex;

    /**
     * Create an instance of this class.
     * <p>
     *     创建这个类的实例。
     *
     * @param topicPartition The topic and partition to which this record set was sent was sent
     *                       此记录集发送到的主题和分区
     */
    public ProduceRequestResult(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    /**
     * Set the result of the produce request.
     * <p>
     *     设置生产请求的结果。
     *
     * @param baseOffset The base offset assigned to the record
     *                   分配给记录的基本偏移量
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     *                      日志追加时间，如果使用 CreateTime，则为 -1
     * @param errorsByIndex Function mapping the batch index to the exception, or null if the response was successful
     *                      将批次索引映射到异常的函数，如果响应成功，则为 null
     */
    public void set(long baseOffset, long logAppendTime, Function<Integer, RuntimeException> errorsByIndex) {
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.errorsByIndex = errorsByIndex;
    }

    /**
     * Mark this request as complete and unblock any threads waiting on its completion.
     * <p>
     *     将此请求标记为完成，并解除等待其完成的任何线程的阻塞。
     */
    public void done() {
        if (baseOffset == null)
            throw new IllegalStateException("The method `set` must be invoked before this method.");
        this.latch.countDown();
    }

    /**
     * Await the completion of this request
     * <p>
     *     等待此请求完成
     */
    public void await() throws InterruptedException {
        latch.await();
    }

    /**
     * Await the completion of this request (up to the given time interval)
     * <p>
     *     等待此请求完成（最多到给定的时间间隔）
     *
     * @param timeout The maximum time to wait
     *                等待的最大时间
     * @param unit The unit for the max time
     *             最大时间的单位
     * @return true if the request completed, false if we timed out
     *        如果请求完成，则为 true；如果超时，则为 false
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    /**
     * The base offset for the request (the first offset in the record set)
     * <p>
     *     请求的基本偏移量（记录集中的第一个偏移量）
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * Return true if log append time is being used for this topic
     * <p>
     *     如果为此主题使用日志追加时间，则返回 true
     */
    public boolean hasLogAppendTime() {
        return logAppendTime != RecordBatch.NO_TIMESTAMP;
    }

    /**
     * The log append time or -1 if CreateTime is being used
     * <p>
     *     日志追加时间，如果使用 CreateTime，则为 -1
     */
    public long logAppendTime() {
        return logAppendTime;
    }

    /**
     * The error thrown (generally on the server) while processing this request
     * <p>
     *     在处理此请求时抛出的错误（通常在服务器上）
     */
    public RuntimeException error(int batchIndex) {
        if (errorsByIndex == null) {
            return null;
        } else {
            return errorsByIndex.apply(batchIndex);
        }
    }

    /**
     * The topic and partition to which the record was appended
     * <p>
     *     记录附加到的主题和分区
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * Has the request completed?
     * <p>
     *     请求是否已完成？
     */
    public boolean completed() {
        return this.latch.getCount() == 0L;
    }
}
