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


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

/**
 * A container that holds the list {@link org.apache.kafka.clients.producer.ProducerInterceptor}
 * and wraps calls to the chain of custom interceptors.
 * 一个容器，包含 {@link org.apache.kafka.clients.producer.ProducerInterceptor} 列表，并包装对自定义拦截器链的调用。
 */
public class ProducerInterceptors<K, V> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ProducerInterceptors.class);
    private final List<ProducerInterceptor<K, V>> interceptors;

    public ProducerInterceptors(List<ProducerInterceptor<K, V>> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * This is called when client sends the record to KafkaProducer, before key and value gets serialized.
     * The method calls {@link ProducerInterceptor#onSend(ProducerRecord)} method. ProducerRecord
     * returned from the first interceptor's onSend() is passed to the second interceptor onSend(), and so on in the
     * interceptor chain. The record returned from the last interceptor is returned from this method.
     * 当客户端将记录发送到 KafkaProducer 时调用此方法，在键和值被序列化之前。
     * 该方法调用 {@link ProducerInterceptor#onSend(ProducerRecord)} 方法。第一个拦截器的 onSend() 返回的 ProducerRecord
     * 被传递给第二个拦截器的 onSend()，依此类推，直到拦截器链的最后一个拦截器。此方法返回最后一个拦截器返回的记录。
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     * If an interceptor in the middle of the chain, that normally modifies the record, throws an exception,
     * the next interceptor in the chain will be called with a record returned by the previous interceptor that did not
     * throw an exception.
     * 此方法不会抛出异常。任何拦截器方法抛出的异常都会被捕获并忽略。
     * 如果链中的一个拦截器通常会修改记录，但抛出了异常，
     * 链中的下一个拦截器将使用前一个未抛出异常的拦截器返回的记录。
     *
     * @param record the record from client
     *               来自客户端的记录
     * @return producer record to send to topic/partition
     *         要发送到主题/分区的生产者记录
     */
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        ProducerRecord<K, V> interceptRecord = record;
        // 遍历所有拦截器，依次调用它们的 onSend 方法
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptRecord = interceptor.onSend(interceptRecord);
            } catch (Exception e) {
                // 不传播拦截器异常，记录日志并继续调用其他拦截器
                // 注意不要从这里抛出异常
                if (record != null)
                    log.warn("Error executing interceptor onSend callback for topic: {}, partition: {}", record.topic(), record.partition(), e);
                else
                    log.warn("Error executing interceptor onSend callback", e);
            }
        }
        return interceptRecord;
    }

    /**
     * This method is called when the record sent to the server has been acknowledged, or when sending the record fails before
     * it gets sent to the server. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)}
     * method for each interceptor.
     * 当发送到服务器的记录被确认时，或者在记录发送到服务器之前发送失败时调用此方法。
     * 此方法为每个拦截器调用 {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)} 方法。
     * 
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     * 这个方法不会抛出异常。任何拦截器方法抛出的异常都会被捕获并忽略。
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
     *                 If an error occurred, metadata will only contain valid topic and maybe partition.
     *                 已经被确认的记录的元数据（即分区和偏移量）。
     *                 如果发生错误，元数据将仅包含有效的主题和可能的分区。
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     *                 处理此记录时抛出的异常。如果没有错误发生，则为 Null。
     *
     * 此方法不会抛出异常。任何拦截器方法抛出的异常都会被捕获并忽略。
     */
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 遍历所有拦截器，依次调用它们的 onAcknowledgement 方法
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptor.onAcknowledgement(metadata, exception);
            } catch (Exception e) {
                // 不传播拦截器异常，只记录日志
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * This method is called when sending the record fails in {@link ProducerInterceptor#onSend
     * (ProducerRecord)} method. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)}
     * method for each interceptor
     * 当在 {@link ProducerInterceptor#onSend(ProducerRecord)} 方法中发送记录失败时调用此方法。
     * 此方法为每个拦截器调用 {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)} 方法。
     *
     * @param record The record from client 来自客户端的记录
     * @param interceptTopicPartition  The topic/partition for the record if an error occurred 
     *        after partition gets assigned; the topic part of interceptTopicPartition is the same as in record.
     *        如果发生错误，记录的主题/分区；
     *        interceptTopicPartition 的主题部分与记录中的相同。
     * @param exception The exception thrown during processing of this record.
     *         处理此记录时抛出的异常。
     */
    public void onSendError(ProducerRecord<K, V> record, TopicPartition interceptTopicPartition, Exception exception) {
        // 遍历所有拦截器，依次调用它们的 onAcknowledgement 方法
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                if (record == null && interceptTopicPartition == null) {
                    interceptor.onAcknowledgement(null, exception);
                } else {
                    if (interceptTopicPartition == null) {
                        interceptTopicPartition = extractTopicPartition(record);
                    }
                    interceptor.onAcknowledgement(new RecordMetadata(interceptTopicPartition, -1, -1,
                                    RecordBatch.NO_TIMESTAMP, -1, -1), exception);
                }
            } catch (Exception e) {
                // 不传播拦截器异常，只记录日志
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    public static <K, V> TopicPartition extractTopicPartition(ProducerRecord<K, V> record) {
        // 从 ProducerRecord 中提取 TopicPartition
        return new TopicPartition(record.topic(), record.partition() == null ? RecordMetadata.UNKNOWN_PARTITION : record.partition());
    }

    /**
     * Closes every interceptor in a container.
     * 关闭容器中的每个拦截器。
     */
    @Override
    public void close() {
        // 遍历所有拦截器，依次调用它们的 close 方法
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptor.close();
            } catch (Exception e) {
                log.error("Failed to close producer interceptor ", e);
            }
        }
    }
}
