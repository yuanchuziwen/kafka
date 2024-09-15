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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Objects;

/**
 * A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional
 * partition number, and an optional key and value.
 * 要发送到Kafka的键/值对。
 * 它包含记录要发送到的主题名称、可选的分区号以及可选的键和值。
 * <p>
 * If a valid partition number is specified that partition will be used when sending the record. If no partition is
 * specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is
 * present a partition will be assigned in a round-robin fashion.
 * 如果指定了有效的分区号，则在发送记录时将使用该分区。
 * 如果未指定分区但存在键，则将使用键的哈希值选择分区。如果既没有键也没有分区，则将以轮询方式分配分区。
 * <p>
 * The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the
 * record with its current time. The timestamp eventually used by Kafka depends on the timestamp type configured for
 * the topic.
 * 记录还有一个关联的时间戳。如果用户没有提供时间戳，生产者将使用其当前时间为记录加上时间戳。
 * Kafka最终使用的时间戳取决于为主题配置的时间戳类型。
 * <li>
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime},
 * the timestamp in the producer record will be used by the broker.
 * 如果主题配置为使用 {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime}，
 * 则代理将使用生产者记录中的时间戳。
 * </li>
 * <li>
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime},
 * the timestamp in the producer record will be overwritten by the broker with the broker local time when it appends the
 * message to its log.
 * 如果主题配置为使用 {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime}，
 * 则代理将在将消息追加到其日志时，用代理本地时间覆盖生产者记录中的时间戳。
 * </li>
 * <p>
 * In either of the cases above, the timestamp that has actually been used will be returned to user in
 * {@link RecordMetadata}
 * 在上述任何情况下，实际使用的时间戳都将在 {@link RecordMetadata} 中返回给用户。
 */
public class ProducerRecord<K, V> {

    /**
     * 记录要发送到的主题
     */
    private final String topic;

    /**
     * 记录要发送到的分区，可为null
     */
    private final Integer partition;

    /**
     * 记录的头部信息
     */
    private final Headers headers;

    /**
     * 记录的键，可为null
     */
    private final K key;

    /**
     * 记录的值，可为null
     */
    private final V value;

    /**
     * 记录的时间戳，单位为毫秒，可为null
     */
    private final Long timestamp;

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     * 创建一个具有指定时间戳的记录，以发送到指定的主题和分区
     *
     * @param topic     The topic the record will be appended to
     *                  记录要发送到的主题
     * @param partition The partition to which the record should be sent
     *                  记录要发送到的分区
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null, the producer will assign
     *                  the timestamp using System.currentTimeMillis().
     *                  记录的时间戳，单位为毫秒。如果为 null，则生产者将使用 System.currentTimeMillis() 分配时间戳。
     * @param key       The key that will be included in the record
     *                  记录中要包含的键
     * @param value     The record contents
     *                  记录的内容
     * @param headers   the headers that will be included in the record
     *                  记录中要包含的头部信息
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        // 参数校验
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null.");
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        if (partition != null && partition < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative or null.", partition));
        // 赋值
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        // 将 Iterable<Header> 转换为 RecordHeaders
        this.headers = new RecordHeaders(headers);
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     * 创建一个具有指定时间戳的记录，以发送到指定的主题和分区
     *
     * @param topic     The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null, the producer will assign the
     *                  timestamp using System.currentTimeMillis().
     * @param key       The key that will be included in the record
     * @param value     The record contents
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this(topic, partition, timestamp, key, value, null);
    }

    /**
     * Creates a record to be sent to a specified topic and partition
     * 创建一个记录，以发送到指定的主题和分区
     *
     * @param topic     The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key       The key that will be included in the record
     * @param value     The record contents
     * @param headers   The headers that will be included in the record
     */
    public ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers) {
        this(topic, partition, null, key, value, headers);
    }

    /**
     * Creates a record to be sent to a specified topic and partition
     * 创建一个记录，以发送到指定的主题和分区
     *
     * @param topic     The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key       The key that will be included in the record
     * @param value     The record contents
     */
    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, partition, null, key, value, null);
    }

    /**
     * Create a record to be sent to Kafka
     * 创建一个记录，以发送到指定的主题
     *
     * @param topic The topic the record will be appended to
     * @param key   The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, null, key, value, null);
    }

    /**
     * Create a record with no key
     * 创建一个没有键的记录
     *
     * @param topic The topic this record should be sent to
     * @param value The record contents
     */
    public ProducerRecord(String topic, V value) {
        this(topic, null, null, null, value, null);
    }

    /**
     * @return The topic this record is being sent to
     */
    public String topic() {
        return topic;
    }

    /**
     * @return The headers
     */
    public Headers headers() {
        return headers;
    }

    /**
     * @return The key (or null if no key is specified)
     */
    public K key() {
        return key;
    }

    /**
     * @return The value
     */
    public V value() {
        return value;
    }

    /**
     * @return The timestamp, which is in milliseconds since epoch.
     */
    public Long timestamp() {
        return timestamp;
    }

    /**
     * @return The partition to which the record will be sent (or null if no partition was specified)
     */
    public Integer partition() {
        return partition;
    }

    @Override
    public String toString() {
        String headers = this.headers == null ? "null" : this.headers.toString();
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(topic=" + topic + ", partition=" + partition + ", headers=" + headers + ", key=" + key + ", value=" + value +
                ", timestamp=" + timestamp + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        else if (!(o instanceof ProducerRecord))
            return false;

        ProducerRecord<?, ?> that = (ProducerRecord<?, ?>) o;

        return Objects.equals(key, that.key) &&
                Objects.equals(partition, that.partition) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(headers, that.headers) &&
                Objects.equals(value, that.value) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
