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

import java.util.Map;

import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;


/**
 * The partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>Otherwise choose the sticky partition that changes when the batch is full.
 *
 * 分区策略：
 * - 如果记录中指定了分区，则使用它
 * - 否则选择在批次满时更改的粘性分区。
 * 
 * NOTE: In constrast to the DefaultPartitioner, the record key is NOT used as part of the partitioning strategy in this 
 *       partitioner. Records with the same key are not guaranteed to be sent to the same partition.
 * 注意：与 DefaultPartitioner 不同，此分区器中不使用记录键作为分区策略的一部分。不能保证具有相同键的记录将发送到同一分区。
 * 
 * See KIP-480 for details about sticky partitioning.
 */
public class UniformStickyPartitioner implements Partitioner {

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    public void configure(Map<String, ?> configs) {}

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 完全依赖于 StickyPartitionCache
        return stickyPartitionCache.partition(topic, cluster);
    }

    public void close() {}
    
    /**
     * If a batch completed for the current sticky partition, change the sticky partition. 
     * Alternately, if no sticky partition has been determined, set one.
     */
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        stickyPartitionCache.nextPartition(topic, cluster, prevPartition);
    }
}
