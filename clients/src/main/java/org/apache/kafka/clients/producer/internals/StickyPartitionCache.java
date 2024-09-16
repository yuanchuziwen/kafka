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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.Utils;

/**
 * An internal class that implements a cache used for sticky partitioning behavior. The cache tracks the current sticky
 * partition for any given topic. This class should not be used externally.
 *
 * 一个内部类，实现了用于粘性分区行为的缓存。
 * 缓存跟踪任何给定主题的当前粘性分区。
 * 此类不应在外部使用。
 */
public class StickyPartitionCache {
    // 一个下一个分配分区的缓存？
    private final ConcurrentMap<String, Integer> indexCache;
    public StickyPartitionCache() {
        this.indexCache = new ConcurrentHashMap<>();
    }

    /*
    TODO
    这个 cache 的特点是：如果需要确定消息的分区，那么就指派成同一个分区；只有当这个分区对应的消息刚发送的时候，才会重新指派一个分区。
     */

    public int partition(String topic, Cluster cluster) {
        // 如果能从缓存中取到，就返回，否则调用 nextPartition 方法
        Integer part = indexCache.get(topic);
        if (part == null) {
            return nextPartition(topic, cluster, -1);
        }
        return part;
    }

    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
        // 获取 topic 的所有分区
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        // 取出之前针对这个 topic 缓存着的分区
        Integer oldPart = indexCache.get(topic);
        Integer newPart = oldPart;
        // Check that the current sticky partition for the topic is either not set or that the partition that 
        // triggered the new batch matches the sticky partition that needs to be changed.
        // 如果 oldPart 为 null 或者 oldPart 等于 prevPartition，那么就需要重新选择一个分区
        if (oldPart == null || oldPart == prevPartition) {
            // 获取到 topic 当前可用的分区
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            // 如果当前没有可用的分区，那么就根据所有的分区来 random 一个
            if (availablePartitions.size() < 1) {
                Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                newPart = random % partitions.size();

                // 如果当前只有一个可用的分区，那么就直接选择这个分区
            } else if (availablePartitions.size() == 1) {
                newPart = availablePartitions.get(0).partition();
            } else {

                // 否则，就从可用的分区中随机选择一个分区，但是不能和之前的分区相同
                while (newPart == null || newPart.equals(oldPart)) {
                    int random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                    newPart = availablePartitions.get(random % availablePartitions.size()).partition();
                }
            }
            // Only change the sticky partition if it is null or prevPartition matches the current sticky partition.
            // 如果 oldPart 为 null
            if (oldPart == null) {
                // 那么通过 putIfAbsent 方法来设置新的分区，这样能保证线程安全
                indexCache.putIfAbsent(topic, newPart);
            } else {
                // 否则，通过 replace 方法来替换旧的分区
                indexCache.replace(topic, prevPartition, newPart);
            }
            return indexCache.get(topic);
        }

        // 否则说明 oldPart 不为 null 且不等于 prevPartition
        // 进而说明它已经被其他线程更新过了，那么直接返回 oldPart 即可
        return indexCache.get(topic);
    }

}