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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>The range assignor works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumers in lexicographic order. We then divide the number of partitions by the total number of
 * consumers to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition.
 * <p>
 *     RangeAssignor 是 Kafka 的默认分区分配策略，它会将每个 topic 的所有分区按照分区编号排序，将所有的消费者按照消费者 ID 排序，
 *     然后将每个消费者分配到一个或多个连续的分区。如果分区数不能被消费者数整除，那么前几个消费者会多分配一个分区。
 *
 * <p>For example, suppose there are two consumers <code>C0</code> and <code>C1</code>, two topics <code>t0</code> and
 * <code>t1</code>, and each topic has 3 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>,
 * <code>t0p2</code>, <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p1, t1p0, t1p1]</code></li>
 * <li><code>C1: [t0p2, t1p2]</code></li>
 * </ul>
 * <p>
 *     例如，假设有两个消费者 C0 和 C1，两个 topic t0 和 t1，每个 topic 有 3 个分区，那么分区的编号分别为 t0p0、t0p1、t0p2、t1p0、t1p1 和 t1p2。
 *     RangeAssignor 会将分区按照编号排序，将消费者按照 ID 排序，然后将每个消费者分配到一个或多个连续的分区。如果分区数不能被消费者数整除，那么前几个消费者会多分配一个分区。
 *     那么分配结果如下：
 *     C0: [t0p0, t0p1, t1p0, t1p1]
 *     C1: [t0p2, t1p2]
 *
 * Since the introduction of static membership, we could leverage <code>group.instance.id</code> to make the assignment behavior more sticky.
 * For the above example, after one rolling bounce, group coordinator will attempt to assign new <code>member.id</code> towards consumers,
 * for example <code>C0</code> -&gt; <code>C3</code> <code>C1</code> -&gt; <code>C2</code>.
 * <p>
 *     在引入静态成员身份之后，我们可以使用 group.instance.id 来使分区分配更加稳定。
 *     例如，对于上面的例子，在一个滚动重启之后，组协调器会尝试将新的 member.id 配给消费者，例如 C0 -> C3，C1 -> C2。
 *
 * <p>The assignment could be completely shuffled to:
 * <ul>
 * <li><code>C3 (was C0): [t0p2, t1p2] (before was [t0p0, t0p1, t1p0, t1p1])</code>
 * <li><code>C2 (was C1): [t0p0, t0p1, t1p0, t1p1] (before was [t0p2, t1p2])</code>
 * </ul>
 * <p>
 *     那么分配结果可能会完全被打乱，如下：
 *     C3 (原来是 C0)：[t0p2, t1p2]（原来是 [t0p0, t0p1, t1p0, t1p1]）
 *     C2 (原来是 C1)：[t0p0, t0p1, t1p0, t1p1]（原来是 [t0p2, t1p2]）
 *
 * The assignment change was caused by the change of <code>member.id</code> relative order, and
 * can be avoided by setting the group.instance.id.
 * Consumers will have individual instance ids <code>I1</code>, <code>I2</code>. As long as
 * 1. Number of members remain the same across generation
 * 2. Static members' identities persist across generation
 * 3. Subscription pattern doesn't change for any member
 * <p>
 *     分配结果的变化是由于 member.id 的相对顺序发生了变化，可以通过设置 group.instance.id 来避免这种变化。
 *     消费者会有独立的实例 ID I1、I2。只要满足以下条件，分配结果就不会发生变化：
 *     1. 每一代中成员数量保持不变
 *     2. 静态成员的身份在每一代中保持不变
 *     3. 任何成员的订阅模式都不发生变化
 *
 * <p>The assignment will always be:
 * <ul>
 * <li><code>I0: [t0p0, t0p1, t1p0, t1p1]</code>
 * <li><code>I1: [t0p2, t1p2]</code>
 * </ul>
 * <p>
 *     那么分配结果将始终如下：
 *     I0: [t0p0, t0p1, t1p0, t1p1]
 *     I1: [t0p2, t1p2]
 */
public class RangeAssignor extends AbstractPartitionAssignor {
    public static final String RANGE_ASSIGNOR_NAME = "range";

    @Override
    public String name() {
        return RANGE_ASSIGNOR_NAME;
    }

    private Map<String, List<MemberInfo>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<MemberInfo>> topicToConsumers = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            MemberInfo memberInfo = new MemberInfo(consumerId, subscriptionEntry.getValue().groupInstanceId());
            for (String topic : subscriptionEntry.getValue().topics()) {
                put(topicToConsumers, topic, memberInfo);
            }
        }
        return topicToConsumers;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        // 统计每个 topic 有哪些 consumer 关注
        Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);

        // 准备 assignment map 来记录分配结果
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<>());

        for (Map.Entry<String, List<MemberInfo>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<MemberInfo> consumersForTopic = topicEntry.getValue();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null)
                continue;

            Collections.sort(consumersForTopic);

            // 计算每个 consumer 平均被分配到几个 partition
            int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();
            // 计算剩下几个 partition 无法被平均分配
            int consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();

            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
            for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
                // 切割一下 partition，分配给每个 consumer
                int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
                int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
                assignment.get(consumersForTopic.get(i).memberId).addAll(partitions.subList(start, start + length));
            }
        }
        return assignment;
    }
}
