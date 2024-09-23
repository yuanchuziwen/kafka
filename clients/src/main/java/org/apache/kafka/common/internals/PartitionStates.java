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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * This class is a useful building block for doing fetch requests where topic partitions have to be rotated via
 * round-robin to ensure fairness and some level of determinism given the existence of a limit on the fetch response
 * size. Because the serialization of fetch requests is more efficient if all partitions for the same topic are grouped
 * together, we do such grouping in the method `set`.
 * <p>
 *     当前类是一个有用的构建块，用于执行获取请求，其中主题分区必须通过循环轮询进行旋转，以确保公平性和某种程度的确定性，因为存在对获取响应大小的限制。
 *     由于如果将同一主题的所有分区分组在一起，那么获取请求的序列化将更有效，因此我们在方法 `set` 中进行这样的分组。
 *
 * As partitions are moved to the end, the same topic may be repeated more than once. In the optimal case, a single
 * topic would "wrap around" and appear twice. However, as partitions are fetched in different orders and partition
 * leadership changes, we will deviate from the optimal. If this turns out to be an issue in practice, we can improve
 * it by tracking the partitions per node or calling `set` every so often.
 * <p>
 *     随着分区被移动到末尾，相同的主题可能会重复多次。
 *     在最佳情况下，单个主题将“环绕”并出现两次。
 *     但是，由于分区以不同的顺序获取并且分区领导权发生变化，
 *     如果这在实践中成为问题，我们可以通过跟踪每个节点的分区或定期调用 `set` 来改进它。
 *
 * Note that this class is not thread-safe with the exception of {@link #size()} which returns the number of
 * partitions currently tracked.
 * <p>
 *     请注意，除了 {@link #size()} 返回当前跟踪的分区数之外，此类不是线程安全的。
 */
public class PartitionStates<S> {

    // 实际存储的类
    private final LinkedHashMap<TopicPartition, S> map = new LinkedHashMap<>();
    // 返回的一个当时的不可变的视图
    private final Set<TopicPartition> partitionSetView = Collections.unmodifiableSet(map.keySet());

    /* the number of partitions that are currently assigned available in a thread safe manner */
    // 当前分配的分区数，以线程安全的方式可用
    private volatile int size = 0;

    public PartitionStates() {}

    public void moveToEnd(TopicPartition topicPartition) {
        S state = map.remove(topicPartition);
        if (state != null)
            map.put(topicPartition, state);
    }

    public void updateAndMoveToEnd(TopicPartition topicPartition, S state) {
        map.remove(topicPartition);
        map.put(topicPartition, state);
        updateSize();
    }

    public void remove(TopicPartition topicPartition) {
        map.remove(topicPartition);
        updateSize();
    }

    /**
     * Returns an unmodifiable view of the partitions in random order.
     * changes to this PartitionStates instance will be reflected in this view.
     * <p>
     *     以随机顺序返回分区的不可修改视图。
     *     对此 PartitionStates 实例的更改将反映在此视图中。
     */
    public Set<TopicPartition> partitionSet() {
        return partitionSetView;
    }

    public void clear() {
        map.clear();
        updateSize();
    }

    public boolean contains(TopicPartition topicPartition) {
        return map.containsKey(topicPartition);
    }

    public Iterator<S> stateIterator() {
        return map.values().iterator();
    }

    public void forEach(BiConsumer<TopicPartition, S> biConsumer) {
        map.forEach(biConsumer);
    }

    public Map<TopicPartition, S> partitionStateMap() {
        return Collections.unmodifiableMap(map);
    }

    /**
     * Returns the partition state values in order.
     */
    public List<S> partitionStateValues() {
        return new ArrayList<>(map.values());
    }

    public S stateValue(TopicPartition topicPartition) {
        return map.get(topicPartition);
    }

    /**
     * Get the number of partitions that are currently being tracked. This is thread-safe.
     */
    public int size() {
        return size;
    }

    /**
     * Update the builder to have the received map as its state (i.e. the previous state is cleared). The builder will
     * "batch by topic", so if we have a, b and c, each with two partitions, we may end up with something like the
     * following (the order of topics and partitions within topics is dependent on the iteration order of the received
     * map): a0, a1, b1, b0, c0, c1.
     * <p>
     *     更新构建器以具有接收到的映射作为其状态（即清除先前的状态）。
     *     生成器将“按主题批处理”，因此如果我们有 a、b 和 c，每个主题有两个分区，我们最终可能会得到类似以下的内容
     *     （主题和主题内的分区的顺序取决于接收到的映射的迭代顺序）：a0、a1、b1、b0、c0、c1。
     */
    public void set(Map<TopicPartition, S> partitionToState) {
        // 清空当前的状态
        map.clear();
        update(partitionToState);
        updateSize();
    }

    private void updateSize() {
        size = map.size();
    }

    private void update(Map<TopicPartition, S> partitionToState) {
        // 建把 partitionToState 中的数据放到一个局部变量的 LinkedHashMap 中
        // 注意这个局部变量是以 topicName 作为 key 的，value 是这个 topic 下的 partition 集合
        LinkedHashMap<String, List<TopicPartition>> topicToPartitions = new LinkedHashMap<>();
        for (TopicPartition tp : partitionToState.keySet()) {
            List<TopicPartition> partitions = topicToPartitions.computeIfAbsent(tp.topic(), k -> new ArrayList<>());
            partitions.add(tp);
        }
        // 遍历 topicToPartitions，把 partitionToState 中的数据放到 map 中
        for (Map.Entry<String, List<TopicPartition>> entry : topicToPartitions.entrySet()) {
            for (TopicPartition tp : entry.getValue()) {
                S state = partitionToState.get(tp);
                map.put(tp, state);
            }
        }
    }

    public static class PartitionState<S> {
        private final TopicPartition topicPartition;
        private final S value;

        public PartitionState(TopicPartition topicPartition, S state) {
            this.topicPartition = Objects.requireNonNull(topicPartition);
            this.value = Objects.requireNonNull(state);
        }

        public S value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PartitionState<?> that = (PartitionState<?>) o;

            return topicPartition.equals(that.topicPartition) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            int result = topicPartition.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        @Override
        public String toString() {
            return "PartitionState(" + topicPartition + "=" + value + ')';
        }
    }

}
