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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

/**
 * This interface is used to define custom partition assignment for use in
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer}. Members of the consumer group subscribe
 * to the topics they are interested in and forward their subscriptions to a Kafka broker serving
 * as the group coordinator. The coordinator selects one member to perform the group assignment and
 * propagates the subscriptions of all members to it. Then {@link #assign(Cluster, GroupSubscription)} is called
 * to perform the assignment and the results are forwarded back to each respective members
 * <p>
 * 这个接口用于定义 {@link org.apache.kafka.clients.consumer.KafkaConsumer} 的自定义分区分配。
 * consumer group 中的成员订阅他们感兴趣的 topic，并将他们的订阅转发给充当 group coordinator 的 Kafka broker。
 * coordinator 选择一个成员执行 group 分配，并将所有成员的订阅传播给它。
 * 然后调用 {@link #assign(Cluster, GroupSubscription)} 来执行分配，并将结果转发回每个相应的成员。
 *
 * In some cases, it is useful to forward additional metadata to the assignor in order to make
 * assignment decisions. For this, you can override {@link #subscriptionUserData(Set)} and provide custom
 * userData in the returned Subscription. For example, to have a rack-aware assignor, an implementation
 * can use this user data to forward the rackId belonging to each member.
 * <p>
 * 在某些情况下，使用额外的元数据来执行分配决策是有用的。
 * 为此，可以重写 {@link #subscriptionUserData(Set)} 并提供自定义的用户数据。
 * 例如，要实现一个 rack-aware assignor，可以在此方法中使用用户数据来转发每个成员的 rackId。
 */
public interface ConsumerPartitionAssignor {

    /**
     * Return serialized data that will be included in the {@link Subscription} sent to the leader
     * and can be leveraged in {@link #assign(Cluster, GroupSubscription)} ((e.g. local host/rack information)
     *
     * @param topics Topics subscribed to through {@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(java.util.Collection)}
     *               and variants
     * @return nullable subscription user data
     */
    default ByteBuffer subscriptionUserData(Set<String> topics) {
        return null;
    }

    /**
     * Perform the group assignment given the member subscriptions and current cluster metadata.
     * <p>
     *     给定成员订阅和当前集群元数据，执行 group 分配。
     *
     * @param metadata Current topic/broker metadata known by consumer
     * @param groupSubscription Subscriptions from all members including metadata provided through {@link #subscriptionUserData(Set)}
     * @return A map from the members to their respective assignments. This should have one entry
     *         for each member in the input subscription map.
     */
    GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription);

    /**
     * Callback which is invoked when a group member receives its assignment from the leader.
     * <p>
     *     当 group 成员从 leader 接收到分配时调用的回调。
     *
     * @param assignment The local member's assignment as provided by the leader in {@link #assign(Cluster, GroupSubscription)}
     * @param metadata Additional metadata on the consumer (optional)
     */
    default void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
    }

    /**
     * Indicate which rebalance protocol this assignor works with;
     * <p>
     *     指示此 assignor 使用哪种 rebalance protocol；
     * By default it should always work with {@link RebalanceProtocol#EAGER}.
     */
    default List<RebalanceProtocol> supportedProtocols() {
        return Collections.singletonList(RebalanceProtocol.EAGER);
    }

    /**
     * Return the version of the assignor which indicates how the user metadata encodings
     * and the assignment algorithm gets evolved.
     */
    default short version() {
        return (short) 0;
    }

    /**
     * Unique name for this assignor (e.g. "range" or "roundrobin" or "sticky"). Note, this is not required
     * to be the same as the class name specified in {@link ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
     * @return non-null unique name
     */
    String name();

    /**
     * 封装了 consumer 的订阅信息
     */
    final class Subscription {
        // 订阅的 topic 列表
        private final List<String> topics;
        // 用户数据
        private final ByteBuffer userData;
        // 上一次分配到的分区
        private final List<TopicPartition> ownedPartitions;
        // 实例 id
        private Optional<String> groupInstanceId;

        public Subscription(List<String> topics, ByteBuffer userData, List<TopicPartition> ownedPartitions) {
            this.topics = topics;
            this.userData = userData;
            this.ownedPartitions = ownedPartitions;
            this.groupInstanceId = Optional.empty();
        }

        public Subscription(List<String> topics, ByteBuffer userData) {
            this(topics, userData, Collections.emptyList());
        }

        public Subscription(List<String> topics) {
            this(topics, null, Collections.emptyList());
        }

        public List<String> topics() {
            return topics;
        }

        public ByteBuffer userData() {
            return userData;
        }

        public List<TopicPartition> ownedPartitions() {
            return ownedPartitions;
        }

        public void setGroupInstanceId(Optional<String> groupInstanceId) {
            this.groupInstanceId = groupInstanceId;
        }

        public Optional<String> groupInstanceId() {
            return groupInstanceId;
        }

        @Override
        public String toString() {
            return "Subscription(" +
                "topics=" + topics +
                (userData == null ? "" : ", userDataSize=" + userData.remaining()) +
                ", ownedPartitions=" + ownedPartitions +
                ", groupInstanceId=" + (groupInstanceId.map(String::toString).orElse("null")) +
                ")";
        }
    }

    /**
     * 封装了分区分配信息
     */
    final class Assignment {
        private List<TopicPartition> partitions;
        private ByteBuffer userData;

        public Assignment(List<TopicPartition> partitions, ByteBuffer userData) {
            this.partitions = partitions;
            this.userData = userData;
        }

        public Assignment(List<TopicPartition> partitions) {
            this(partitions, null);
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Assignment(" +
                "partitions=" + partitions +
                (userData == null ? "" : ", userDataSize=" + userData.remaining()) +
                ')';
        }
    }

    /**
     * 封装了 consumer group 中所有 consumer 的订阅信息
     */
    final class GroupSubscription {
        // key=consumer id, value=订阅信息
        private final Map<String, Subscription> subscriptions;

        public GroupSubscription(Map<String, Subscription> subscriptions) {
            this.subscriptions = subscriptions;
        }

        public Map<String, Subscription> groupSubscription() {
            return subscriptions;
        }

        @Override
        public String toString() {
            return "GroupSubscription(" +
                "subscriptions=" + subscriptions +
                ")";
        }
    }

    /**
     * 封装了 consumer group 中所有 consumer 的分区分配信息
     */
    final class GroupAssignment {
        // key=consumer id, value=分区分配信息
        private final Map<String, Assignment> assignments;

        public GroupAssignment(Map<String, Assignment> assignments) {
            this.assignments = assignments;
        }

        public Map<String, Assignment> groupAssignment() {
            return assignments;
        }

        @Override
        public String toString() {
            return "GroupAssignment(" +
                "assignments=" + assignments +
                ")";
        }
    }

    /**
     * The rebalance protocol defines partition assignment and revocation semantics. The purpose is to establish a
     * consistent set of rules that all consumers in a group follow in order to transfer ownership of a partition.
     * {@link ConsumerPartitionAssignor} implementors can claim supporting one or more rebalance protocols via the
     * {@link ConsumerPartitionAssignor#supportedProtocols()}, and it is their responsibility to respect the rules
     * of those protocols in their {@link ConsumerPartitionAssignor#assign(Cluster, GroupSubscription)} implementations.
     * Failures to follow the rules of the supported protocols would lead to runtime error or undefined behavior.
     * <p>
     * rebalance protocol 定义了 partition assignment 和 revocation 的语义。
     * 目的是建立一组一致的规则，所有消费者在 group 中遵循，以便转移 partition 的所有权。
     * {@link ConsumerPartitionAssignor} 实现者可以通过 {@link ConsumerPartitionAssignor#supportedProtocols()} 声明支持一个或多个 rebalance protocol，
     * 并负责在其实现的 {@link ConsumerPartitionAssignor#assign(Cluster, GroupSubscription)} 中遵循这些协议的规则。
     * 未能遵循协议的规则将导致运行时错误或未定义的行为。
     * </p>
     *
     * The {@link RebalanceProtocol#EAGER} rebalance protocol requires a consumer to always revoke all its owned
     * partitions before participating in a rebalance event. It therefore allows a complete reshuffling of the assignment.
     * <p>
     * {@link RebalanceProtocol#EAGER} rebalance protocol 需要一个 consumer 在参与 rebalance 事件之前，总是撤销所有它拥有的 partition。
     * 因此，允许完全重新分配。
     * </p>
     *
     * {@link RebalanceProtocol#COOPERATIVE} rebalance protocol allows a consumer to retain its currently owned
     * partitions before participating in a rebalance event. The assignor should not reassign any owned partitions
     * immediately, but instead may indicate consumers the need for partition revocation so that the revoked
     * partitions can be reassigned to other consumers in the next rebalance event. This is designed for sticky assignment
     * logic which attempts to minimize partition reassignment with cooperative adjustments.
     * <p>
     * {@link RebalanceProtocol#COOPERATIVE} rebalance protocol 允许 consumer 在参与 rebalance 事件之前，保留其当前拥有的 partition。
     * 因此，assignor 不应该立即重新分配任何已拥有的 partition，而是可以指示 consumer 需要 partition revocation，
     * 以便在下一个 rebalance 事件中将已撤销的 partition 重新分配给其他 consumer。
     * 这是为了实现最小化 partition 重新分配的 sticky assignment 逻辑，通过协作调整来实现。
     * </p>
     */
    enum RebalanceProtocol {
        /**
         * Eager rebalance protocol requires a consumer to always revoke all its owned partitions before participating in a rebalance event.
         * It therefore allows a complete reshuffling of the assignment.
         */
        EAGER((byte) 0),

        /**
         * Cooperative rebalance protocol allows a consumer to retain its currently owned partitions before participating in a rebalance event.
         * The assignor should not reassign any owned partitions immediately, but instead may indicate consumers the need for partition revocation
         * so that the revoked partitions can be reassigned to other consumers in the next rebalance event.
         */
        COOPERATIVE((byte) 1);

        private final byte id;

        RebalanceProtocol(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static RebalanceProtocol forId(byte id) {
            switch (id) {
                case 0:
                    return EAGER;
                case 1:
                    return COOPERATIVE;
                default:
                    throw new IllegalArgumentException("Unknown rebalance protocol id: " + id);
            }
        }
    }

    /**
     * Get a list of configured instances of {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor}
     * based on the class names/types specified by {@link org.apache.kafka.clients.consumer.ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
     * <p>
     *     根据 {@link org.apache.kafka.clients.consumer.ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG} 指定的类名/类型，
     */
    static List<ConsumerPartitionAssignor> getAssignorInstances(List<String> assignorClasses, Map<String, Object> configs) {
        // 帮助实例化 assignor 并调用 configure 方法
        List<ConsumerPartitionAssignor> assignors = new ArrayList<>();

        if (assignorClasses == null)
            return assignors;

        for (Object klass : assignorClasses) {
            // first try to get the class if passed in as a string
            if (klass instanceof String) {
                try {
                    klass = Class.forName((String) klass, true, Utils.getContextOrKafkaClassLoader());
                } catch (ClassNotFoundException classNotFound) {
                    throw new KafkaException(klass + " ClassNotFoundException exception occurred", classNotFound);
                }
            }

            if (klass instanceof Class<?>) {
                Object assignor = Utils.newInstance((Class<?>) klass);
                if (assignor instanceof Configurable)
                    ((Configurable) assignor).configure(configs);

                if (assignor instanceof ConsumerPartitionAssignor) {
                    assignors.add((ConsumerPartitionAssignor) assignor);
                } else {
                    throw new KafkaException(klass + " is not an instance of " + ConsumerPartitionAssignor.class.getName());
                }
            } else {
                throw new KafkaException("List contains element of type " + klass.getClass().getName() + ", expected String or Class");
            }
        }
        return assignors;
    }

}
