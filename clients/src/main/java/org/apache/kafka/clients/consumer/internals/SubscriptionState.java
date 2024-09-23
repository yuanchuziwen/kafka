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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.internals.Fetcher.hasUsableOffsetForLeaderEpochVersion;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 * <p>
 * 用于跟踪消费者的主题、分区和偏移量的类。
 * 分区可以通过 {@link #assignFromUser(Set)}（手动分配）或
 *  {@link #assignFromSubscribed(Collection)}（从订阅自动分配）直接“分配”。
 * <p>
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seekValidated(TopicPartition, FetchPosition)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 * <p>
 * 一旦分配，分区在其初始位置通过 {@link #seekValidated(TopicPartition, FetchPosition)} 设置之前，
 * 不被视为“可获取”。可获取的分区跟踪一个获取位置，该位置用于设置下一次获取的偏移量，以及一个已消费的位置，
 * 即最后返回给用户的偏移量。你可以通过 {@link #pause(TopicPartition)} 暂停从分区获取数据，而不影响已获取/已消费的偏移量。
 * 分区将保持不可获取状态，直到使用 {@link #resume(TopicPartition)}。你也可以通过 {@link #isPaused(TopicPartition)} 独立查询暂停状态。
 * <p>
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 * <p>
 * 请注意，当分区分配被用户直接更改或通过组再平衡更改时，暂停状态以及获取/消费位置不会被保留。
 * <p>
 * Thread Safety: this class is thread-safe.
 * <p>
 * 线程安全：这个类是线程安全的。
 */
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    private final Logger log;

    /* the partitions that are currently assigned, note that the order of partition matters (see FetchBuilder for more details) */
    /* 当前分配的分区，注意分区的顺序很重要（详见 FetchBuilder） */
    private final PartitionStates<TopicPartitionState> assignment;

    /* Default offset reset strategy */
    /* 默认的偏移量重置策略 */
    private final OffsetResetStrategy defaultResetStrategy;

    /* the type of subscription */
    /* 订阅类型 */
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    /* 用户请求的模式 */
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    /* 用户请求的主题列表 */
    private Set<String> subscription;

    /* The list of topics the group has subscribed to. This may include some topics which are not part
     * of `subscription` for the leader of a group since it is responsible for detecting metadata changes
     * which require a group rebalance. */
    /* 消费者组已订阅的 topic 列表。
    对于 group leader，这可能包括一些不属于 `subscription` 的主题，
    因为它负责检测需要组再平衡的 metadata 更新。 */
    private Set<String> groupSubscription;

    /* User-provided listener to be invoked when assignment changes */
    /* 用户提供的在分配更改时调用的 listener */
    private ConsumerRebalanceListener rebalanceListener;

    /* 分配 ID */
    private int assignmentId = 0;

    /**
     * Monotonically increasing id which is incremented after every assignment change. This can
     * be used to check when an assignment has changed.
     * <p>
     *     单调递增的 ID，在每次分配更改后递增。
     *     这可以用来检查分配是否发生了变化。
     *
     * @return The current assignment Id
     */
    synchronized int assignmentId() {
        return assignmentId;
    }

    @Override
    public synchronized String toString() {
        return "SubscriptionState{" +
            "type=" + subscriptionType +
            ", subscribedPattern=" + subscribedPattern +
            ", subscription=" + String.join(",", subscription) +
            ", groupSubscription=" + String.join(",", groupSubscription) +
            ", defaultResetStrategy=" + defaultResetStrategy +
            ", assignment=" + assignment.partitionStateValues() + " (id=" + assignmentId + ")}";
    }

    public synchronized String prettyString() {
        switch (subscriptionType) {
            case NONE:
                return "None";
            case AUTO_TOPICS:
                return "Subscribe(" + String.join(",", subscription) + ")";
            case AUTO_PATTERN:
                return "Subscribe(" + subscribedPattern + ")";
            case USER_ASSIGNED:
                return "Assign(" + assignedPartitions() + " , id=" + assignmentId + ")";
            default:
                throw new IllegalStateException("Unrecognized subscription type: " + subscriptionType);
        }
    }

    public SubscriptionState(LogContext logContext, OffsetResetStrategy defaultResetStrategy) {
        this.log = logContext.logger(this.getClass());
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     * <p>
     *     这个方法在订阅类型未设置时（即为 NONE）设置订阅类型，
     *     或在订阅类型已设置时（即不为 NONE）验证订阅类型是否等于给定类型。
     *
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        // 如果订阅类型为 NONE，则设置为给定类型
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        // 否则，校验当前订阅类型是否等于给定类型
        // 这意味着，如果已经确定订阅类型，则不能再改变；即不可能同时使用 AUTO_TOPICS 和 AUTO_PATTERN
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    // 订阅类型
    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
    }

    /**
     * 订阅给定的 topic 列表。
     *
     * @param topics 要订阅的 topic 列表
     * @param listener 在分配更改时调用的 listener
     * @return 如果订阅成功则返回 true，否则返回 false
     */
    public synchronized boolean subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
        /* 注册 rebalance listener */
        registerRebalanceListener(listener);
        /* 设置订阅类型为 AUTO_TOPICS */
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);
        /* 改变订阅 */
        return changeSubscription(topics);
    }

    /**
     * 订阅给定的 pattern。
     *
     * @param pattern 要订阅的 pattern
     * @param listener 在分配更改时调用的 listener
     */
    public synchronized void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        /* 注册 rebalance listener */
        registerRebalanceListener(listener);
        /* 设置订阅类型为 AUTO_PATTERN */
        setSubscriptionType(SubscriptionType.AUTO_PATTERN);
        /* 设置订阅的 pattern */
        this.subscribedPattern = pattern;
    }

    public synchronized boolean subscribeFromPattern(Set<String> topics) {
        if (subscriptionType != SubscriptionType.AUTO_PATTERN)
            throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);

        return changeSubscription(topics);
    }

    /**
     * 改变订阅
     *
     * @param topicsToSubscribe 要订阅的 topic 列表
     * @return 如果订阅成功则返回 true，否则返回 false
     */
    private boolean changeSubscription(Set<String> topicsToSubscribe) {
        // 如果当前订阅的 topic 列表与要订阅的 topic 列表相同，则返回 false
        if (subscription.equals(topicsToSubscribe))
            return false;

        // 否则，更新订阅的 topic 列表
        subscription = topicsToSubscribe;
        return true;
    }

    /**
     * Set the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     * <p>
     *     设置当前的 group 订阅。
     *     这由 group leader 调用，以确保接收所有 group 感兴趣的 topic 的 metadata 更新。
     *
     * @param topics All topics from the group subscription
     *               所有来自 group 订阅的 topic
     * @return true if the group subscription contains topics which are not part of the local subscription
     *        如果 group 订阅包含本地订阅中不存在的 topic，则返回 true
     */
    synchronized boolean groupSubscribe(Collection<String> topics) {
        // 如果当前没有自动分配的分区，则抛出异常，即 state 不是 AUTO_TOPICS 或 AUTO_PATTERN
        if (!hasAutoAssignedPartitions())
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        // 更新 group 订阅
        groupSubscription = new HashSet<>(topics);
        return !subscription.containsAll(groupSubscription);
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     * <p>
     *     将 group 的订阅重置为仅包含此消费者订阅的 topic。
     */
    synchronized void resetGroupSubscription() {
        groupSubscription = Collections.emptySet();
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     * <p>
     *     改变分配为指定的分区，这些分区由用户提供，
     *     注意这与 {@link #assignFromSubscribed(Collection)} 不同，
     *     后者从订阅的 topic 中获取分区。
     *
     * @param partitions 要分配的分区
     * @return 如果分配成功则返回 true，否则返回 false
     */
    public synchronized boolean assignFromUser(Set<TopicPartition> partitions) {
        // 设置订阅类型为 USER_ASSIGNED
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        // 如果当前分配的分区与要分配的分区相同，则返回 false
        if (this.assignment.partitionSet().equals(partitions))
            return false;

        // 更新分配 ID
        assignmentId++;

        // 更新订阅的 topic
        Set<String> manualSubscribedTopics = new HashSet<>();
        Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
        for (TopicPartition partition : partitions) {
            // 获取分区状态
            TopicPartitionState state = assignment.stateValue(partition);
            // 如果分区状态为 null，则创建一个新的分区状态
            if (state == null)
                state = new TopicPartitionState();
            // 将分区状态添加到分区状态映射中
            partitionToState.put(partition, state);
            // 将分区 topic 添加到手动订阅的 topic 列表中
            manualSubscribedTopics.add(partition.topic());
        }

        // 更新分配 
        this.assignment.set(partitionToState);
        // 更新订阅的 topic
        return changeSubscription(manualSubscribedTopics);
    }

    /**
     * @return true if assignments matches subscription, otherwise false
     * 检查分配是否匹配订阅
     */
    public synchronized boolean checkAssignmentMatchedSubscription(Collection<TopicPartition> assignments) {
        for (TopicPartition topicPartition : assignments) {
            // 如果是以 pattern 订阅
            if (this.subscribedPattern != null) {
                // 检查分区 topic 是否匹配订阅的 pattern
                if (!this.subscribedPattern.matcher(topicPartition.topic()).matches()) {
                    log.info("Assigned partition {} for non-subscribed topic regex pattern; subscription pattern is {}",
                        topicPartition,
                        this.subscribedPattern);

                    return false;
                }

                // 如果是以 topic 订阅
            } else {
                // 检查分区 topic 是否在订阅的 topic 列表中
                if (!this.subscription.contains(topicPartition.topic())) {
                    log.info("Assigned partition {} for non-subscribed topic; subscription is {}", topicPartition, this.subscription);

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator, note this is
     * different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs.
     * <p>
     *     改变分配为指定的分区，这些分区从 coordinator 返回，
     *     注意这与 {@link #assignFromUser(Set)} 不同，
     *     后者直接从用户输入设置分配。
     *
     * @param assignments 要分配的分区
     */
    public synchronized void assignFromSubscribed(Collection<TopicPartition> assignments) {
        // 如果当前没有自动分配的分区，则抛出异常，即 state 不是 AUTO_TOPICS 或 AUTO_PATTERN
        if (!this.hasAutoAssignedPartitions())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");

        // 创建分区状态映射
        Map<TopicPartition, TopicPartitionState> assignedPartitionStates = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments) {
            TopicPartitionState state = this.assignment.stateValue(tp);
            if (state == null)
                state = new TopicPartitionState();
            // 将分区状态添加到分区状态映射中
            assignedPartitionStates.put(tp, state);
        }

        // 更新分配
        assignmentId++;
        this.assignment.set(assignedPartitionStates);
    }

    private void registerRebalanceListener(ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        // 这里会覆盖掉之前注册的 listener
        this.rebalanceListener = listener;
    }

    /**
     * 检查是否使用 pattern 订阅。
     *
     * @return 如果使用 pattern 订阅则返回 true，否则返回 false
     */
    synchronized boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public synchronized boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    public synchronized void unsubscribe() {
        // 清空订阅的 topic 列表
        this.subscription = Collections.emptySet();
        // 清空 group 订阅的 topic 列表
        this.groupSubscription = Collections.emptySet();
        // 清空分配
        this.assignment.clear();
        // 清空订阅的 pattern
        this.subscribedPattern = null;
        // 设置订阅类型为 NONE
        this.subscriptionType = SubscriptionType.NONE;
        // 更新分配 ID
        this.assignmentId++;
    }

    /**
     * Check whether a topic matches a subscribed pattern.
     * <p>
     *     检查 topic 是否匹配订阅的 pattern。
     *
     * @return 如果使用 pattern 订阅且 topic 匹配订阅的 pattern，则返回 true，否则返回 false
     */
    synchronized boolean matchesSubscribedPattern(String topic) {
        // 获取订阅的 pattern
        Pattern pattern = this.subscribedPattern;
        // 如果使用 pattern 订阅且 pattern 不为 null，则检查 topic 是否匹配 pattern
        if (hasPatternSubscription() && pattern != null)
            return pattern.matcher(topic).matches();
        return false;
    }

    /**
     * 获取订阅的 topic 列表。
     *
     * @return 如果使用自动分配的分区，则返回订阅的 topic 列表，否则返回空集合
     */
    public synchronized Set<String> subscription() {
        // 如果使用自动分配的分区，则返回订阅的 topic 列表
        if (hasAutoAssignedPartitions())
            return this.subscription;
        // 否则，返回空集合
        return Collections.emptySet();
    }

    /**
     * 获取暂停的分区列表。
     *
     * @return 返回所有处于暂停状态的分区列表
     */
    public synchronized Set<TopicPartition> pausedPartitions() {
        // 收集所有处于暂停状态的分区
        return collectPartitions(TopicPartitionState::isPaused);
    }

    /**
     * Get the subscription topics for which metadata is required. For the leader, this will include
     * the union of the subscriptions of all group members. For followers, it is just that member's
     * subscription. This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     * <p>
     *     获取需要元数据的订阅 topic。对于 leader，这将是所有 group 成员的订阅的并集。
     *     对于 followers，这只是该成员的订阅。
     *     在查询 topic 元数据时使用，以检测需要重新平衡的元数据更改。
     *     leader 为所有 group 成员获取所有订阅 topic 的元数据，以便进行分区分配（需要分配所有 topic 的分区计数）。
     *
     * @return 如果 this member 是当前 generation 的 leader，则返回所有订阅 topic 的并集；
     *         否则返回 {@link #subscription()} 的相同集合
     */
    synchronized Set<String> metadataTopics() {
        // 如果 group 订阅为空，则返回订阅的 topic 列表
        if (groupSubscription.isEmpty())
            return subscription;
        // 如果 group 订阅包含所有订阅的 topic，则返回 group 订阅
        else if (groupSubscription.containsAll(subscription))
            return groupSubscription;
        else {
            // 当订阅改变时，`groupSubscription` 可能过时，确保返回新的订阅 topic。
            // 创建一个新的集合，包含 group 订阅和订阅的 topic
            Set<String> topics = new HashSet<>(groupSubscription);
            // 将订阅的 topic 添加到集合中
            topics.addAll(subscription);
            // 返回包含所有订阅 topic 的集合
            return topics;
        }
    }

    /**
     * 检查是否需要 metadata。
     *
     * @param topic 要检查的 topic
     * @return 如果订阅的 topic 或 group 订阅的 topic 包含给定的 topic，则返回 true，否则返回 false
     */
    synchronized boolean needsMetadata(String topic) {
        return subscription.contains(topic) || groupSubscription.contains(topic);
    }

    /**
     * 获取给定分区的分配状态。
     *
     * @param tp 要获取分配状态的分区
     * @return 给定分区的分配状态
     * @throws IllegalStateException 如果没有当前分配
     */
    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    private TopicPartitionState assignedStateOrNull(TopicPartition tp) {
        return this.assignment.stateValue(tp);
    }

    /**
     * 将给定的分区移动到指定的位置。
     *
     * @param tp 要移动的分区
     * @param position 要移动到的位置
     */
    public synchronized void seekValidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekValidated(position);
    }

    public void seek(TopicPartition tp, long offset) {
        seekValidated(tp, new FetchPosition(offset));
    }

    public void seekUnvalidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekUnvalidated(position);
    }

    synchronized void maybeSeekUnvalidated(TopicPartition tp, FetchPosition position, OffsetResetStrategy requestedResetStrategy) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping reset of partition {} since it is no longer assigned", tp);
        } else if (!state.awaitingReset()) {
            log.debug("Skipping reset of partition {} since reset is no longer needed", tp);
        } else if (requestedResetStrategy != state.resetStrategy) {
            log.debug("Skipping reset of partition {} since an alternative reset has been requested", tp);
        } else {
            log.info("Resetting offset for partition {} to position {}.", tp, position);
            state.seekUnvalidated(position);
        }
    }

    /**
     * @return a modifiable copy of the currently assigned partitions
     */
    public synchronized Set<TopicPartition> assignedPartitions() {
        return new HashSet<>(this.assignment.partitionSet());
    }

    /**
     * @return a modifiable copy of the currently assigned partitions as a list
     */
    public synchronized List<TopicPartition> assignedPartitionsList() {
        return new ArrayList<>(this.assignment.partitionSet());
    }

    /**
     * Provides the number of assigned partitions in a thread safe manner.
     * @return the number of assigned partitions.
     */
    synchronized int numAssignedPartitions() {
        return this.assignment.size();
    }

    // Visible for testing
    public synchronized List<TopicPartition> fetchablePartitions(Predicate<TopicPartition> isAvailable) {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream API
        List<TopicPartition> result = new ArrayList<>();
        assignment.forEach((topicPartition, topicPartitionState) -> {
            // Cheap check is first to avoid evaluating the predicate if possible
            if (topicPartitionState.isFetchable() && isAvailable.test(topicPartition)) {
                result.add(topicPartition);
            }
        });
        return result;
    }

    public synchronized boolean hasAutoAssignedPartitions() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public synchronized void position(TopicPartition tp, FetchPosition position) {
        assignedState(tp).position(position);
    }

    /**
     * Enter the offset validation state if the leader for this partition is known to support a usable version of the
     * OffsetsForLeaderEpoch API. If the leader node does not support the API, simply complete the offset validation.
     *
     * @param apiVersions supported API versions
     * @param tp topic partition to validate
     * @param leaderAndEpoch leader epoch of the topic partition
     * @return true if we enter the offset validation state
     */
    public synchronized boolean maybeValidatePositionForCurrentLeader(ApiVersions apiVersions,
                                                                      TopicPartition tp,
                                                                      Metadata.LeaderAndEpoch leaderAndEpoch) {
        if (leaderAndEpoch.leader.isPresent()) {
            NodeApiVersions nodeApiVersions = apiVersions.get(leaderAndEpoch.leader.get().idString());
            if (nodeApiVersions == null || hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
            } else {
                // If the broker does not support a newer version of OffsetsForLeaderEpoch, we skip validation
                assignedState(tp).updatePositionLeaderNoValidation(leaderAndEpoch);
                return false;
            }
        } else {
            return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
        }
    }


    /**
     * 尝试使用从 OffsetForLeaderEpoch 请求返回的 end offset 完成验证。
     * <p>
     * 该方法会检查给定的 TopicPartition 是否处于等待验证状态，并根据返回的 epochEndOffset 进行相应处理。
     * 如果分区不再等待验证，或者当前的 FetchPosition 与请求时的 FetchPosition 不匹配，则跳过验证。
     * 如果 epochEndOffset 的 endOffset 或 leaderEpoch 未定义，并且存在默认的 offset 重置策略，则重置偏移量。
     * 如果 epochEndOffset 的 endOffset 小于当前偏移量，并且存在默认的 offset 重置策略，则将偏移量重置为第一个已知的偏移量。
     * 否则，完成验证。
     *
     * @param tp 需要验证的 TopicPartition
     * @param requestPosition 请求时的 FetchPosition
     * @param epochEndOffset 从 OffsetForLeaderEpoch 请求返回的 epochEndOffset
     * @return 如果检测到日志截断且未定义重置策略，则返回日志截断的详细信息
     */
    public synchronized Optional<LogTruncation> maybeCompleteValidation(TopicPartition tp,
                                                                        FetchPosition requestPosition,
                                                                        EpochEndOffset epochEndOffset) {
        // 获取分区的状态
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            // 如果分区状态为空，记录调试日志并跳过验证
            log.debug("Skipping completed validation for partition {} which is not currently assigned.", tp);
        } else if (!state.awaitingValidation()) {
            // 如果分区不再等待验证，记录调试日志并跳过验证
            log.debug("Skipping completed validation for partition {} which is no longer expecting validation.", tp);
        } else {
            // 获取当前的 FetchPosition
            SubscriptionState.FetchPosition currentPosition = state.position;
            if (!currentPosition.equals(requestPosition)) {
                // 如果当前的 FetchPosition 与请求时的 FetchPosition 不匹配，记录调试日志并跳过验证
                log.debug("Skipping completed validation for partition {} since the current position {} " +
                          "no longer matches the position {} when the request was sent",
                          tp, currentPosition, requestPosition);
            } else if (epochEndOffset.endOffset() == UNDEFINED_EPOCH_OFFSET ||
                        epochEndOffset.leaderEpoch() == UNDEFINED_EPOCH) {
                // 如果 epochEndOffset 的 endOffset 或 leaderEpoch 未定义
                if (hasDefaultOffsetResetPolicy()) {
                    // 如果存在默认的 offset 重置策略，记录信息日志并重置偏移量
                    log.info("Truncation detected for partition {} at offset {}, resetting offset",
                             tp, currentPosition);
                    requestOffsetReset(tp);
                } else {
                    // 如果不存在默认的 offset 重置策略，记录警告日志并返回日志截断的详细信息
                    log.warn("Truncation detected for partition {} at offset {}, but no reset policy is set",
                             tp, currentPosition);
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.empty()));
                }
            } else if (epochEndOffset.endOffset() < currentPosition.offset) {
                // 如果 epochEndOffset 的 endOffset 小于当前偏移量
                if (hasDefaultOffsetResetPolicy()) {
                    // 如果存在默认的 offset 重置策略，记录信息日志并将偏移量重置为第一个已知的偏移量
                    SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                            epochEndOffset.endOffset(), Optional.of(epochEndOffset.leaderEpoch()),
                            currentPosition.currentLeader);
                    log.info("Truncation detected for partition {} at offset {}, resetting offset to " +
                             "the first offset known to diverge {}", tp, currentPosition, newPosition);
                    state.seekValidated(newPosition);
                } else {
                    // 如果不存在默认的 offset 重置策略，记录警告日志并返回日志截断的详细信息
                    OffsetAndMetadata divergentOffset = new OffsetAndMetadata(epochEndOffset.endOffset(),
                        Optional.of(epochEndOffset.leaderEpoch()), null);
                    log.warn("Truncation detected for partition {} at offset {} (the end offset from the " +
                             "broker is {}), but no reset policy is set", tp, currentPosition, divergentOffset);
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.of(divergentOffset)));
                }
            } else {
                // 完成验证
                state.completeValidation();
            }
        }

        // 返回空的 Optional 对象，表示没有检测到日志截断或已处理日志截断
        return Optional.empty();
    }

    public synchronized boolean awaitingValidation(TopicPartition tp) {
        return assignedState(tp).awaitingValidation();
    }

    public synchronized void completeValidation(TopicPartition tp) {
        assignedState(tp).completeValidation();
    }

    public synchronized FetchPosition validPosition(TopicPartition tp) {
        return assignedState(tp).validPosition();
    }

    public synchronized FetchPosition position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    /**
     * 获取给定分区的 lag。
     *
     * @param tp 需要获取 lag 的 TopicPartition
     * @param isolationLevel 隔离级别
     * @return 给定分区的 lag
     */
    public synchronized Long partitionLag(TopicPartition tp, IsolationLevel isolationLevel) {
        // 获取分区的状态
        TopicPartitionState topicPartitionState = assignedState(tp);
        // 如果当前的 FetchPosition 为空，返回 null
        if (topicPartitionState.position == null) {
            return null;
        } else if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            // 如果隔离级别为 READ_COMMITTED，返回 lastStableOffset 与 position 的差值
            return topicPartitionState.lastStableOffset == null ? null : topicPartitionState.lastStableOffset - topicPartitionState.position.offset;
        } else {
            // 如果隔离级别为 READ_UNCOMMITTED，返回 highWatermark 与 position 的差值
            return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position.offset;
        }
    }

    public synchronized Long partitionEndOffset(TopicPartition tp, IsolationLevel isolationLevel) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            return topicPartitionState.lastStableOffset;
        } else {
            return topicPartitionState.highWatermark;
        }
    }

    public synchronized void requestPartitionEndOffset(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        topicPartitionState.requestEndOffset();
    }

    public synchronized boolean partitionEndOffsetRequested(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.endOffsetRequested();
    }

    synchronized Long partitionLead(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.logStartOffset == null ? null : topicPartitionState.position.offset - topicPartitionState.logStartOffset;
    }

    synchronized void updateHighWatermark(TopicPartition tp, long highWatermark) {
        assignedState(tp).highWatermark(highWatermark);
    }

    synchronized void updateLogStartOffset(TopicPartition tp, long logStartOffset) {
        assignedState(tp).logStartOffset(logStartOffset);
    }

    synchronized void updateLastStableOffset(TopicPartition tp, long lastStableOffset) {
        assignedState(tp).lastStableOffset(lastStableOffset);
    }

    /**
     * Set the preferred read replica with a lease timeout. After this time, the replica will no longer be valid and
     * {@link #preferredReadReplica(TopicPartition, long)} will return an empty result.
     * <p>
     * 设置首选读副本并设置租约超时。在超时后，副本将不再有效，
     * {@link #preferredReadReplica(TopicPartition, long)} 将返回一个空结果。
     *
     * @param tp The topic partition
     * @param preferredReadReplicaId The preferred read replica
     * @param timeMs The time at which this preferred replica is no longer valid
     */
    public synchronized void updatePreferredReadReplica(TopicPartition tp, int preferredReadReplicaId, LongSupplier timeMs) {
        assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
    }

    /**
     * Get the preferred read replica
     * <p>
     * 获取首选读副本。如果副本不再有效，返回一个空结果。
     *
     * @param tp The topic partition
     * @param timeMs The current time
     * @return Returns the current preferred read replica, if it has been set and if it has not expired.
     */
    public synchronized Optional<Integer> preferredReadReplica(TopicPartition tp, long timeMs) {
        final TopicPartitionState topicPartitionState = assignedStateOrNull(tp);
        if (topicPartitionState == null) {
            return Optional.empty();
        } else {
            return topicPartitionState.preferredReadReplica(timeMs);
        }
    }

    /**
     * Unset the preferred read replica. This causes the fetcher to go back to the leader for fetches.
     * <p>
     * 取消设置首选读副本。这将导致 fetcher 返回到 leader 进行 fetch。
     *
     * @param tp The topic partition
     * @return true if the preferred read replica was set, false otherwise.
     */
    public synchronized Optional<Integer> clearPreferredReadReplica(TopicPartition tp) {
        return assignedState(tp).clearPreferredReadReplica();
    }

    /**
     * 获取所有已消费的偏移量。
     *
     * @return 所有已消费的偏移量
     */
    public synchronized Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        assignment.forEach((topicPartition, partitionState) -> {
            // 如果分区的状态有有效的偏移量，将偏移量添加到 allConsumed 中
            if (partitionState.hasValidPosition())
                allConsumed.put(topicPartition, new OffsetAndMetadata(partitionState.position.offset,
                        partitionState.position.offsetEpoch, ""));
        });
        // 返回所有已消费的偏移量
        return allConsumed;
    }

    /**
     * 请求重置给定分区的偏移量。
     *
     * @param partition 需要重置偏移量的 TopicPartition
     * @param offsetResetStrategy 偏移量重置策略
     */
    public synchronized void requestOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).reset(offsetResetStrategy);
    }

    /**
     * 请求重置给定分区的偏移量。
     *
     * @param partitions 需要重置偏移量的 TopicPartition 集合
     * @param offsetResetStrategy 偏移量重置策略
     */
    public synchronized void requestOffsetReset(Collection<TopicPartition> partitions, OffsetResetStrategy offsetResetStrategy) {
        partitions.forEach(tp -> {
            log.info("Seeking to {} offset of partition {}", offsetResetStrategy, tp);
            assignedState(tp).reset(offsetResetStrategy);
        });
    }

    public void requestOffsetReset(TopicPartition partition) {
        requestOffsetReset(partition, defaultResetStrategy);
    }

    synchronized void setNextAllowedRetry(Set<TopicPartition> partitions, long nextAllowResetTimeMs) {
        for (TopicPartition partition : partitions) {
            assignedState(partition).setNextAllowedRetry(nextAllowResetTimeMs);
        }
    }

    boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public synchronized boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public synchronized OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy();
    }

    public synchronized boolean hasAllFetchPositions() {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream API
        Iterator<TopicPartitionState> it = assignment.stateIterator();
        while (it.hasNext()) {
            if (!it.next().hasValidPosition()) {
                return false;
            }
        }
        return true;
    }

    public synchronized Set<TopicPartition> initializingPartitions() {
        return collectPartitions(state -> state.fetchState.equals(FetchStates.INITIALIZING));
    }

    private Set<TopicPartition> collectPartitions(Predicate<TopicPartitionState> filter) {
        Set<TopicPartition> result = new HashSet<>();
        assignment.forEach((topicPartition, topicPartitionState) -> {
            if (filter.test(topicPartitionState)) {
                result.add(topicPartition);
            }
        });
        return result;
    }


    public synchronized void resetInitializingPositions() {
        final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
        assignment.forEach((tp, partitionState) -> {
            if (partitionState.fetchState.equals(FetchStates.INITIALIZING)) {
                if (defaultResetStrategy == OffsetResetStrategy.NONE)
                    partitionsWithNoOffsets.add(tp);
                else
                    requestOffsetReset(tp);
            }
        });

        if (!partitionsWithNoOffsets.isEmpty())
            throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
    }

    public synchronized Set<TopicPartition> partitionsNeedingReset(long nowMs) {
        return collectPartitions(state -> state.awaitingReset() && !state.awaitingRetryBackoff(nowMs));
    }

    public synchronized Set<TopicPartition> partitionsNeedingValidation(long nowMs) {
        return collectPartitions(state -> state.awaitingValidation() && !state.awaitingRetryBackoff(nowMs));
    }

    public synchronized boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    public synchronized boolean isPaused(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isPaused();
    }

    synchronized boolean isFetchable(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isFetchable();
    }

    public synchronized boolean hasValidPosition(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.hasValidPosition();
    }

    public synchronized void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public synchronized void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    synchronized void requestFailed(Set<TopicPartition> partitions, long nextRetryTimeMs) {
        for (TopicPartition partition : partitions) {
            // by the time the request failed, the assignment may no longer
            // contain this partition any more, in which case we would just ignore.
            final TopicPartitionState state = assignedStateOrNull(partition);
            if (state != null)
                state.requestFailed(nextRetryTimeMs);
        }
    }

    synchronized void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public synchronized ConsumerRebalanceListener rebalanceListener() {
        return rebalanceListener;
    }

    private static class TopicPartitionState {

        private FetchState fetchState;
        private FetchPosition position; // last consumed position

        private Long highWatermark; // the high watermark from last fetch
        private Long logStartOffset; // the log start offset
        private Long lastStableOffset;
        private boolean paused;  // whether this partition has been paused by the user
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
        private Long nextRetryTimeMs;
        private Integer preferredReadReplica;
        private Long preferredReadReplicaExpireTimeMs;
        private boolean endOffsetRequested;
        
        TopicPartitionState() {
            this.paused = false;
            this.endOffsetRequested = false;
            this.fetchState = FetchStates.INITIALIZING;
            this.position = null;
            this.highWatermark = null;
            this.logStartOffset = null;
            this.lastStableOffset = null;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
            this.preferredReadReplica = null;
        }

        public boolean endOffsetRequested() {
            return endOffsetRequested;
        }

        public void requestEndOffset() {
            endOffsetRequested = true;
        }

        private void transitionState(FetchState newState, Runnable runIfTransitioned) {
            FetchState nextState = this.fetchState.transitionTo(newState);
            if (nextState.equals(newState)) {
                this.fetchState = nextState;
                runIfTransitioned.run();
                if (this.position == null && nextState.requiresPosition()) {
                    throw new IllegalStateException("Transitioned subscription state to " + nextState + ", but position is null");
                } else if (!nextState.requiresPosition()) {
                    this.position = null;
                }
            }
        }

        private Optional<Integer> preferredReadReplica(long timeMs) {
            if (preferredReadReplicaExpireTimeMs != null && timeMs > preferredReadReplicaExpireTimeMs) {
                preferredReadReplica = null;
                return Optional.empty();
            } else {
                return Optional.ofNullable(preferredReadReplica);
            }
        }

        private void updatePreferredReadReplica(int preferredReadReplica, LongSupplier timeMs) {
            if (this.preferredReadReplica == null || preferredReadReplica != this.preferredReadReplica) {
                this.preferredReadReplica = preferredReadReplica;
                this.preferredReadReplicaExpireTimeMs = timeMs.getAsLong();
            }
        }

        private Optional<Integer> clearPreferredReadReplica() {
            if (preferredReadReplica != null) {
                int removedReplicaId = this.preferredReadReplica;
                this.preferredReadReplica = null;
                this.preferredReadReplicaExpireTimeMs = null;
                return Optional.of(removedReplicaId);
            } else {
                return Optional.empty();
            }
        }

        private void reset(OffsetResetStrategy strategy) {
            transitionState(FetchStates.AWAIT_RESET, () -> {
                this.resetStrategy = strategy;
                this.nextRetryTimeMs = null;
            });
        }

        /**
         * Check if the position exists and needs to be validated. If so, enter the AWAIT_VALIDATION state. This method
         * also will update the position with the current leader and epoch.
         *
         * @param currentLeaderAndEpoch leader and epoch to compare the offset with
         * @return true if the position is now awaiting validation
         */
        private boolean maybeValidatePosition(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (this.fetchState.equals(FetchStates.AWAIT_RESET)) {
                return false;
            }

            if (!currentLeaderAndEpoch.leader.isPresent()) {
                return false;
            }

            if (position != null && !position.currentLeader.equals(currentLeaderAndEpoch)) {
                FetchPosition newPosition = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                validatePosition(newPosition);
                preferredReadReplica = null;
            }
            return this.fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        /**
         * For older versions of the API, we cannot perform offset validation so we simply transition directly to FETCHING
         */
        private void updatePositionLeaderNoValidation(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (position != null) {
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                    this.nextRetryTimeMs = null;
                });
            }
        }

        private void validatePosition(FetchPosition position) {
            if (position.offsetEpoch.isPresent() && position.currentLeader.epoch.isPresent()) {
                transitionState(FetchStates.AWAIT_VALIDATION, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            } else {
                // If we have no epoch information for the current position, then we can skip validation
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            }
        }

        /**
         * Clear the awaiting validation state and enter fetching.
         */
        private void completeValidation() {
            if (hasPosition()) {
                transitionState(FetchStates.FETCHING, () -> this.nextRetryTimeMs = null);
            }
        }

        private boolean awaitingValidation() {
            return fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        private boolean awaitingRetryBackoff(long nowMs) {
            return nextRetryTimeMs != null && nowMs < nextRetryTimeMs;
        }

        private boolean awaitingReset() {
            return fetchState.equals(FetchStates.AWAIT_RESET);
        }

        private void setNextAllowedRetry(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private void requestFailed(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private boolean hasValidPosition() {
            return fetchState.hasValidPosition();
        }

        private boolean hasPosition() {
            return position != null;
        }

        private boolean isPaused() {
            return paused;
        }

        private void seekValidated(FetchPosition position) {
            transitionState(FetchStates.FETCHING, () -> {
                this.position = position;
                this.resetStrategy = null;
                this.nextRetryTimeMs = null;
            });
        }

        private void seekUnvalidated(FetchPosition fetchPosition) {
            seekValidated(fetchPosition);
            validatePosition(fetchPosition);
        }

        private void position(FetchPosition position) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = position;
        }

        private FetchPosition validPosition() {
            if (hasValidPosition()) {
                return position;
            } else {
                return null;
            }
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

        private void highWatermark(Long highWatermark) {
            this.highWatermark = highWatermark;
            this.endOffsetRequested = false;
        }

        private void logStartOffset(Long logStartOffset) {
            this.logStartOffset = logStartOffset;
        }

        private void lastStableOffset(Long lastStableOffset) {
            this.lastStableOffset = lastStableOffset;
            this.endOffsetRequested = false;
        }

        private OffsetResetStrategy resetStrategy() {
            return resetStrategy;
        }
    }

    /**
     * The fetch state of a partition. This class is used to determine valid state transitions and expose the some of
     * the behavior of the current fetch state. Actual state variables are stored in the {@link TopicPartitionState}.
     */
    interface FetchState {
        default FetchState transitionTo(FetchState newState) {
            if (validTransitions().contains(newState)) {
                return newState;
            } else {
                return this;
            }
        }

        /**
         * Return the valid states which this state can transition to
         */
        Collection<FetchState> validTransitions();

        /**
         * Test if this state requires a position to be set
         */
        boolean requiresPosition();

        /**
         * Test if this state is considered to have a valid position which can be used for fetching
         */
        boolean hasValidPosition();
    }

    /**
     * An enumeration of all the possible fetch states. The state transitions are encoded in the values returned by
     * {@link FetchState#validTransitions}.
     */
    enum FetchStates implements FetchState {
        INITIALIZING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        FETCHING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return true;
            }
        },

        AWAIT_RESET() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        AWAIT_VALIDATION() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        }
    }

    /**
     * Represents the position of a partition subscription.
     *
     * This includes the offset and epoch from the last record in
     * the batch from a FetchResponse. It also includes the leader epoch at the time the batch was consumed.
     */
    public static class FetchPosition {
        public final long offset;
        final Optional<Integer> offsetEpoch;
        final Metadata.LeaderAndEpoch currentLeader;

        FetchPosition(long offset) {
            this(offset, Optional.empty(), Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        }

        public FetchPosition(long offset, Optional<Integer> offsetEpoch, Metadata.LeaderAndEpoch currentLeader) {
            this.offset = offset;
            this.offsetEpoch = Objects.requireNonNull(offsetEpoch);
            this.currentLeader = Objects.requireNonNull(currentLeader);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchPosition that = (FetchPosition) o;
            return offset == that.offset &&
                    offsetEpoch.equals(that.offsetEpoch) &&
                    currentLeader.equals(that.currentLeader);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, offsetEpoch, currentLeader);
        }

        @Override
        public String toString() {
            return "FetchPosition{" +
                    "offset=" + offset +
                    ", offsetEpoch=" + offsetEpoch +
                    ", currentLeader=" + currentLeader +
                    '}';
        }
    }

    public static class LogTruncation {
        public final TopicPartition topicPartition;
        public final FetchPosition fetchPosition;
        public final Optional<OffsetAndMetadata> divergentOffsetOpt;

        public LogTruncation(TopicPartition topicPartition,
                             FetchPosition fetchPosition,
                             Optional<OffsetAndMetadata> divergentOffsetOpt) {
            this.topicPartition = topicPartition;
            this.fetchPosition = fetchPosition;
            this.divergentOffsetOpt = divergentOffsetOpt;
        }

        @Override
        public String toString() {
            StringBuilder bldr = new StringBuilder()
                .append("(partition=")
                .append(topicPartition)
                .append(", fetchOffset=")
                .append(fetchPosition.offset)
                .append(", fetchEpoch=")
                .append(fetchPosition.offsetEpoch);

            if (divergentOffsetOpt.isPresent()) {
                OffsetAndMetadata divergentOffset = divergentOffsetOpt.get();
                bldr.append(", divergentOffset=")
                    .append(divergentOffset.offset())
                    .append(", divergentEpoch=")
                    .append(divergentOffset.leaderEpoch());
            } else {
                bldr.append(", divergentOffset=unknown")
                    .append(", divergentEpoch=unknown");
            }

            return bldr.append(")").toString();

        }
    }
}
