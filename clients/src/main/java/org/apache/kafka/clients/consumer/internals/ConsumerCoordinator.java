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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS;
import static org.apache.kafka.clients.consumer.CooperativeStickyAssignor.COOPERATIVE_STICKY_ASSIGNOR_NAME;

/**
 * This class manages the coordination process with the consumer coordinator.
 * <p>
 *     该类管理与消费者协调器的协调过程。
 */
public final class ConsumerCoordinator extends AbstractCoordinator {
    // 组再平衡配置
    private final GroupRebalanceConfig rebalanceConfig;
    // 日志记录器
    private final Logger log;
    // 分区分配器列表
    private final List<ConsumerPartitionAssignor> assignors;
    // 消费者元数据
    private final ConsumerMetadata metadata;
    // 消费者协调器指标
    private final ConsumerCoordinatorMetrics sensors;
    // 订阅状态
    private final SubscriptionState subscriptions;
    // 默认的偏移量提交回调
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    // 是否启用自动提交
    private final boolean autoCommitEnabled;
    // 自动提交间隔时间（毫秒）
    private final int autoCommitIntervalMs;
    // 消费者拦截器
    private final ConsumerInterceptors<?, ?> interceptors;
    // 未完成的异步提交计数
    private final AtomicInteger pendingAsyncCommits;

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread

    // 这个集合必须线程安全，因为它的修改可能来自心跳线程的 offset commit 请求响应处理程序
    // 这个集合用来存储 commitOffset 响应成功后的回调方法
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;

    // 是否为领导者
    private boolean isLeader = false;
    // 已加入的订阅（这个类似于一个快照，和 subscriptionStates 是分开的，能用于判断是否 subscribe 了新的 topic）
    private Set<String> joinedSubscription;
    // 元数据快照
    private MetadataSnapshot metadataSnapshot;
    // 分配快照
    private MetadataSnapshot assignmentSnapshot;
    // 下次自动提交的计时器
    private Timer nextAutoCommitTimer;
    // 异步提交的一个 fence 异常暂存标志位？
    // consumer 收到 commitOffset 的 fenceException 后，会标记这个标志位，然后在下一次尝试触发回调的时候抛异常
    private AtomicBoolean asyncCommitFenced;
    // 消费者组元数据
    private ConsumerGroupMetadata groupMetadata;
    // 是否在获取稳定偏移量不支持时抛出异常
    private final boolean throwOnFetchStableOffsetsUnsupported;

    // hold onto request&future for committed offset requests to enable async calls.
    // 持有已提交偏移量请求的请求和未来，以启用异步调用
    private PendingCommittedOffsetRequest pendingCommittedOffsetRequest = null;

    private static class PendingCommittedOffsetRequest {
        private final Set<TopicPartition> requestedPartitions;
        private final Generation requestedGeneration;
        private final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response;

        private PendingCommittedOffsetRequest(final Set<TopicPartition> requestedPartitions,
                                              final Generation generationAtRequestTime,
                                              final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response) {
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions);
            this.response = Objects.requireNonNull(response);
            this.requestedGeneration = generationAtRequestTime;
        }

        private boolean sameRequest(final Set<TopicPartition> currentRequest, final Generation currentGeneration) {
            return Objects.equals(requestedGeneration, currentGeneration) && requestedPartitions.equals(currentRequest);
        }
    }

    private final RebalanceProtocol protocol;

    /**
     * Initialize the coordination manager.
     * <p>
     *     初始化 coordination manager。
     */
    public ConsumerCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               List<ConsumerPartitionAssignor> assignors,
                               ConsumerMetadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean throwOnFetchStableOffsetsUnsupported) {
        // 先嗲用父类 AbstractCoordinator 的构造方法
        super(rebalanceConfig,
              logContext,
              client,
              metrics,
              metricGrpPrefix,
              time);
        // 初始化一些属性
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(ConsumerCoordinator.class);
        this.metadata = metadata;
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch(), metadata.updateVersion());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.pendingAsyncCommits = new AtomicInteger();
        this.asyncCommitFenced = new AtomicBoolean(false);
        this.groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID, JoinGroupRequest.UNKNOWN_MEMBER_ID, rebalanceConfig.groupInstanceId);
        this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported;

        if (autoCommitEnabled)
            this.nextAutoCommitTimer = time.timer(autoCommitIntervalMs);

        // select the rebalance protocol such that:
        //   1. only consider protocols that are supported by all the assignors. If there is no common protocols supported
        //      across all the assignors, throw an exception.
        //   2. if there are multiple protocols that are commonly supported, select the one with the highest id (i.e. the
        //      id number indicates how advanced the protocol is).
        // we know there are at least one assignor in the list, no need to double check for NPE
        // 选择再平衡协议，使得：
        // 1. 仅考虑所有分配器都支持的协议。如果没有跨所有分配器支持的公共协议，则抛出异常。
        // 2. 如果有多个常用支持的协议，则选择具有最高 id 的协议（即 id 号表示协议的高级程度）。
        // 我们知道列表中至少有一个分配器，不需要再次检查 NPE
        if (!assignors.isEmpty()) {
            // 取第一个 assignor 支持的 protocol 集合
            List<RebalanceProtocol> supportedProtocols = new ArrayList<>(assignors.get(0).supportedProtocols());

            // 使用这个 protocol 集合与其他所有 assignor 支持的 protocol 集合取交集
            for (ConsumerPartitionAssignor assignor : assignors) {
                supportedProtocols.retainAll(assignor.supportedProtocols());
            }

            // 如果最终没有交集，说明没有公共支持的 protocol，需要抛出异常
            if (supportedProtocols.isEmpty()) {
                throw new IllegalArgumentException("Specified assignors " +
                    assignors.stream().map(ConsumerPartitionAssignor::name).collect(Collectors.toSet()) +
                    " do not have commonly supported rebalance protocol");
            }

            // 针对 protocol 排序后，取第一个
            Collections.sort(supportedProtocols);
            protocol = supportedProtocols.get(supportedProtocols.size() - 1);
        } else {
            protocol = null;
        }

        this.metadata.requestUpdate();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
        // 针对订阅信息，封装成 JoinGroupRequestProtocolCollection，用于发送 JoinGroupRequest
        log.debug("Joining group with current subscription: {}", subscriptions.subscription());
        this.joinedSubscription = subscriptions.subscription();
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocolSet = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();

        List<String> topics = new ArrayList<>(joinedSubscription);
        for (ConsumerPartitionAssignor assignor : assignors) {
            Subscription subscription = new Subscription(topics,
                                                         assignor.subscriptionUserData(joinedSubscription),
                                                         subscriptions.assignedPartitionsList());
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);

            protocolSet.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(assignor.name())
                    .setMetadata(Utils.toArray(metadata)));
        }
        return protocolSet;
    }

    /**
     * 更新 pattern 订阅
     *
     * @param cluster
     */
    public void updatePatternSubscription(Cluster cluster) {
        // 遍历所有的 topic，过滤出匹配订阅 pattern 的 topic 集合
        final Set<String> topicsToSubscribe = cluster.topics().stream()
                .filter(subscriptions::matchesSubscribedPattern)
                .collect(Collectors.toSet());
        // 更新订阅
        if (subscriptions.subscribeFromPattern(topicsToSubscribe))
            metadata.requestUpdateForNewTopics();
    }

    private ConsumerPartitionAssignor lookupAssignor(String name) {
        // 根据 name 寻找对应的 ConsumerPartitionAssignor
        for (ConsumerPartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    private void maybeUpdateJoinedSubscription(Set<TopicPartition> assignedPartitions) {
        // 在 onJoinComplete 方法中调用，会更新 joinedSubscription
        // 如果使用了 PATTERN 方式的订阅
        if (subscriptions.hasPatternSubscription()) {
            // Check if the assignment contains some topics that were not in the original
            // subscription, if yes we will obey what leader has decided and add these topics
            // into the subscriptions as long as they still match the subscribed pattern
            // 检查分配是否包含一些原始订阅中没有的主题，如果是，我们将遵循 leader 的决定，并将这些主题添加到订阅中，只要它们仍然匹配订阅的模式

            Set<String> addedTopics = new HashSet<>();
            // this is a copy because its handed to listener below
            for (TopicPartition tp : assignedPartitions) {
                if (!joinedSubscription.contains(tp.topic()))
                    addedTopics.add(tp.topic());
            }

            if (!addedTopics.isEmpty()) {
                Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
                Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
                newSubscription.addAll(addedTopics);
                newJoinedSubscription.addAll(addedTopics);

                // 更新订阅 topic 的集合
                if (this.subscriptions.subscribeFromPattern(newSubscription))
                    metadata.requestUpdateForNewTopics();
                this.joinedSubscription = newJoinedSubscription;
            }
        }
    }

    private Exception invokeOnAssignment(final ConsumerPartitionAssignor assignor, final Assignment assignment) {
        // 在 onJoinComplete 方法中调用
        log.info("Notifying assignor about the new {}", assignment);

        try {
            // 触发 assignor 的回调函数
            assignor.onAssignment(assignment, groupMetadata);
        } catch (Exception e) {
            return e;
        }

        return null;
    }

    private Exception invokePartitionsAssigned(final Set<TopicPartition> assignedPartitions) {
        // 在 onJoinComplete 方法中调用
        log.info("Adding newly assigned partitions: {}", Utils.join(assignedPartitions, ", "));

        // 通过 subscriptionStates 获取到 ConsumerRebalanceListener
        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            // 触发 ConsumerRebalanceListener 的 onPartitionsAssigned 方法
            listener.onPartitionsAssigned(assignedPartitions);
            sensors.assignCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsAssigned for partitions {}",
                listener.getClass().getName(), assignedPartitions, e);
            return e;
        }

        return null;
    }

    private Exception invokePartitionsRevoked(final Set<TopicPartition> revokedPartitions) {
        // 在 onJoinPrepare、onJoinComplete、onLeavePrepare 方法中调用
        log.info("Revoke previously assigned partitions {}", Utils.join(revokedPartitions, ", "));

        // 通过 subscriptionStates 获取到 ConsumerRebalanceListener
        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            // 触发 ConsumerRebalanceListener 的 onPartitionsRevoked 方法
            listener.onPartitionsRevoked(revokedPartitions);
            sensors.revokeCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsRevoked for partitions {}",
                listener.getClass().getName(), revokedPartitions, e);
            return e;
        }

        return null;
    }

    private Exception invokePartitionsLost(final Set<TopicPartition> lostPartitions) {
        // 在 onJoinPrepare、onLeavePrepare 方法中调用
        log.info("Lost previously assigned partitions {}", Utils.join(lostPartitions, ", "));

        // 通过 subscriptionStates 获取到 ConsumerRebalanceListener
        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            // 触发 ConsumerRebalanceListener 的 onPartitionsLost 方法
            listener.onPartitionsLost(lostPartitions);
            sensors.loseCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsLost for partitions {}",
                listener.getClass().getName(), lostPartitions, e);
            return e;
        }

        return null;
    }

    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        log.debug("Executing onJoinComplete with generation {} and memberId {}", generation, memberId);

        // Only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        // 只有 leader 负责监视元数据更改（即分区更改）
        if (!isLeader)
            assignmentSnapshot = null;

        // 再次获取 assignor
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        // Give the assignor a chance to update internal state based on the received assignment
        // 让 assignor 有机会根据接收到的分配更新内部状态
        groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId, generation, memberId, rebalanceConfig.groupInstanceId);

        // 获取当前被分配的（上一次被分配的）partition
        Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        // should at least encode the short version
        if (assignmentBuffer.remaining() < 2)
            throw new IllegalStateException("There are insufficient bytes available to read assignment from the sync-group response (" +
                "actual byte size " + assignmentBuffer.remaining() + ") , this is not expected; " +
                "it is possible that the leader's assign function is buggy and did not return any assignment for this member, " +
                "or because static member is configured and the protocol is buggy hence did not get the assignment for this member");

        // 从 byteBuffer 解析分配得到的 partition
        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        Set<TopicPartition> assignedPartitions = new HashSet<>(assignment.partitions());

        // 检查分配是否满足订阅
        if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
            final String reason = String.format("received assignment %s does not match the current subscription %s; " +
                    "it is likely that the subscription has changed since we joined the group, will re-join with current subscription",
                    assignment.partitions(), subscriptions.prettyString());
            // 触发重新加入 consumerGroup
            requestRejoin(reason);

            return;
        }

        final AtomicReference<Exception> firstException = new AtomicReference<>(null);
        // 集合计算，得到新增的 partition
        Set<TopicPartition> addedPartitions = new HashSet<>(assignedPartitions);
        addedPartitions.removeAll(ownedPartitions);

        // 如果 protocol 是 COOPERATIVE 的
        if (protocol == RebalanceProtocol.COOPERATIVE) {
            // 计算需要撤销的 partition
            Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions);
            revokedPartitions.removeAll(assignedPartitions);

            log.info("Updating assignment with\n" +
                    "\tAssigned partitions:                       {}\n" +
                    "\tCurrent owned partitions:                  {}\n" +
                    "\tAdded partitions (assigned - owned):       {}\n" +
                    "\tRevoked partitions (owned - assigned):     {}\n",
                assignedPartitions,
                ownedPartitions,
                addedPartitions,
                revokedPartitions
            );

            // 如果存在需要撤销的 partition
            if (!revokedPartitions.isEmpty()) {
                // Revoke partitions that were previously owned but no longer assigned;
                // note that we should only change the assignment (or update the assignor's state)
                // AFTER we've triggered  the revoke callback
                // 撤销以前拥有但不再分配的 partition；
                // 注意，我们应该在触发撤销回调之后才更改分配（或更新分配器的状态）
                firstException.compareAndSet(null, invokePartitionsRevoked(revokedPartitions));

                // If revoked any partitions, need to re-join the group afterwards
                // 如果撤销了任何分区，之后需要重新加入组
                final String reason = String.format("need to revoke partitions %s as indicated " +
                        "by the current assignment and re-join", revokedPartitions);
                // 触发重新加入 consumerGroup
                requestRejoin(reason);
            }
        }

        // The leader may have assigned partitions which match our subscription pattern, but which
        // were not explicitly requested, so we update the joined subscription here.
        // leader 可能已经分配了与我们的订阅模式匹配的 partition，但这些 partition 并没有被显式请求，所以我们在这里更新了加入的订阅
        maybeUpdateJoinedSubscription(assignedPartitions);

        // Catch any exception here to make sure we could complete the user callback.
        // 触发 assignor 的 onAssignment 方法，并且捕获异常
        firstException.compareAndSet(null, invokeOnAssignment(assignor, assignment));

        // Reschedule the auto commit starting from now
        if (autoCommitEnabled)
            this.nextAutoCommitTimer.updateAndReset(autoCommitIntervalMs);

        // 更新 subscriptionStates 中维护的分配的 partition 集合
        subscriptions.assignFromSubscribed(assignedPartitions);

        // Add partitions that were not previously owned but are now assigned
        firstException.compareAndSet(null, invokePartitionsAssigned(addedPartitions));

        if (firstException.get() != null) {
            if (firstException.get() instanceof KafkaException) {
                throw (KafkaException) firstException.get();
            } else {
                throw new KafkaException("User rebalance callback throws an error", firstException.get());
            }
        }
    }

    void maybeUpdateSubscriptionMetadata() {
        // 加锁获取 metadata 的更新版本
        int version = metadata.updateVersion();
        if (version > metadataSnapshot.version) {
            Cluster cluster = metadata.fetch();

            // 如果订阅了 pattern，则更新 pattern 订阅
            if (subscriptions.hasPatternSubscription())
                updatePatternSubscription(cluster);

            // Update the current snapshot, which will be used to check for subscription
            // changes that would require a rebalance (e.g. new partitions).
            // 更新当前的订阅元数据快照，它将被用于检查订阅变化，从而触发 rebalance（例如新分区）。
            metadataSnapshot = new MetadataSnapshot(subscriptions, cluster, version);
        }
    }

    /**
     * Poll for coordinator events. This ensures that the coordinator is known and that the consumer
     * has joined the group (if it is using group management). This also handles periodic offset commits
     * if they are enabled.
     * <p>
     * 轮询获取 coordinator 事件。
     * 这确保了 coordinator 是已知的，并且消费者已经加入了组（如果它使用组管理）。
     * 这也处理了定期偏移提交（如果启用）。
     * <p>
     * Returns early if the timeout expires or if waiting on rejoin is not required
     * <p>
     * 如果超时或不需要等待重新加入组，则返回早。
     * <p>
     *
     * @param timer Timer bounding how long this method can block
     * @param waitForJoinGroup Boolean flag indicating if we should wait until re-join group completes
     * @throws KafkaException if the rebalance callback throws an exception
     * @return true iff the operation succeeded
     */
    public boolean poll(Timer timer, boolean waitForJoinGroup) {
        // 更新订阅元数据（会触发 pattern 模式订阅的 topic 更新）
        maybeUpdateSubscriptionMetadata();
        // 针对已经完成的 commit 操作，触发回调
        invokeCompletedOffsetCommitCallbacks();

        // 如果存在自动分配的分区（AUTO_TOPIC、AUTO_PATTERN）
        if (subscriptions.hasAutoAssignedPartitions()) {
            // 必须要有一个 protocol，否则无法 rebalance
            if (protocol == null) {
                throw new IllegalStateException("User configured " + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG +
                    " to empty while trying to subscribe for group protocol to auto assign partitions");
            }
            // Always update the heartbeat last poll time so that the heartbeat thread does not leave the
            // group proactively due to application inactivity even if (say) the coordinator cannot be found.
            // 始终更新心跳的最后轮询时间，以便即使（例如）找不到协调器，心跳线程也不会由于应用程序不活动而主动离开组。
            pollHeartbeat(timer.currentTimeMs());
            // 如果 coordinator 未知，并且无法获取到 coordinator，则返回 false
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
            }

            // 如果需要 rejoin 或处于 pending 状态（已经有一个 joinFuture 了）
            if (rejoinNeededOrPending()) {
                // due to a race condition between the initial metadata fetch and the initial rebalance,
                // we need to ensure that the metadata is fresh before joining initially. This ensures
                // that we have matched the pattern against the cluster's topics at least once before joining.
                // 由于「第一次 fetch metadata」和「第一次 rebalance」之间存在竞争，我们需要确保在 join group 前，metadata 是最新的。
                // 这确保我们在加入之前至少一次将模式与集群的主题匹配。
                if (subscriptions.hasPatternSubscription()) {
                    // For consumer group that uses pattern-based subscription, after a topic is created,
                    // any consumer that discovers the topic after metadata refresh can trigger rebalance
                    // across the entire consumer group. Multiple rebalances can be triggered after one topic
                    // creation if consumers refresh metadata at vastly different times. We can significantly
                    // reduce the number of rebalances caused by single topic creation by asking consumer to
                    // refresh metadata before re-joining the group as long as the refresh backoff time has
                    // passed.
                    // 对于使用基于模式订阅的消费者组，在创建主题后，
                    // 任何在元数据刷新后发现该主题的消费者都可以触发整个消费者组的重新平衡。
                    // 如果消费者在非常不同的时间刷新元数据，则在创建一个主题后可以触发多次重新平衡。
                    // 通过要求消费者在重新加入组之前刷新元数据，只要刷新退避时间已过，
                    // 我们可以显著减少由单个主题创建引起的重新平衡次数。
                    if (this.metadata.timeToAllowUpdate(timer.currentTimeMs()) == 0) {
                        this.metadata.requestUpdate();
                    }

                    // 确保元数据是最新的
                    if (!client.ensureFreshMetadata(timer)) {
                        return false;
                    }

                    // 触发基于 pattern 订阅的 topic 集合更新
                    maybeUpdateSubscriptionMetadata();
                }

                // if not wait for join group, we would just use a timer of 0
                // 如果不等待加入组，我们将只使用 0 的计时器
                // 如果 ensureActiveGroup 失败，则返回 false
                if (!ensureActiveGroup(waitForJoinGroup ? timer : time.timer(0L))) {
                    // since we may use a different timer in the callee, we'd still need
                    // to update the original timer's current time after the call
                    // 由于我们可能在被调用者中使用不同的计时器，因此在调用后仍需要更新原始计时器的当前时间
                    timer.update(time.milliseconds());

                    return false;
                }
            }

            // 否则，说明不需要 rejoin 或不处于 pending 的状态
        } else {
            // For manually assigned partitions, if there are no ready nodes, await metadata.
            // If connections to all nodes fail, wakeups triggered while attempting to send fetch
            // requests result in polls returning immediately, causing a tight loop of polls. Without
            // the wakeup, poll() with no channels would block for the timeout, delaying re-connection.
            // awaitMetadataUpdate() initiates new connections with configured backoff and avoids the busy loop.
            // When group management is used, metadata wait is already performed for this scenario as
            // coordinator is unknown, hence this check is not required.
            // 对于手动 assign 的 partition，如果没有准备好的 node，则等待元数据。
            // 如果所有 node 的 connection 都失败，在尝试发送 fetch 请求时触发的 wakeup 会导致轮询立即返回，从而导致轮询的紧密循环。
            // 如果没有 wakeup，poll() 在没有 channel 的情况下会阻塞超时，延迟重新连接。
            // awaitMetadataUpdate() 以配置的退避时间启动新连接，避免繁忙循环。
            // 使用组管理时，元数据等待已在这种情况下执行，因为协调器未知，因此不需要此检查。
            if (metadata.updateRequested() && 
                !client.hasReadyNodes(timer.currentTimeMs())) {
                // 等待元数据更新
                client.awaitMetadataUpdate(timer);
            }
        }

        // 异步自动提交偏移量
        maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
        return true;
    }

    /**
     * Return the time to the next needed invocation of {@link ConsumerNetworkClient#poll(Timer)}.
     * <p>
     *     返回下一次需要调用 {@link ConsumerNetworkClient#poll(Timer)} 的时间。
     *
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        // 如果没有开启 autoCommit，则返回 timeToNextHeartbeat
        if (!autoCommitEnabled)
            return timeToNextHeartbeat(now);

        // 否则，返回 nextAutoCommitTimer 的剩余时间和 timeToNextHeartbeat 的最小值
        return Math.min(nextAutoCommitTimer.remainingMs(), timeToNextHeartbeat(now));
    }

    private void updateGroupSubscription(Set<String> topics) {
        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        // leader 将开始监视组感兴趣的任何主题的更改，这确保所有元数据更改最终都会被看到
        if (this.subscriptions.groupSubscribe(topics))
            metadata.requestUpdateForNewTopics();

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        // 更新元数据（如果需要）并跟踪用于分配的元数据，以便在重新平衡完成后检查是否有任何更改
        if (!client.ensureFreshMetadata(time.timer(Long.MAX_VALUE)))
            throw new TimeoutException();

        maybeUpdateSubscriptionMetadata();
    }

    // 判断是否是通过官方提供的 assignor 分配的
    private boolean isAssignFromSubscribedTopicsAssignor(String name) {
        return ASSIGN_FROM_SUBSCRIBED_ASSIGNORS.contains(name);
    }

    /**
     * user-customized assignor may have created some topics that are not in the subscription list
     * and assign their partitions to the members; in this case we would like to update the leader's
     * own metadata with the newly added topics so that it will not trigger a subsequent rebalance
     * when these topics gets updated from metadata refresh.
     * <p>
     *     使用自定义的 assignor 可能会创建一些不在订阅列表中的主题，并将它们的分区分配给成员；
     *     在这种情况下，我们希望更新 leader 自己的元数据，以便在这些主题从元数据刷新中更新时不会触发后续的重新平衡。
     *
     * We skip the check for in-product assignors since this will not happen in in-product assignors.
     * <p>
     *     我们跳过了对产品内 assignor 的检查，因为这不会发生在产品内 assignor 中。
     *
     * TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
     *       we may need to modify the ConsumerPartitionAssignor API to better support this case.
     *
     * @param assignorName          the selected assignor name
     * @param assignments           the assignments after assignor assigned
     * @param allSubscribedTopics   all consumers' subscribed topics
     */
    private void maybeUpdateGroupSubscription(String assignorName,
                                              Map<String, Assignment> assignments,
                                              Set<String> allSubscribedTopics) {
        // 如果不是通过官方提供的 assignor 分配的
        if (!isAssignFromSubscribedTopicsAssignor(assignorName)) {
            // 统计所有已分配的 topic
            Set<String> assignedTopics = new HashSet<>();
            for (Assignment assigned : assignments.values()) {
                for (TopicPartition tp : assigned.partitions())
                    assignedTopics.add(tp.topic());
            }

            // 如果已分配的 topic 不包含所有订阅的 topic，则打印警告
            if (!assignedTopics.containsAll(allSubscribedTopics)) {
                Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
                notAssignedTopics.removeAll(assignedTopics);
                log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
            }

            // 如果已分配的 topic 不包含所有订阅的 topic，则更新 groupSubscription
            if (!allSubscribedTopics.containsAll(assignedTopics)) {
                Set<String> newlyAddedTopics = new HashSet<>(assignedTopics);
                newlyAddedTopics.removeAll(allSubscribedTopics);
                log.info("The following not-subscribed topics are assigned, and their metadata will be " +
                    "fetched from the brokers: {}", newlyAddedTopics);

                allSubscribedTopics.addAll(newlyAddedTopics);
                updateGroupSubscription(allSubscribedTopics);
            }
        }
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        List<JoinGroupResponseData.JoinGroupResponseMember> allSubscriptions) {
        // 只在 prepare_join_group 后，response 告知当前 client 它是 leader 后调用

        // 根据 response 给的 protocolName 找到对应的 ConsumerPartitionAssignor
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        String assignorName = assignor.name();

        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();

        // collect all the owned partitions
        // 收集所有拥有的分区
        Map<String, List<TopicPartition>> ownedPartitions = new HashMap<>();

        // 遍历来自 response 的所有订阅信息，反序列化并采集信息
        for (JoinGroupResponseData.JoinGroupResponseMember memberSubscription : allSubscriptions) {
            Subscription subscription = ConsumerProtocol.deserializeSubscription(ByteBuffer.wrap(memberSubscription.metadata()));
            subscription.setGroupInstanceId(Optional.ofNullable(memberSubscription.groupInstanceId()));
            subscriptions.put(memberSubscription.memberId(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
            ownedPartitions.put(memberSubscription.memberId(), subscription.ownedPartitions());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        // leader 将开始监视组感兴趣的任何主题的更改，这确保所有元数据更改最终都会被看到
        // 更新当前 consumer 的 subscriptionStates 中的 groupSubscribe 集合
        updateGroupSubscription(allSubscribedTopics);

        isLeader = true;

        log.debug("Performing assignment using strategy {} with subscriptions {}", assignorName, subscriptions);

        // 执行 partition assign 的计算
        Map<String, Assignment> assignments = assignor.assign(metadata.fetch(), new GroupSubscription(subscriptions)).groupAssignment();

        // skip the validation for built-in cooperative sticky assignor since we've considered
        // the "generation" of ownedPartition inside the assignor
        // 跳过内置 cooperative sticky assignor 的验证，因为我们已经在 assignor 内部考虑了 ownedPartition 的 "generation"
        if (protocol == RebalanceProtocol.COOPERATIVE &&
                !assignorName.equals(COOPERATIVE_STICKY_ASSIGNOR_NAME)) {
            validateCooperativeAssignment(ownedPartitions, assignments);
        }

        // 可能更新组订阅
        maybeUpdateGroupSubscription(assignorName, assignments, allSubscribedTopics);

        // 更新快照
        assignmentSnapshot = metadataSnapshot;

        log.info("Finished assignment for group at generation {}: {}", generation().generationId, assignments);

        // 序列化分配结果
        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignments.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    /**
     * Used by COOPERATIVE rebalance protocol only.
     * <p>
     *     仅由 COOPERATIVE rebalance protocol 使用。
     *
     * Validate the assignments returned by the assignor such that no owned partitions are going to
     * be reassigned to a different consumer directly: if the assignor wants to reassign an owned partition,
     * it must first remove it from the new assignment of the current owner so that it is not assigned to any
     * member, and then in the next rebalance it can finally reassign those partitions not owned by anyone to consumers.
     * <p>
     *     验证 assignor 返回的分配，以便不会将拥有的 partition 直接重新分配给不同的 consumer：
     *     如果 assignor 想要重新分配一个拥有的 partition，它必须首先从当前所有者的新分配中删除它，以便它不分配给任何成员，
     *     然后在下一次重新平衡中，它最终可以将那些没有任何人拥有的 partition 重新分配给消费者。
     */
    private void validateCooperativeAssignment(final Map<String, List<TopicPartition>> ownedPartitions,
                                               final Map<String, Assignment> assignments) {
        // 统计需要撤销的 partition 和新增的 partition
        Set<TopicPartition> totalRevokedPartitions = new HashSet<>();
        Set<TopicPartition> totalAddedPartitions = new HashSet<>();
        // 遍历每个分配结果
        for (final Map.Entry<String, Assignment> entry : assignments.entrySet()) {
            final Assignment assignment = entry.getValue();
            // 计算对于该 member 来说，新增的 partition 和撤销的 partition
            final Set<TopicPartition> addedPartitions = new HashSet<>(assignment.partitions());
            addedPartitions.removeAll(ownedPartitions.get(entry.getKey()));
            final Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions.get(entry.getKey()));
            revokedPartitions.removeAll(assignment.partitions());

            totalAddedPartitions.addAll(addedPartitions);
            totalRevokedPartitions.addAll(revokedPartitions);
        }

        // if there are overlap between revoked partitions and added partitions, it means some partitions
        // immediately gets re-assigned to another member while it is still claimed by some member
        // 如果撤销的 partition 和新增的 partition 有重叠，则意味着一些 partition 立即被重新分配给另一个成员，而它仍然被某些成员声明
        totalAddedPartitions.retainAll(totalRevokedPartitions);
        if (!totalAddedPartitions.isEmpty()) {
            log.error("With the COOPERATIVE protocol, owned partitions cannot be " +
                "reassigned to other members; however the assignor has reassigned partitions {} which are still owned " +
                "by some members", totalAddedPartitions);

            throw new IllegalStateException("Assignor supporting the COOPERATIVE protocol violates its requirements");
        }
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        log.debug("Executing onJoinPrepare with generation {} and memberId {}", generation, memberId);
        // commit offsets prior to rebalance if auto-commit enabled
        // 如果启用了自动提交，则在重新平衡之前提交偏移量
        maybeAutoCommitOffsetsSync(time.timer(rebalanceConfig.rebalanceTimeoutMs));

        // the generation / member-id can possibly be reset by the heartbeat thread
        // upon getting errors or heartbeat timeouts; in this case whatever is previously
        // owned partitions would be lost, we should trigger the callback and cleanup the assignment;
        // otherwise we can proceed normally and revoke the partitions depending on the protocol,
        // and in that case we should only change the assignment AFTER the revoke callback is triggered
        // so that users can still access the previously owned partitions to commit offsets etc.

        // generation / member-id 可能由心跳线程在收到错误或心跳超时后重置；
        // 在这种情况下，之前拥有的 partition 将丢失，我们应该触发 callback 回调并清理 assignment；
        // 否则我们可以正常进行，并根据 protocol 撤销 partition，在这种情况下，我们只能在撤销回调触发后更改 assignment
        // 这样用户仍然可以访问之前拥有的分区来提交偏移量等。
        Exception exception = null;
        final Set<TopicPartition> revokedPartitions;
        // 如果 generation 为 NO_GENERATION && memberId 为 Generation.NO_MEMBER_ID，则触发 partitionsLost 回调
        if (generation == Generation.NO_GENERATION.generationId &&
            memberId.equals(Generation.NO_GENERATION.memberId)) {
            // 取到所有之前分配的分区
            revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());
            // 如果存在之前分配的分区，说明肯定不是第一次加入；而是因为 generation 重置导致的
            if (!revokedPartitions.isEmpty()) {
                log.info("Giving away all assigned partitions as lost since generation has been reset," +
                    "indicating that consumer is no longer part of the group");
                // 触发 ConsumerRebalanceListener 的 onPartitionsLost 回调
                exception = invokePartitionsLost(revokedPartitions);
                // 清空已分配的所有分区，注意是清空 partition 而不是清空 topic（不是 unsubscribe）；因此不会 reset SubscriptionType
                subscriptions.assignFromSubscribed(Collections.emptySet());
            }

            // 否则，说明 generation 不为 NO_GENERATION 或者 memberId 不为 Generation.NO_MEMBER_ID
        } else {
            switch (protocol) {
                // 如果 protocol 是 EAGER，则撤销当前 consumer 分配得到的所有分区
                case EAGER:
                    // revoke all partitions
                    revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    // 触发所有已分配分区的 onPartitionsRevoked 回调
                    exception = invokePartitionsRevoked(revokedPartitions);

                    // 清空已分配的所有分区，注意是清空 partition 而不是清空 topic；因此不会 reset SubscriptionType
                    subscriptions.assignFromSubscribed(Collections.emptySet());
                    break;

                // 如果 protocol 是 COOPERATIVE，则只撤销那些不再订阅的分区
                case COOPERATIVE:
                    // only revoke those partitions that are not in the subscription any more.
                    // 获取当前被分配的所有 partition
                    Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    // 可能由于订阅信息的变更，这里会过滤掉那些不再被订阅的 topic 下的 partition
                    // 也就是说，这里实际上被 revoked 的 partition 可能非常少，甚至几乎不会 revoked
                    revokedPartitions = ownedPartitions.stream()
                        .filter(tp -> !subscriptions.subscription().contains(tp.topic()))
                        .collect(Collectors.toSet());

                    if (!revokedPartitions.isEmpty()) {
                        // 针对这些不再订阅的分区，触发 onPartitionsRevoked 回调
                        exception = invokePartitionsRevoked(revokedPartitions);

                        // 更新 ownedPartitions，移除那些不再订阅的分区
                        ownedPartitions.removeAll(revokedPartitions);
                        subscriptions.assignFromSubscribed(ownedPartitions);
                    }
                    break;
            }
        }

        // 重置 leader 信息，重置 groupSubscription
        isLeader = false;
        subscriptions.resetGroupSubscription();

        if (exception != null) {
            throw new KafkaException("User rebalance callback throws an error", exception);
        }
    }

    /**
     * 在 consumer 离开 consumer group 之前准备
     */
    @Override
    public void onLeavePrepare() {
        // Save the current Generation and use that to get the memberId, as the hb thread can change it at any time
        // 保存当前的 Generation 并使用它来获取 memberId，因为 hb 线程可以随时更改它
        final Generation currentGeneration = generation();
        final String memberId = currentGeneration.memberId;

        log.debug("Executing onLeavePrepare with generation {} and memberId {}", currentGeneration, memberId);

        // we should reset assignment and trigger the callback before leaving group
        // 在离开 group 之前，我们应该重置 assignment 并触发回调
        Set<TopicPartition> droppedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        // 如果 subscription 是 AUTO_TOPIC 或 AUTO_PATTERN 分配的，并且确实分配了 partition
        // 则需要执行清理和回调工作
        if (subscriptions.hasAutoAssignedPartitions() &&
                !droppedPartitions.isEmpty()) {
            final Exception e;
            // 如果 generation 为 NO_GENERATION 或者处于 rebalance 状态
            // 则触发 partitionsLost 回调
            if (generation() == Generation.NO_GENERATION || rebalanceInProgress()) {
                e = invokePartitionsLost(droppedPartitions);
            } else {
                // 否则，触发 partitionsRevoked 回调
                e = invokePartitionsRevoked(droppedPartitions);
            }

            // 重置 subscription
            subscriptions.assignFromSubscribed(Collections.emptySet());

            // 如果回调抛出异常，则抛出 KafkaException
            if (e != null) {
                throw new KafkaException("User rebalance callback throws an error", e);
            }
        }
    }

    /**
     * @throws KafkaException if the callback throws exception
     */
    @Override
    public boolean rejoinNeededOrPending() {
        // 如果没有使用自动分配的分区，则不需要重新加入
        if (!subscriptions.hasAutoAssignedPartitions())
            return false;

        // we need to rejoin if we performed the assignment and metadata has changed;
        // also for those owned-but-no-longer-existed partitions we should drop them as lost
        // 如果我们执行了 assignment 行为，或者 metadata 发生了变化，则需要重新加入 consumer group
        // 同时，对于那些不再存在的分区，我们应该将它们丢弃
        if (assignmentSnapshot != null &&
                !assignmentSnapshot.matches(metadataSnapshot)) {
            final String reason = String.format("cached metadata has changed from %s at the beginning of the rebalance to %s",
                assignmentSnapshot, metadataSnapshot);
            // 标记需要 rejoin
            requestRejoin(reason);
            return true;
        }

        // we need to join if our subscription has changed since the last join
        // 如果我们的订阅发生了变化，则需要重新加入 consumer group
        if (joinedSubscription != null &&
                !joinedSubscription.equals(subscriptions.subscription())) {
            final String reason = String.format("subscription has changed from %s at the beginning of the rebalance to %s",
                joinedSubscription, subscriptions.subscription());
            // 标记需要 rejoin
            requestRejoin(reason);
            return true;
        }

        // 使用父类的判断
        return super.rejoinNeededOrPending();
    }

    /**
     * Refresh the committed offsets for provided partitions.
     * <p>
     * 刷新提供的分区的已提交偏移量。
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    public boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
        // 由 KafkaConsumer 调用

        // 获取正在初始化的分区（此时这些分区只是完成了 assign，但是还不知道具体的消费细节信息）
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();
        // 获取这些分区的已提交偏移量
        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(initializingPartitions, timer);
        // 如果获取失败，返回 false
        if (offsets == null) {
            return false;
        }

        // 遍历这些分区，设置它们的偏移量
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final OffsetAndMetadata offsetAndMetadata = entry.getValue();
            // 如果偏移量不为空，设置它们的偏移量
            if (offsetAndMetadata != null) {
                // first update the epoch if necessary
                // 如果 epoch 不为空，更新 epoch
                entry.getValue().leaderEpoch().ifPresent(epoch -> this.metadata.updateLastSeenEpochIfNewer(entry.getKey(), epoch));

                // it's possible that the partition is no longer assigned when the response is received,
                // so we need to ignore seeking if that's the case
                // 可能这个分区已经分配给了其他 consumer，所以需要忽略这些消息
                if (this.subscriptions.isAssigned(tp)) {
                    final ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);
                    final SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                            offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(),
                            leaderAndEpoch);

                    // 更新 subscriptionStates 中的对应 partition 的 position
                    this.subscriptions.seekUnvalidated(tp, position);

                    log.info("Setting offset for partition {} to the committed offset {}", tp, position);
                } else {
                    log.info("Ignoring the returned {} since its partition {} is no longer assigned",
                        offsetAndMetadata, tp);
                }
            }
        }
        return true;
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     * <p>
     *     从 coordinator 获取一组分区的当前提交偏移量。
     *
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset or null if the operation timed out
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(final Set<TopicPartition> partitions,
                                                                        final Timer timer) {
        // 成员变量 pendingCommittedOffsetRequest 就只在这个方法里用

        // 如果入参中需要拉取 offset 的分区为空，则直接返回空 map
        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }

        // 如果当前的 memberState 为 STABLE（已经成功加入了 group）那么就获取到 generation
        final Generation generationForOffsetRequest = generationIfStable();
        // 如果此时存在一个异步提交的请求，并且 generation 或 partitions 不同，则直接清空这个请求
        if (pendingCommittedOffsetRequest != null &&
            !pendingCommittedOffsetRequest.sameRequest(partitions, generationForOffsetRequest)) {
            // if we were waiting for a different request, then just clear it.
            pendingCommittedOffsetRequest = null;
        }

        do {
            // 如果在是一定时间内无法获取到 coordinator，则返回 null
            if (!ensureCoordinatorReady(timer)) {
                return null;
            }

            // contact coordinator to fetch committed offsets
            // 发送一个 offsetFetchRequest 请求
            final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future;
            if (pendingCommittedOffsetRequest != null) {
                future = pendingCommittedOffsetRequest.response;
            } else {
                future = sendOffsetFetchRequest(partitions);
                pendingCommittedOffsetRequest = new PendingCommittedOffsetRequest(partitions, generationForOffsetRequest, future);
            }
            // 触发一次 io 操作
            client.poll(future, timer);

            // 如果请求结束了
            if (future.isDone()) {
                // 清空 pendingCommittedOffsetRequest
                pendingCommittedOffsetRequest = null;

                if (future.succeeded()) {
                    return future.value();
                } else if (!future.isRetriable()) {
                    throw future.exception();

                    // 通过 sleep 来实现 backoff
                } else {
                    timer.sleep(rebalanceConfig.retryBackoffMs);
                }
            } else {
                return null;
            }
        } while (timer.notExpired());
        return null;
    }

    /**
     * Return the consumer group metadata.
     * <p>
     *     返回当前 consumer group 的 metadata。
     *
     * @return the current consumer group metadata
     */
    public ConsumerGroupMetadata groupMetadata() {
        return groupMetadata;
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public void close(final Timer timer) {
        // we do not need to re-enable wakeups since we are closing already
        // 我们不需要重新启用 wakeups，因为我们已经在关闭了
        client.disableWakeups();
        try {
            // 尝试触发一次自动提交
            maybeAutoCommitOffsetsSync(timer);
            // 如果没有超时并且存在异步的提交请求，则持续循环
            while (pendingAsyncCommits.get() > 0 && timer.notExpired()) {
                // 确保 coordinator 连接正常
                ensureCoordinatorReady(timer);
                // 触发 io
                client.poll(timer);
                // 触发已完成的偏移提交回调
                invokeCompletedOffsetCommitCallbacks();
            }
        } finally {
            // 调用父类 AbstractCoordinator 的 close 方法
            super.close(timer);
        }
    }

    // visible for testing
    /**
     * 执行已完成的偏移提交回调
     * 这个方法会在 coordinator.poll、commitOffsetsAsync、commitOffsetsSync、close 方法中调用
     */
    void invokeCompletedOffsetCommitCallbacks() {
        // 如果已经获取到 fencing 异常，则抛出 FencedInstanceIdException 异常
        if (asyncCommitFenced.get()) {
            throw new FencedInstanceIdException("Get fenced exception for group.instance.id "
                + rebalanceConfig.groupInstanceId.orElse("unset_instance_id")
                + ", current member.id is " + memberId());
        }
        while (true) {
            // 持续执行所有 commitAsync 完成后的回调
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                break;
            }
            // 执行已完成的偏移提交回调
            completion.invoke();
        }
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        // 执行已完成的偏移提交回调
        invokeCompletedOffsetCommitCallbacks();

        // 如果 coordinator 已知，则发送偏移量提交请求
        if (!coordinatorUnknown()) {
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.

            // 我们不知道当前的 coordinator，所以尝试找到它并发送提交请求，或者失败
            // （我们不希望递归的重试，这可能会导致偏移量提交到达的顺序不正确）。
            // 注意，可能会有多个偏移量提交链接到同一个 coordinator 查找请求。
            // 这是可以的，因为 listeners 将按它们被添加的顺序被调用。
            // 还要注意，AbstractCoordinator 防止并发 coordinator 查找请求。
            pendingAsyncCommits.incrementAndGet();
            // 查找 coordinator，并添加监听器
            // 这里有没有可能并发的 lookupCoordinator，然后导致 offset 多次提交（覆盖）？
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    // 成功找到 coordinator 后，发送偏移量提交请求
                    doCommitOffsetsAsync(offsets, callback);
                    // 触发 io
                    client.pollNoWakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    // 如果查找 coordinator 失败，则添加一个偏移提交回调
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            new RetriableCommitFailedException(e)));
                }
            });
        }

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.

        // 确保提交有机会被传输（不阻塞其完成）。
        // 注意，提交被视为心跳，因此没有必要通过延迟任务执行来显式允许心跳。
        client.pollNoWakeup();
    }

    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        // 发送偏移量提交请求
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        // 如果没有回调，则使用默认的偏移量提交回调
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        // 添加偏移量提交请求的监听器
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                // 触发拦截器的执行
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                // 将回调方法添加到已完成的偏移提交回调中
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException) {
                    commitException = new RetriableCommitFailedException(e);
                }
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
                // 如果异常是 FencedInstanceIdException，则设置 asyncCommitFenced 为 true，那么在下一次尝试触发回调的时候，会抛出异常
                if (commitException instanceof FencedInstanceIdException) {
                    asyncCommitFenced.set(true);
                }
            }
        });
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * <p>
     * 提交偏移量同步。此方法将重试，直到提交成功或遇到不可恢复的错误。
     *
     * @param offsets The offsets to be committed
     *                要提交的偏移量
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions. See the exception for more details
     *             如果消费者未被授权访问组或任何指定的分区，则抛出 AuthorizationException 异常。有关更多详细信息，请参见异常
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     *            在提交完成之前发生不可恢复的错误时抛出 CommitFailedException 异常
     * @throws FencedInstanceIdException if a static member gets fenced
     *           如果静态成员被隔离，则抛出 FencedInstanceIdException 异常
     * @return If the offset commit was successfully sent and a successful response was received from
     *         the coordinator
     *         如果成功发送了偏移量提交，并且从协调器接收到了成功响应
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, Timer timer) {
        // 执行已完成的偏移提交回调
        invokeCompletedOffsetCommitCallbacks();

        // 如果偏移量为空，返回 true
        if (offsets.isEmpty())
            return true;

        // 持续尝试提交偏移量，直到成功或超时
        do {
            // 如果找不到 coordinator，则返回 false
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
            }

            // 发送偏移量提交请求
            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            // 触发 io
            client.poll(future, timer);

            // We may have had in-flight offset commits when the synchronous commit began. If so, ensure that
            // the corresponding callbacks are invoked prior to returning in order to preserve the order that
            // the offset commits were applied.

            // 当同步的 commit 开始时，可能有一些处于 in-flight 状态的偏移量提交请求。
            // 如果存在，则执行已完成的偏移提交回调，以确保这些偏移量提交请求的回调函数按顺序执行，以保持偏移量提交的顺序。
            invokeCompletedOffsetCommitCallbacks();

            // 如果请求成功，则执行偏移量提交拦截器
            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return true;
            }

            // 如果请求失败，并且不是可重试的，则抛出异常
            if (future.failed() && !future.isRetriable())
                throw future.exception();

            // 如果请求失败，并且是可重试的，则休眠一段时间后重试
            timer.sleep(rebalanceConfig.retryBackoffMs);
        } while (timer.notExpired());

        // 如果超时，则返回 false
        return false;
    }

    /**
     * 自动提交偏移量异步
     *
     * @param now 当前时间
     */
    public void maybeAutoCommitOffsetsAsync(long now) {
        // 如果启用了 autoCommit
        if (autoCommitEnabled) {
            nextAutoCommitTimer.update(now);
            // 如果到了下一次自动提交的时间
            if (nextAutoCommitTimer.isExpired()) {
                nextAutoCommitTimer.reset(autoCommitIntervalMs);
                // 执行异步的 autoCommitOffsets
                doAutoCommitOffsetsAsync();
            }
        }
    }

    private void doAutoCommitOffsetsAsync() {
        // 获取所有已消费的偏移量
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets);

        // 异步提交偏移量，在回调函数中更新 timer
        commitOffsetsAsync(allConsumedOffsets, (offsets, exception) -> {
            if (exception != null) {
                if (exception instanceof RetriableCommitFailedException) {
                    log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", offsets,
                        exception);
                    nextAutoCommitTimer.updateAndReset(rebalanceConfig.retryBackoffMs);
                } else {
                    log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, exception.getMessage());
                }
            } else {
                log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
            }
        });
    }

    private void maybeAutoCommitOffsetsSync(Timer timer) {
        // 如果开起了 autoCommit
        if (autoCommitEnabled) {
            // 通过 subscriptionState 取到所有已消费的偏移量（实际上就是上次拉取到的位移信息）
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
            try {
                log.debug("Sending synchronous auto-commit of offsets {}", allConsumedOffsets);
                // 触发一次同步提交
                if (!commitOffsetsSync(allConsumedOffsets, timer))
                    log.debug("Auto-commit of offsets {} timed out before completion", allConsumedOffsets);
            } catch (WakeupException | InterruptException e) {
                log.debug("Auto-commit of offsets {} was interrupted before completion", allConsumedOffsets);
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Synchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, e.getMessage());
            }
        }
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     * <p>
     * 为指定的主题和分区提交偏移量。
     * 这是一个非阻塞调用，返回一个请求 future，可以在同步提交的情况下进行轮询，或者在异步提交的情况下忽略。
     * <p>
     * NOTE: 此方法仅对测试可见
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 如果偏移量为空，返回一个空的请求 future
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        // 获取 coordinator
        Node coordinator = checkAndGetCoordinator();
        // 如果 coordinator 为空，返回一个 coordinator 不可用的请求 future
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // create the offset commit request
        // 创建偏移量提交请求
        Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
        // 遍历需要提交偏移量的每一个分区
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            // 验证偏移量数据正确
            if (offsetAndMetadata.offset() < 0) {
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }

            // 获取或创建主题数据
            OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                    .getOrDefault(topicPartition.topic(),
                            new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                    .setName(topicPartition.topic())
                    );
            // 添加分区数据
            topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setCommittedOffset(offsetAndMetadata.offset())
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch()
                            .orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setCommittedMetadata(offsetAndMetadata.metadata())
            );
            // 将主题数据添加到请求中
            requestTopicDataMap.put(topicPartition.topic(), topic);
        }

        // 获取 generation
        final Generation generation;
        // 如果订阅了自动分配的分区
        if (subscriptions.hasAutoAssignedPartitions()) {
            generation = generationIfStable();
            // if the generation is null, we are not part of an active group (and we expect to be).
            // the only thing we can do is fail the commit and let the user rejoin the group in poll().
            // 如果 generation 为空，则表示消费者不是活跃组的一部分（并且我们期望它是）。
            // 我们能做的唯一事情是使提交失败，并让用户在 poll() 中重新加入组。 
            if (generation == null) {
                log.info("Failing OffsetCommit request since the consumer is not part of an active group");

                // 如果正在重新平衡，则返回 RebalanceInProgressException 异常
                if (rebalanceInProgress()) {
                    // if the client knows it is already rebalancing, we can use RebalanceInProgressException instead of
                    // CommitFailedException to indicate this is not a fatal error
                    return RequestFuture.failure(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                        "consumer is undergoing a rebalance for auto partition assignment. You can try completing the rebalance " +
                        "by calling poll() and then retry the operation."));
                } else {
                    // 如果消费者不是活跃组的一部分，则返回 CommitFailedException 异常
                    return RequestFuture.failure(new CommitFailedException("Offset commit cannot be completed since the " +
                        "consumer is not part of an active group for auto partition assignment; it is likely that the consumer " +
                        "was kicked out of the group."));
                }
            }
        } else {
            generation = Generation.NO_GENERATION;
        }

        // 创建偏移量提交请求
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
                new OffsetCommitRequestData()
                        .setGroupId(this.rebalanceConfig.groupId)
                        .setGenerationId(generation.generationId)
                        .setMemberId(generation.memberId)
                        .setGroupInstanceId(rebalanceConfig.groupInstanceId.orElse(null))
                        .setTopics(new ArrayList<>(requestTopicDataMap.values()))
        );

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);

        // 发送偏移量提交请求，并注册偏移量提交响应处理器
        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets, generation));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets, Generation generation) {
            // 调用父类构造方法，传入 generation
            super(generation);
            // 初始化 offsets
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitSensor.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            // 遍历 commitResponse 中的每个 topic
            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                // 遍历 topic 中的每个分区
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                    // 获取偏移量
                    OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                    // 获取偏移量
                    long offset = offsetAndMetadata.offset();

                    Errors error = Errors.forCode(partition.errorCode());
                    // 如果偏移量提交成功
                    if (error == Errors.NONE) {
                        log.debug("Committed offset {} for partition {}", offset, tp);
                    } else {
                        // 如果偏移量提交失败，并且错误是可重试的
                        if (error.exception() instanceof RetriableException) {
                            log.warn("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        } else {
                            log.error("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        }

                        // 如果偏移量提交失败，并且错误是 GROUP_AUTHORIZATION_FAILED
                        if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                            future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                            return;
                        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                            unauthorizedTopics.add(tp.topic());
                        } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                                || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                            // raise the error to the user
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS
                                || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                            // just retry
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                                || error == Errors.NOT_COORDINATOR
                                || error == Errors.REQUEST_TIMED_OUT) {
                            markCoordinatorUnknown(error);
                            future.raise(error);
                            return;

                            // 如果偏移量提交失败，并且错误是 FENCED_INSTANCE_ID
                        } else if (error == Errors.FENCED_INSTANCE_ID) {
                            log.info("OffsetCommit failed with {} due to group instance id {} fenced", sentGeneration, rebalanceConfig.groupInstanceId);

                            // if the generation has changed or we are not in rebalancing, do not raise the fatal error but rebalance-in-progress
                            // 如果 generation 没有变化，或者我们不在重新平衡，则不抛出致命错误，而是抛出 RebalanceInProgressException 异常
                            if (generationUnchanged()) {
                                // 如果 generation 没有变化，则抛出 FencedInstanceIdException 异常
                                future.raise(error);
                            } else {
                                // 如果 generation 变化了，则抛出 CommitFailedException 异常
                                KafkaException exception;
                                synchronized (ConsumerCoordinator.this) {
                                    if (ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                        exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                            "consumer member's old generation is fenced by its group instance id, it is possible that " +
                                            "this consumer has already participated another rebalance and got a new generation");
                                    } else {
                                        exception = new CommitFailedException();
                                    }
                                }
                                future.raise(exception);
                            }
                            return;
                        } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                            /* Consumer should not try to commit offset in between join-group and sync-group,
                             * and hence on broker-side it is not expected to see a commit offset request
                             * during CompletingRebalance phase; if it ever happens then broker would return
                             * this error to indicate that we are still in the middle of a rebalance.
                             * In this case we would throw a RebalanceInProgressException,
                             * request re-join but do not reset generations. If the callers decide to retry they
                             * can go ahead and call poll to finish up the rebalance first, and then try commit again.
                             * <p>
                             * 在 join-group 和 sync-group 之间，消费者不应该尝试在偏移量提交之间提交偏移量，
                             * 因此在 CompletingRebalance 阶段，在 broker 端不应看到提交偏移量请求；
                             * 如果发生这种情况，broker 会返回此错误，表示我们仍在重新平衡中间。
                             * 在这种情况下，我们会抛出 RebalanceInProgressException，请求重新加入，但不重置 generation。
                             * 如果调用者决定重试，可以先调用 poll 完成重新平衡，然后再尝试提交。
                             */
                            requestRejoin("offset commit failed since group is already rebalancing");
                            future.raise(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                "consumer group is executing a rebalance at the moment. You can try completing the rebalance " +
                                "by calling poll() and then retry commit again"));
                            return;
                        } else if (error == Errors.UNKNOWN_MEMBER_ID
                                || error == Errors.ILLEGAL_GENERATION) {
                            log.info("OffsetCommit failed with {}: {}", sentGeneration, error.message());

                            // only need to reset generation and re-join group if generation has not changed or we are not in rebalancing;
                            // otherwise only raise rebalance-in-progress error
                            // 如果 generation 没有变化，或者我们不在重新平衡，则只抛出 RebalanceInProgressException 异常
                            // 否则只抛出 CommitFailedException 异常
                            KafkaException exception;
                            synchronized (ConsumerCoordinator.this) {
                                if (!generationUnchanged() && ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                    exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                        "consumer member's generation is already stale, meaning it has already participated another rebalance and " +
                                        "got a new generation. You can try completing the rebalance by calling poll() and then retry commit again");
                                } else {
                                    resetGenerationOnResponseError(ApiKeys.OFFSET_COMMIT, error);
                                    exception = new CommitFailedException();
                                }
                            }
                            future.raise(exception);
                            return;
                        } else {
                            future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                            return;
                        }
                    }
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {}", unauthorizedTopics);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     * <p>
     *     获取一组分区的提交偏移量。
     *     这是一个非阻塞调用。返回的 future 可以轮询以获取从 broker 返回的实际偏移量。
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        // 获取 coordinator
        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}", partitions);
        // construct the request
        // 构造拉取偏移量请求
        OffsetFetchRequest.Builder requestBuilder =
            new OffsetFetchRequest.Builder(this.rebalanceConfig.groupId, true, new ArrayList<>(partitions), throwOnFetchStableOffsetsUnsupported);

        // send the request with a callback
        // 发送请求（放到缓冲区），并添加回调
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        private OffsetFetchResponseHandler() {
            super(Generation.NO_GENERATION);
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            Errors responseError = response.groupLevelError(rebalanceConfig.groupId);
            if (responseError != Errors.NONE) {
                log.debug("Offset fetch failed: {}", responseError.message());

                if (responseError == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(responseError);
                } else if (responseError == Errors.NOT_COORDINATOR) {
                    // re-discover the coordinator and retry
                    markCoordinatorUnknown(responseError);
                    future.raise(responseError);
                } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
                }
                return;
            }

            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData =
                response.partitionDataMap(rebalanceConfig.groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(responseData.size());
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : responseData.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                if (partitionData.hasError()) {
                    Errors error = partitionData.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response for partition " +
                            tp + ": " + error.message()));
                        return;
                    }
                } else if (partitionData.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch);
                    // if there's no committed offset, record as null
                    offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
                } else {
                    log.info("Found no committed offset for partition {}", tp);
                    offsets.put(tp, null);
                }
            }

            if (unauthorizedTopics != null) {
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // just retry
                log.info("The following partitions still have unstable offsets " +
                             "which are not cleared on the broker side: {}" +
                             ", this could be either " +
                             "transactional offsets waiting for completion, or " +
                             "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                future.raise(new UnstableOffsetCommitException("There are unstable offsets for the requested topic partitions"));
            } else {
                future.complete(offsets);
            }
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitSensor;
        private final Sensor revokeCallbackSensor;
        private final Sensor assignCallbackSensor;
        private final Sensor loseCallbackSensor;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitSensor = metrics.sensor("commit-latency");
            this.commitSensor.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitSensor.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitSensor.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            this.revokeCallbackSensor = metrics.sensor("partition-revoked-latency");
            this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-avg",
                this.metricGrpName,
                "The average time taken for a partition-revoked rebalance listener callback"), new Avg());
            this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-max",
                this.metricGrpName,
                "The max time taken for a partition-revoked rebalance listener callback"), new Max());

            this.assignCallbackSensor = metrics.sensor("partition-assigned-latency");
            this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-avg",
                this.metricGrpName,
                "The average time taken for a partition-assigned rebalance listener callback"), new Avg());
            this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-max",
                this.metricGrpName,
                "The max time taken for a partition-assigned rebalance listener callback"), new Max());

            this.loseCallbackSensor = metrics.sensor("partition-lost-latency");
            this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-avg",
                this.metricGrpName,
                "The average time taken for a partition-lost rebalance listener callback"), new Avg());
            this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-max",
                this.metricGrpName,
                "The max time taken for a partition-lost rebalance listener callback"), new Max());

            Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final int version;
        // 记录每个 topic 的分区数量
        private final Map<String, Integer> partitionsPerTopic;

        private MetadataSnapshot(SubscriptionState subscription, Cluster cluster, int version) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            // 遍历订阅的 topic 列表
            for (String topic : subscription.metadataTopics()) {
                // 通过集群元数据获取 topic 的分区数
                Integer numPartitions = cluster.partitionCountForTopic(topic);
                if (numPartitions != null)
                    partitionsPerTopic.put(topic, numPartitions);
            }
            this.partitionsPerTopic = partitionsPerTopic;
            this.version = version;
        }

        // 比较两个 MetadataSnapshot 是否相等
        boolean matches(MetadataSnapshot other) {
            return version == other.version || 
                partitionsPerTopic.equals(other.partitionsPerTopic);
        }

        @Override
        public String toString() {
            return "(version" + version + ": " + partitionsPerTopic + ")";
        }
    }

    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null)
                callback.onComplete(offsets, exception);
        }
    }

    /* test-only classes below */
    RebalanceProtocol getProtocol() {
        return protocol;
    }

    boolean poll(Timer timer) {
        return poll(timer, true);
    }
}
