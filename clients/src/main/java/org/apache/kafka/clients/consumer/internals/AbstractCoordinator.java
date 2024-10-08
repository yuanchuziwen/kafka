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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.MemberIdRequiredException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 * <p>
 * AbstractCoordinator 实现了通过与指定的 Kafka broker（协调器）交互来管理单个组成员的组管理。
 * 组语义通过扩展此类提供。
 * 参见 {@link ConsumerCoordinator} 以获取示例用法。
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 * <p>
 * 从高层次来看，Kafka 的组管理协议由以下一系列操作组语义通过扩展此类提供。
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 *     <li>Group Registration：组成员向协调器注册，提供他们自己的元数据（例如他们感兴趣的主题集合）。</li>
 *     <li>Group/Leader Selection：协调器选择组的成员并选择一个成员作为领导者。</li>
 *     <li>State Assignment：领导者收集所有组成员的元数据并分配状态。</li>
 *     <li>Group Stabilization：每个成员接收领导者分配的状态并开始处理。</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, List)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 * <p>
 * 为了利用此协议，实现必须定义每个成员在 {@link #metadata()} 中提供的组注册元数据的格式，以及领导者在
 * {@link #performAssignment(String, String, List)} 中提供的状态分配格式，
 * 并在 {@link #onJoinComplete(int, String, String, ByteBuffer)} 中变得对成员可用。
 *
 * Note on locking: this class shares state between the caller and a background thread which is
 * used for sending heartbeats after the client has joined the group. All mutable state as well as
 * state transitions are protected with the class's monitor. Generally this means acquiring the lock
 * before reading or writing the state of the group (e.g. generation, memberId) and holding the lock
 * when sending a request that affects the state of the group (e.g. JoinGroup, LeaveGroup).
 * <p>
 * 关于锁定：此类在调用者和后台线程之间共享状态，后台线程用于在客户端加入组后发送心跳。
 * 所有可变状态以及状态转换都受到类的监视器保护。
 * 通常，这意味着在读取或写入组的状态（例如 generation, memberId）之前获取锁，并在发送影响组状态的请求（例如 JoinGroup, LeaveGroup）时持有锁。
 */
public abstract class AbstractCoordinator implements Closeable {
    // 心跳线程的命名前缀
    public static final String HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread";
    // 加入组超时时间
    public static final int JOIN_GROUP_TIMEOUT_LAPSE = 5000;

    // 成员状态
    protected enum MemberState {
        // 客户端不是组的一部分
        UNJOINED,             // the client is not part of a group
        // 客户端已发送加入组请求，但未收到响应
        PREPARING_REBALANCE,  // the client has sent the join group request, but have not received response
        // 客户端已收到加入组响应，但未收到分配
        COMPLETING_REBALANCE, // the client has received join group response, but have not received assignment
        // 客户端已加入并发送心跳
        STABLE;               // the client has joined and is sending heartbeats

        /**
         * 判断 client 是否未加入组
         * @return
         */
        public boolean hasNotJoinedGroup() {
            return equals(UNJOINED) || equals(PREPARING_REBALANCE);
        }
    }

    // 日志记录器
    private final Logger log;
    // 心跳管理器，它管理着心跳相关的标记位、时间等；
    private final Heartbeat heartbeat;
    // 组协调器指标
    private final GroupCoordinatorMetrics sensors;
    // 组再平衡配置
    private final GroupRebalanceConfig rebalanceConfig;

    // 时间管理器
    protected final Time time;
    // 消费者网络客户端
    protected final ConsumerNetworkClient client;

    // 协调器所在的节点
    private Node coordinator = null;
    // 是否需要重新加入组
    private boolean rejoinNeeded = true;
    // 是否需要准备加入组
    private boolean needsJoinPrepare = true;
    // 心跳线程
    private HeartbeatThread heartbeatThread = null;
    // 加入组请求的结果
    private RequestFuture<ByteBuffer> joinFuture = null;
    // 查找协调器请求的结果
    private RequestFuture<Void> findCoordinatorFuture = null;
    // 查找协调器时发生的致命异常
    private volatile RuntimeException fatalFindCoordinatorException = null;
    // 当前的 generation 信息
    private Generation generation = Generation.NO_GENERATION;
    // 上次再平衡开始的时间戳
    private long lastRebalanceStartMs = -1L;
    // 上次再平衡结束的时间戳
    private long lastRebalanceEndMs = -1L;
    // 上次连接的时间戳（仅在无法连接一段时间后开始记录警告）
    private long lastTimeOfConnectionMs = -1L; // starting logging a warning only after unable to connect for a while
    // 成员状态
    protected MemberState state = MemberState.UNJOINED;


    /**
     * Initialize the coordination manager.
     * <p>
     *     初始化协调管理器。
     */
    public AbstractCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time) {
        Objects.requireNonNull(rebalanceConfig.groupId,
                               "Expected a non-null group id for coordinator construction");
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(this.getClass());
        this.client = client;
        this.time = time;
        // 初始化心跳配置
        this.heartbeat = new Heartbeat(rebalanceConfig, time);
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
    }

    /**
     * Unique identifier for the class of supported protocols (e.g. "consumer" or "connect").
     * <p>
     *     支持的协议类的唯一标识符（例如 "consumer" 或 "connect"）。
     *
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * <p>
     *     获取当前支持的 protocol 及其关联的 metadata 列表。
     *     protocol 列表中的顺序表示选择的偏好（第一个 protocol 是最受欢迎的）。
     *     coordinator 在选择生成 protocol 时会考虑这种偏好（只要所有成员支持它们并且没有分歧）。
     *
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     * <p>
     *     在每次加入或重新加入组之前调用。
     *     这通常用于执行任何从上一个 generation 的清理（例如为消费者提交偏移量）。
     *
     * @param generation The previous generation or -1 if there was none
     *                   上一个 generation 或 -1（如果没有）
     * @param memberId The identifier of this member in the previous group or "" if there was none
     *                 上一个组中此成员的标识符，如果没有则为 ""
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * <p>
     *     为组执行分配。
     *     这是由 leader 推送到所有组成员的状态（例如在新的消费者情况下推送分区分配）。
     *
     * @param leaderId The id of the leader (which is this member)
     *                 领导者（即此成员）的 id
     * @param protocol The protocol selected by the coordinator
     *                 由 coordinator 选择的 protocol
     * @param allMemberMetadata Metadata from all members of the group
     *                 组中所有成员的元数据
     * @return A map from each member to their state assignment
     *         每个成员到其状态分配的映射
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group. If this call fails with an exception,
     * then it will be retried using the same assignment state on the next call to {@link #ensureActiveGroup()}.
     * <p>
     *     当一个组成员成功加入组时调用。
     *     如果此调用失败并抛出异常，则将在下一次调用 {@link #ensureActiveGroup()} 时使用相同的分配状态重试。
     *
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Invoked prior to each leave group event. This is typically used to cleanup assigned partitions;
     * note it is triggered by the consumer's API caller thread (i.e. background heartbeat thread would
     * not trigger it even if it tries to force leaving group upon heartbeat session expiration)
     * <p>
     *     在每次 leave group event 之前调用。
     *     这通常用于清理已分配的分区；
     *     注意，它会由消费者 API 调用者线程触发（也就是说，即使心跳线程尝试在心跳会话过期时强制离开组，也不会触发它）。
     */
    protected void onLeavePrepare() {}

    /**
     * Visible for testing.
     *
     * Ensure that the coordinator is ready to receive requests.
     * <p>
     *     确保 coordinator 已经准备好接收请求。
     *
     * @param timer Timer bounding how long this method can block
     *              用于限制此方法可以阻塞的时间
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     *        如果 coordinator 被发现并且连接成功建立，则返回 true，否则返回 false
     */
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        // 如果协调器已知，则返回 true
        if (!coordinatorUnknown())
            return true;

        do {
            // 如果寻找 coordinator 时发生了致命异常，则抛出异常
            if (fatalFindCoordinatorException != null) {
                final RuntimeException fatalException = fatalFindCoordinatorException;
                fatalFindCoordinatorException = null;
                throw fatalException;
            }

            // 准备发出 lookupCoordinator 请求，这里会构造好请求、回调，并把 Request 放到 selector 的缓冲区
            // 这里返回一个 future，方便阻塞以及注册监听器
            final RequestFuture<Void> future = lookupCoordinator();
            // 触发一次 io 操作，发出 lookupCoordinator 请求
            // 阻塞的逻辑会被放在 poll 里面实现
            client.poll(future, timer);

            // 如果 future 超时且未得到结果（没找到 coordinator，也没报错啥的），就跳出循环
            if (!future.isDone()) {
                break;
            }

            RuntimeException fatalException = null;
            // 如果 future 失败了
            if (future.failed()) {
                // 如果是可重试的错误，刷新元数据
                if (future.isRetriable()) {
                    // 如果是可重试的错误，刷新元数据
                    log.debug("Coordinator discovery failed, refreshing metadata", future.exception());
                    client.awaitMetadataUpdate(timer);
                } else {
                    // 如果是致命错误，记录日志并准备抛出异常
                    fatalException = future.exception();
                    log.info("FindCoordinator request hit fatal exception", fatalException);
                }

                // 否则，如果 coordinator 已知，但是连接失败，标记为未知并等待重试
            } else if (coordinator != null && client.isUnavailable(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                markCoordinatorUnknown("coordinator unavailable");
                timer.sleep(rebalanceConfig.retryBackoffMs);
            }

            // 清理成员变量 findCoordinatorFuture，设置为 null
            clearFindCoordinatorFuture();
            if (fatalException != null)
                throw fatalException;
        } while (coordinatorUnknown() && timer.notExpired());

        // 返回协调器是否已知
        return !coordinatorUnknown();
    }

    protected synchronized RequestFuture<Void> lookupCoordinator() {
        // 如果成员变量 findCoordinatorFuture 为空，则寻找 coordinator；
        // 否则直接返回 findCoordinatorFuture
        if (findCoordinatorFuture == null) {
            // find a node to ask about the coordinator
            // 寻找一个负载最低的节点来发请求询问 coordinator 在哪个节点
            Node node = this.client.leastLoadedNode();
            // 如果 leastLoadedNode 返回 null，则记录日志并返回一个带着异常的 future（内部会抛出异常）
            if (node == null) {
                log.debug("No broker available to send FindCoordinator request");
                return RequestFuture.noBrokersAvailable();
            } else {
                // 准备发送 FindCoordinator 请求，放到 send 缓冲区，并返回一个 future
                findCoordinatorFuture = sendFindCoordinatorRequest(node);
            }
        }
        return findCoordinatorFuture;
    }

    private synchronized void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes) or whether a
     * rejoin request is already in flight and needs to be completed.
     * <p>
     *     检查是否应该重新加入组（例如，如果元数据发生变化）或者是否已经有一个重新加入请求在飞行中并需要完成。
     *
     * @return true if it should, false otherwise
     */
    protected synchronized boolean rejoinNeededOrPending() {
        // if there's a pending joinFuture, we should try to complete handling it.
        return rejoinNeeded || joinFuture != null;
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness
     * of the client. This must be called periodically after joining with {@link #ensureActiveGroup()}
     * to ensure that the member stays in the group. If an interval of time longer than the
     * provided rebalance timeout expires without calling this method, then the client will proactively
     * leave the group.
     * <p>
     * 检查心跳线程的状态（如果它处于活动状态）并确认客户端是否存活。
     * 这必须在调用 {@link #ensureActiveGroup()} 之后定期调用，以确保成员保持在组中。
     * 如果超过 rebalance timeout 指定的时间间隔而没有调用此方法，则客户端将主动离开组。
     *
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    protected synchronized void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            // 如果心跳线程失败了，则抛出异常
            if (heartbeatThread.hasFailed()) {
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                // 将心跳线程设置为 null 并抛出异常。
                // 如果用户捕获了它，下一次调用 ensureActiveGroup() 将生成一个新的心跳线程。
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }
            // Awake the heartbeat thread if needed
            // 如果心跳线程需要保持心跳连接，则唤醒心跳线程
            if (heartbeat.shouldHeartbeat(now)) {
                notify();
            }
            heartbeat.poll(now);
        }
    }

    protected synchronized long timeToNextHeartbeat(long now) {
        // if we have not joined the group or we are preparing rebalance,
        // we don't need to send heartbeats
        // 如果我们还没有加入组或者正在准备再平衡，我们不需要发送心跳
        if (state.hasNotJoinedGroup())
            return Long.MAX_VALUE;
        return heartbeat.timeToNextHeartbeat(now);
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     * <p>
     *     确保组是活动的（即已加入并已同步）
     */
    public void ensureActiveGroup() {
        // 如果 ensureActiveGroup 返回 false，则一直等待直到返回 true
        while (!ensureActiveGroup(time.timer(Long.MAX_VALUE))) {
            log.warn("still waiting to ensure active group");
        }
    }

    /**
     * Ensure the group is active (i.e., joined and synced)
     * <p>
     *     确保组是活动的（即已加入并已同步）
     *
     * @param timer Timer bounding how long this method can block
     *              用于限制此方法可以阻塞的时间
     * @throws KafkaException if the callback throws exception
     * @return true iff the group is active
     */
    boolean ensureActiveGroup(final Timer timer) {
        // always ensure that the coordinator is ready because we may have been disconnected
        // when sending heartbeats and does not necessarily require us to rejoin the group.
        // 总是确保协调器已准备好，因为在发送心跳时可能已断开连接，不一定需要我们重新加入组。
        if (!ensureCoordinatorReady(timer)) {
            return false;
        }

        // 如果 heartbeatThread 为 null，则新建并开启心跳线程
        startHeartbeatThreadIfNeeded();
        return joinGroupIfNeeded(timer);
    }

    private synchronized void startHeartbeatThreadIfNeeded() {
        // 如果心跳线程为空，则开启心跳线程
        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        }
    }

    private void closeHeartbeatThread() {
        HeartbeatThread thread;
        synchronized (this) {
            if (heartbeatThread == null)
                return;
            heartbeatThread.close();
            thread = heartbeatThread;
            heartbeatThread = null;
        }
        try {
            // 将执行权交给 heartbeatThread，等待其关闭
            thread.join();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for consumer heartbeat thread to close");
            throw new InterruptException(e);
        }
    }

    /**
     * Joins the group without starting the heartbeat thread.
     * <p>
     * 加入 consumer group 但是不开启心跳线程
     *
     * If this function returns true, the state must always be in STABLE and heartbeat enabled.
     * If this function returns false, the state can be in one of the following:
     *  * UNJOINED: got error response but times out before being able to re-join, heartbeat disabled
     *  * PREPARING_REBALANCE: not yet received join-group response before timeout, heartbeat disabled
     *  * COMPLETING_REBALANCE: not yet received sync-group response before timeout, heartbeat enabled
     * 
     * 如果这个方法返回 true，那么状态必须是 STABLE 并且心跳是开启的。
     * 如果这个方法返回 false，那么状态可以是以下之一：
     *  * UNJOINED: 在超时之前收到错误响应，但是无法重新加入，心跳关闭
     *  * PREPARING_REBALANCE: 在超时之前没有收到 join-group 响应，心跳关闭
     *  * COMPLETING_REBALANCE: 在超时之前没有收到 sync-group 响应，心跳开启
     *
     * Visible for testing.
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the operation succeeded
     */
    boolean joinGroupIfNeeded(final Timer timer) {
        // 这个方法只会被 ensureActiveGroup 调用

        // 如果需要重新加入组，则一直循环
        while (rejoinNeededOrPending()) {
            // 确保 coordinator 已准备好
            if (!ensureCoordinatorReady(timer)) {
                return false;
            }

            // call onJoinPrepare if needed. We set a flag to make sure that we do not call it a second
            // time if the client is woken up before a pending rebalance completes. This must be called
            // on each iteration of the loop because an event requiring a rebalance (such as a metadata
            // refresh which changes the matched subscription set) can occur while another rebalance is
            // still in progress.
            // 如果需要调用 onJoinPrepare，则调用。
            // 我们设置一个标志，以确保在 pending rebalance 结束之前不会重复调用它。
            // onJoinPrepare 必须在每次循环中调用，因为可能在 rebalance 期间，再次产生需要重新平衡的事件（例如，改变匹配的订阅集的元数据刷新）
            if (needsJoinPrepare) {
                // need to set the flag before calling onJoinPrepare since the user callback may throw
                // exception, in which case upon retry we should not retry onJoinPrepare either.
                // 在调用 onJoinPrepare 之前需要设置标志，因为用户回调可能会抛出异常，
                // 在重试时，我们也不应该重试 onJoinPrepare。
                needsJoinPrepare = false;
                // 执行模板方法，在 join group 之前做一些准备
                // consumerCoordinator 会先提交偏移量，然后尝试触发 rebalanceListener 的一些方法
                onJoinPrepare(generation.generationId, generation.memberId);
            }

            // 发送 joinGroup 的请求，future 中有一个回调，即得到 response 之后，如果是 leader 则进行 partition assign；然后再发送 syncGroup 请求
            final RequestFuture<ByteBuffer> future = initiateJoinGroup();
            // 执行 io 操作
            client.poll(future, timer);
            if (!future.isDone()) {
                // we ran out of time
                return false;
            }

            // 如果 joinGroup 成功了
            if (future.succeeded()) {
                Generation generationSnapshot;
                MemberState stateSnapshot;

                // Generation data maybe concurrently cleared by Heartbeat thread.
                // Can't use synchronized for {@code onJoinComplete}, because it can be long enough
                // and shouldn't block heartbeat thread.
                // See {@link PlaintextConsumerTest#testMaxPollIntervalMsDelayInAssignment}
                // generation 信息可能会被心跳线程并发清除。
                // 不能使用 synchronized 来执行 {@code onJoinComplete}，因为它可能足够长，不应该阻塞心跳线程。
                // 参见 {@link PlaintextConsumerTest#testMaxPollIntervalMsDelayInAssignment}
                synchronized (AbstractCoordinator.this) {
                    generationSnapshot = this.generation;
                    stateSnapshot = this.state;
                }

                // 如果 generationSnapshot 不是 NO_GENERATION 并且 stateSnapshot 是 STABLE，则调用 onJoinComplete
                // 这明确说明了 joinGroup 的成功
                if (!generationSnapshot.equals(Generation.NO_GENERATION) && stateSnapshot == MemberState.STABLE) {
                    // Duplicate the buffer in case `onJoinComplete` does not complete and needs to be retried.
                    // 复制缓冲区，以防 `onJoinComplete` 没有完成并且需要重试。
                    ByteBuffer memberAssignment = future.value().duplicate();

                    // 执行模板方法
                    // ConsumerCoordinator 会触发 assigner 的回调方法，并且将分配结果更新到 subscriptionStates 中
                    onJoinComplete(generationSnapshot.generationId, generationSnapshot.memberId, generationSnapshot.protocolName, memberAssignment);

                    // Generally speaking we should always resetJoinGroupFuture once the future is done, but here
                    // we can only reset the join group future after the completion callback returns. This ensures
                    // that if the callback is woken up, we will retry it on the next joinGroupIfNeeded.
                    // And because of that we should explicitly trigger resetJoinGroupFuture in other conditions below.
                    // 通常情况下，一旦 future 完成，我们应该重置 joinGroupFuture，但是这里只能在完成回调返回后重置 join group future。
                    // 这确保了如果回调被唤醒，我们将在下一个 joinGroupIfNeeded 上重试它。
                    resetJoinGroupFuture();
                    // 设置在下一次 join 的时候需要 prepare
                    needsJoinPrepare = true;

                    // 否则，说明上面两个条件没用同时满足，那么需要重置状态并重新加入组
                } else {
                    final String reason = String.format("rebalance failed since the generation/state was " +
                            "modified by heartbeat thread to %s/%s before the rebalance callback triggered",
                            generationSnapshot, stateSnapshot);

                    resetStateAndRejoin(reason);
                    resetJoinGroupFuture();
                }

                // 否则，说明 future 没有成功，那么需要处理异常
            } else {
                final RuntimeException exception = future.exception();

                resetJoinGroupFuture();
                rejoinNeeded = true;

                if (exception instanceof UnknownMemberIdException ||
                    exception instanceof IllegalGenerationException ||
                    exception instanceof RebalanceInProgressException ||
                    exception instanceof MemberIdRequiredException)
                    continue;
                else if (!future.isRetriable())
                    throw exception;

                timer.sleep(rebalanceConfig.retryBackoffMs);
            }
        }
        return true;
    }

    private synchronized void resetJoinGroupFuture() {
        this.joinFuture = null;
    }

    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        // we store the join future in case we are woken up by the user after beginning the
        // rebalance in the call to poll below. This ensures that we do not mistakenly attempt
        // to rejoin before the pending rebalance has completed.
        // 我们存储 join future，以防在调用 poll 时，用户在开始 rebalance 后被唤醒。
        // 这确保了我们不会在 pending rebalance 完成之前错误地尝试重新加入。
        if (joinFuture == null) {
            // 设置当前状态为 PREPARING_REBALANCE
            state = MemberState.PREPARING_REBALANCE;
            // a rebalance can be triggered consecutively if the previous one failed,
            // in this case we would not update the start time.
            // 如果之前的 rebalance 失败，可以连续触发 rebalance，此时我们不会更新开始时间。
            if (lastRebalanceStartMs == -1L)
                lastRebalanceStartMs = time.milliseconds();
            // 发送 join group 请求，即放到 send 缓冲区
            joinFuture = sendJoinGroupRequest();
            joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // do nothing since all the handler logic are in SyncGroupResponseHandler already
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin;
                    // this can be triggered when either join or sync request failed
                    synchronized (AbstractCoordinator.this) {
                        sensors.failedRebalanceSensor.record();
                    }
                }
            });
        }
        return joinFuture;
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, List)} if
     * elected leader by the coordinator.
     * <p>
     * 加入 group 并返回下一个 generation 的 assignment。
     * 这个方法处理 JoinGroup 和 SyncGroup，如果被 coordinator 选为 leader，则调用 {@link #performAssignment(String, String, List)}。
     *
     * NOTE: This is visible only for testing
     *
     * @return A request future which wraps the assignment returned from the group leader
     */
    RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.info("(Re-)joining group");
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
                new JoinGroupRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setSessionTimeoutMs(this.rebalanceConfig.sessionTimeoutMs)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setProtocolType(protocolType())
                        .setProtocols(metadata())
                        .setRebalanceTimeoutMs(this.rebalanceConfig.rebalanceTimeoutMs)
        );

        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);

        // Note that we override the request timeout using the rebalance timeout since that is the
        // maximum time that it may block on the coordinator. We add an extra 5 seconds for small delays.
        int joinGroupTimeoutMs = Math.max(client.defaultRequestTimeoutMs(),
            rebalanceConfig.rebalanceTimeoutMs + JOIN_GROUP_TIMEOUT_LAPSE);
        return client.send(coordinator, requestBuilder, joinGroupTimeoutMs)
                .compose(new JoinGroupResponseHandler(generation));
    }

    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
        private JoinGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            Errors error = joinResponse.error();
            // 如果 joinResponse 成功
            if (error == Errors.NONE) {
                // 检查 protocolType 是否一致（consumer、connect）
                if (isProtocolTypeInconsistent(joinResponse.data().protocolType())) {
                    log.error("JoinGroup failed: Inconsistent Protocol Type, received {} but expected {}",
                        joinResponse.data().protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    log.debug("Received successful JoinGroup response: {}", joinResponse);
                    sensors.joinSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        // 验证此时状态必须为 PREPARING_REBALANCE
                        if (state != MemberState.PREPARING_REBALANCE) {
                            // if the consumer was woken up before a rebalance completes, we may have already left
                            // the group. In this case, we do not want to continue with the sync group.
                            // 如果在 rebalance 完成之前唤醒了消费者，我们可能已经离开了组。
                            future.raise(new UnjoinedGroupException());
                        } else {
                            // 将状态设置为 COMPLETING_REBALANCE
                            state = MemberState.COMPLETING_REBALANCE;

                            // we only need to enable heartbeat thread whenever we transit to
                            // COMPLETING_REBALANCE state since we always transit from this state to STABLE
                            // 我们只需要在从 COMPLETING_REBALANCE 状态转换到 STABLE 状态时启用心跳线程，
                            if (heartbeatThread != null)
                                heartbeatThread.enable();

                            // 基于 response 更新 generation
                            AbstractCoordinator.this.generation = new Generation(
                                joinResponse.data().generationId(),
                                joinResponse.data().memberId(), joinResponse.data().protocolName());

                            log.info("Successfully joined group with generation {}", AbstractCoordinator.this.generation);

                            // 如果 response 中告知当前 consumer 是 leader
                            if (joinResponse.isLeader()) {
                                onJoinLeader(joinResponse).chain(future);
                            } else {
                                onJoinFollower().chain(future);
                            }
                        }
                    }
                }
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                log.info("JoinGroup failed: Coordinator {} is loading the group.", coordinator());
                // backoff and retry
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                log.info("JoinGroup failed: {} Need to re-join the group. Sent generation was {}",
                         error.message(), sentGeneration);
                // only need to reset the member id if generation has not been changed,
                // then retry immediately
                if (generationUnchanged())
                    resetGenerationOnResponseError(ApiKeys.JOIN_GROUP, error);

                future.raise(error);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                // re-discover the coordinator and retry with backoff
                markCoordinatorUnknown(error);
                log.info("JoinGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                          error.message(), sentGeneration);
                future.raise(error);
            } else if (error == Errors.FENCED_INSTANCE_ID) {
                // for join-group request, even if the generation has changed we would not expect the instance id
                // gets fenced, and hence we always treat this as a fatal error
                log.error("JoinGroup failed: The group instance id {} has been fenced by another instance. " +
                              "Sent generation was {}", rebalanceConfig.groupInstanceId, sentGeneration);
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID
                    || error == Errors.GROUP_AUTHORIZATION_FAILED
                    || error == Errors.GROUP_MAX_SIZE_REACHED) {
                // log the error and re-throw the exception
                log.error("JoinGroup failed due to fatal error: {}", error.message());
                if (error == Errors.GROUP_MAX_SIZE_REACHED) {
                    future.raise(new GroupMaxSizeReachedException("Consumer group " + rebalanceConfig.groupId +
                            " already has the configured maximum number of members."));
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(error);
                }
            } else if (error == Errors.UNSUPPORTED_VERSION) {
                log.error("JoinGroup failed due to unsupported version error. Please unset field group.instance.id " +
                          "and retry to see if the problem resolves");
                future.raise(error);
            } else if (error == Errors.MEMBER_ID_REQUIRED) {
                // Broker requires a concrete member id to be allowed to join the group. Update member id
                // and send another join group request in next cycle.
                String memberId = joinResponse.data().memberId();
                log.debug("JoinGroup failed due to non-fatal error: {} Will set the member id as {} and then rejoin. " +
                              "Sent generation was  {}", error, memberId, sentGeneration);
                synchronized (AbstractCoordinator.this) {
                    AbstractCoordinator.this.generation = new Generation(OffsetCommitRequest.DEFAULT_GENERATION_ID, memberId, null);
                }
                requestRejoin("need to re-join with the given member-id");

                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                log.info("JoinGroup failed due to non-fatal error: REBALANCE_IN_PROGRESS, " +
                    "which could indicate a replication timeout on the broker. Will retry.");
                future.raise(error);
            } else {
                // unexpected error, throw the exception
                log.error("JoinGroup failed due to unexpected error: {}", error.message());
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        // 发送 follower 的 sync group 请求，分配结果为空
        SyncGroupRequest.Builder requestBuilder =
                new SyncGroupRequest.Builder(
                        new SyncGroupRequestData()
                                .setGroupId(rebalanceConfig.groupId)
                                .setMemberId(generation.memberId)
                                .setProtocolType(protocolType())
                                .setProtocolName(generation.protocolName)
                                .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                                .setGenerationId(generation.generationId)
                                .setAssignments(Collections.emptyList())
                );
        log.debug("Sending follower SyncGroup to coordinator {} at generation {}: {}", this.coordinator, this.generation, requestBuilder);
        return sendSyncGroupRequest(requestBuilder);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            // 执行 leader 同步并发送各个分区消费的分配结果
            // 下面是一个模板方法
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.data().leader(), joinResponse.data().protocolName(),
                    joinResponse.data().members());

            // 针对分配的结果，封装为一个 SyncGroupRequest
            List<SyncGroupRequestData.SyncGroupRequestAssignment> groupAssignmentList = new ArrayList<>();
            for (Map.Entry<String, ByteBuffer> assignment : groupAssignment.entrySet()) {
                groupAssignmentList.add(new SyncGroupRequestData.SyncGroupRequestAssignment()
                        .setMemberId(assignment.getKey())
                        .setAssignment(Utils.toArray(assignment.getValue()))
                );
            }

            SyncGroupRequest.Builder requestBuilder =
                    new SyncGroupRequest.Builder(
                            new SyncGroupRequestData()
                                    .setGroupId(rebalanceConfig.groupId)
                                    .setMemberId(generation.memberId)
                                    .setProtocolType(protocolType())
                                    .setProtocolName(generation.protocolName)
                                    .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                                    .setGenerationId(generation.generationId)
                                    .setAssignments(groupAssignmentList)
                    );
            log.debug("Sending leader SyncGroup to coordinator {} at generation {}: {}", this.coordinator, this.generation, requestBuilder);
            return sendSyncGroupRequest(requestBuilder);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        return client.send(coordinator, requestBuilder)
                .compose(new SyncGroupResponseHandler(generation));
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
        private SyncGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            Errors error = syncResponse.error();
            if (error == Errors.NONE) {
                if (isProtocolTypeInconsistent(syncResponse.data().protocolType())) {
                    log.error("SyncGroup failed due to inconsistent Protocol Type, received {} but expected {}",
                        syncResponse.data().protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    log.debug("Received successful SyncGroup response: {}", syncResponse);
                    sensors.syncSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        // 如果 generation 有值 && state 为 COMPLETING_REBALANCE
                        if (!generation.equals(Generation.NO_GENERATION) && state == MemberState.COMPLETING_REBALANCE) {
                            // check protocol name only if the generation is not reset
                            final String protocolName = syncResponse.data().protocolName();
                            final boolean protocolNameInconsistent = protocolName != null &&
                                !protocolName.equals(generation.protocolName);

                            if (protocolNameInconsistent) {
                                log.error("SyncGroup failed due to inconsistent Protocol Name, received {} but expected {}",
                                    protocolName, generation.protocolName);

                                future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                            } else {
                                log.info("Successfully synced group in generation {}", generation);
                                // 更新状态为 STABLE
                                state = MemberState.STABLE;
                                // 将 rejoinNeeded 设置为 false
                                rejoinNeeded = false;
                                // record rebalance latency
                                lastRebalanceEndMs = time.milliseconds();
                                sensors.successfulRebalanceSensor.record(lastRebalanceEndMs - lastRebalanceStartMs);
                                lastRebalanceStartMs = -1L;

                                future.complete(ByteBuffer.wrap(syncResponse.data().assignment()));
                            }
                        } else {
                            log.info("Generation data was cleared by heartbeat thread to {} and state is now {} before " +
                                "receiving SyncGroup response, marking this rebalance as failed and retry",
                                generation, state);
                            // use ILLEGAL_GENERATION error code to let it retry immediately
                            future.raise(Errors.ILLEGAL_GENERATION);
                        }
                    }
                }
            } else {
                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.info("SyncGroup failed: The group began another rebalance. Need to re-join the group. " +
                                 "Sent generation was {}", sentGeneration);
                    future.raise(error);
                } else if (error == Errors.FENCED_INSTANCE_ID) {
                    // for sync-group request, even if the generation has changed we would not expect the instance id
                    // gets fenced, and hence we always treat this as a fatal error
                    log.error("SyncGroup failed: The group instance id {} has been fenced by another instance. " +
                        "Sent generation was {}", rebalanceConfig.groupInstanceId, sentGeneration);
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    log.info("SyncGroup failed: {} Need to re-join the group. Sent generation was {}",
                            error.message(), sentGeneration);
                    if (generationUnchanged())
                        resetGenerationOnResponseError(ApiKeys.SYNC_GROUP, error);

                    future.raise(error);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR) {
                    log.info("SyncGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                             error.message(), sentGeneration);
                    markCoordinatorUnknown(error);
                    future.raise(error);
                } else {
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * <p>
     *     发现 group 的当前 coordinator。向其中一个 broker 发送 GroupMetadata 请求。
     *     返回的 future 应该被轮询以获取请求的结果。
     *
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
        // initiate the group metadata request
        log.debug("Sending FindCoordinator request to broker {}", node);
        // 构造 FindCoordinatorRequestData
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                // 这里 keyType 有两种类型：GROUP 和 TRANSACTION，也就是说 FindCoordinatorRequestData 可以用于查找 group coordinator 和 transaction coordinator
                .setKeyType(CoordinatorType.GROUP.id())
                .setKey(this.rebalanceConfig.groupId);
        // 构造 FindCoordinatorRequest.Builder
        FindCoordinatorRequest.Builder requestBuilder = new FindCoordinatorRequest.Builder(data);
        // 发送 FindCoordinator 请求，并且注册一个 FindCoordinatorResponseHandler 处理器作为回调
        // send 得到的 future 是 Future<FindCoordinatorResponse> 类型，而这里要返回一个 Future<Void> 类型，所以需要通过 compose 方法转换一下
        return client.send(node, requestBuilder)
                .compose(new FindCoordinatorResponseHandler());
    }

    /**
     * FindCoordinatorResponseHandler 处理器，用于处理 FindCoordinator 请求的响应
     */
    private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        /**
         * 处理 FindCoordinator 请求成功的响应
         * 
         * @param resp 响应
         * @param future 请求 future
         */
        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received FindCoordinator response {}", resp);

            // 从 response 中获取 coordinators 列表
            List<Coordinator> coordinators = ((FindCoordinatorResponse) resp.responseBody()).coordinators();
            // 验证 coordinators 列表的大小为 1
            if (coordinators.size() != 1) {
                log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
                future.raise(new IllegalStateException("Group coordinator lookup failed: Invalid response containing more than a single coordinator"));
            }
            // 获取 coordinator 数据
            Coordinator coordinatorData = coordinators.get(0);
            // 获取错误码
            Errors error = Errors.forCode(coordinatorData.errorCode());

            // 如果错误码为 NONE，则表示成功找到 coordinator
            if (error == Errors.NONE) {
                // 锁住当前 AbstractCoordinator 对象
                synchronized (AbstractCoordinator.this) {
                    // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                    // for the coordinator in the underlying network client layer
                    // 生成一个 coordinator 连接 id
                    // 使用 Integer.MAX_VALUE - node.id 作为 coordinator id，以允许在底层网络客户端层中为 coordinator 使用单独的连接
                    int coordinatorConnectionId = Integer.MAX_VALUE - coordinatorData.nodeId();

                    // 创建一个新的 Node 对象，表示 coordinator
                    // 这里会直接赋值给成员变量
                    AbstractCoordinator.this.coordinator = new Node(
                            coordinatorConnectionId,
                            coordinatorData.host(),
                            coordinatorData.port());
                    log.info("Discovered group coordinator {}", coordinator);

                    // 尝试连接 coordinator
                    client.tryConnect(coordinator);

                    // 重置 session 超时时间
                    heartbeat.resetSessionTimeout();
                }
                future.complete(null);

                // 如果错误码为 GROUP_AUTHORIZATION_FAILED，则抛出 GroupAuthorizationException 异常
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                // 如果错误码不为 NONE 或 GROUP_AUTHORIZATION_FAILED，则抛出异常
                log.debug("Group coordinator lookup failed: {}", coordinatorData.errorMessage());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            log.debug("FindCoordinator request failed due to {}", e.toString());

            if (!(e instanceof RetriableException)) {
                // Remember the exception if fatal so we can ensure it gets thrown by the main thread
                fatalFindCoordinatorException = e;
            }

            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * <p>
     *     检查我们是否知道 group coordinator 是谁，以及我们是否有一个活动的连接
     *
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        return checkAndGetCoordinator() == null;
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     * <p>
     *     如果 client 和 coordinator 之间连接仍然处于 active 状态，则获取 coordinator。否则标记为未知并返回 null。
     *
     * @return the current coordinator or null if it is unknown
     *          返回当前的 coordinator，如果未知则返回 null
     */
    protected synchronized Node checkAndGetCoordinator() {
        if (coordinator != null && // coordinator 不为 null
                client.isUnavailable(coordinator)) { // client 和 coordinator 之间的连接不可用
            // 标记 coordinator 未知，并返回 null
            markCoordinatorUnknown(true, "coordinator unavailable");
            return null;
        }
        return this.coordinator;
    }

    private synchronized Node coordinator() {
        return this.coordinator;
    }


    protected synchronized void markCoordinatorUnknown(Errors error) {
        markCoordinatorUnknown(false, "error response " + error.name());
    }

    protected synchronized void markCoordinatorUnknown(String cause) {
        markCoordinatorUnknown(false, cause);
    }

    protected synchronized void markCoordinatorUnknown(boolean isDisconnected, String cause) {
        // 如果 coordinator 不为 null
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}."
                    + "isDisconnected: {}. Rediscovery will be attempted.", this.coordinator,
                    cause, isDisconnected);
            Node oldCoordinator = this.coordinator;

            // Mark the coordinator dead before disconnecting requests since the callbacks for any pending
            // requests may attempt to do likewise. This also prevents new requests from being sent to the
            // coordinator while the disconnect is in progress.
            // 在断开连接请求之前标记 coordinator 死亡，因为任何挂起的请求的回调可能也会这样做。
            // 这也阻止了在断开连接期间向 coordinator 发送新请求。
            this.coordinator = null;

            // Disconnect from the coordinator to ensure that there are no in-flight requests remaining.
            // Pending callbacks will be invoked with a DisconnectException on the next call to poll.
            // 断开与 coordinator 的连接，以确保没有剩余的未完成请求。
            // 挂起的回调将在下次调用 poll 时使用 DisconnectException 调用。
            if (!isDisconnected)
                client.disconnectAsync(oldCoordinator);

            lastTimeOfConnectionMs = time.milliseconds();
        } else {
            long durationOfOngoingDisconnect = time.milliseconds() - lastTimeOfConnectionMs;
            if (durationOfOngoingDisconnect > rebalanceConfig.rebalanceTimeoutMs)
                log.warn("Consumer has been disconnected from the group coordinator for {}ms", durationOfOngoingDisconnect);
        }
    }

    /**
     * Get the current generation state, regardless of whether it is currently stable.
     * Note that the generation information can be updated while we are still in the middle
     * of a rebalance, after the join-group response is received.
     * <p>
     * 获取当前的 generation 状态，无论它是否稳定。
     * 注意，generation 信息可以在我们仍在进行 rebalance 的过程中更新，在收到 join-group 响应之后。
     *
     * @return the current generation
     */
    protected synchronized Generation generation() {
        return generation;
    }

    /**
     * Get the current generation state if the group is stable, otherwise return null
     * <p>
     *     如果 group 是稳定的，则返回当前的 generation 状态，否则返回 null
     *
     * @return the current generation or null
     */
    protected synchronized Generation generationIfStable() {
        if (this.state != MemberState.STABLE)
            return null;
        return generation;
    }

    protected synchronized boolean rebalanceInProgress() {
        return this.state == MemberState.PREPARING_REBALANCE || this.state == MemberState.COMPLETING_REBALANCE;
    }

    protected synchronized String memberId() {
        return generation.memberId;
    }

    private synchronized void resetStateAndGeneration(final String reason) {
        log.info("Resetting generation due to: {}", reason);

        state = MemberState.UNJOINED;
        generation = Generation.NO_GENERATION;
    }

    /**
     * 重置状态并重新加入
     * @param reason 原因
     */
    private synchronized void resetStateAndRejoin(final String reason) {
        resetStateAndGeneration(reason);
        requestRejoin(reason);
        needsJoinPrepare = true;
    }

    synchronized void resetGenerationOnResponseError(ApiKeys api, Errors error) {
        final String reason = String.format("encountered %s from %s response", error, api);
        resetStateAndRejoin(reason);
    }

    synchronized void resetGenerationOnLeaveGroup() {
        resetStateAndRejoin("consumer pro-actively leaving the group");
    }

    public synchronized void requestRejoin(final String reason) {
        log.info("Request joining group due to: {}", reason);
        this.rejoinNeeded = true;
    }

    private boolean isProtocolTypeInconsistent(String protocolType) {
        return protocolType != null && !protocolType.equals(protocolType());
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     * <p>
     *     关闭 coordinator，并等待发送 LeaveGroup 请求。
     */
    @Override
    public final void close() {
        close(time.timer(0));
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    protected void close(Timer timer) {
        try {
            // 关闭心跳线程
            closeHeartbeatThread();
        } finally {
            // Synchronize after closing the heartbeat thread since heartbeat thread
            // needs this lock to complete and terminate after close flag is set.
            // 在关闭心跳线程之后同步，因为心跳线程需要此锁来在设置 close 标志后完成并终止。
            synchronized (this) {
                // 如果开启了 close 后自动离开 group
                if (rebalanceConfig.leaveGroupOnClose) {
                    // 做一些清理和回调工作
                    onLeavePrepare();
                    maybeLeaveGroup("the consumer is being closed");
                }

                // At this point, there may be pending commits (async commits or sync commits that were
                // interrupted using wakeup) and the leave group request which have been queued, but not
                // yet sent to the broker. Wait up to close timeout for these pending requests to be processed.
                // If coordinator is not known, requests are aborted.

                // 此时，可能有挂起的提交（异步提交或使用 wakeup 中断的同步提交）和已排队但尚未发送到 broker 的 leave group 请求。
                // 等待关闭超时时间，以便处理这些挂起的请求。如果 coordinator 未知，则请求将被中止。
                Node coordinator = checkAndGetCoordinator();
                // 如果 coordinator 存在，则阻塞直到超时或者 pending 请求处理完毕
                if (coordinator != null &&
                        !client.awaitPendingRequests(coordinator, timer))
                    log.warn("Close timed out with {} pending requests to coordinator, terminating client connections",
                            client.pendingRequestCount(coordinator));
            }
        }
    }

    /**
     * Sends LeaveGroupRequest and logs the {@code leaveReason}, unless this member is using static membership or is already
     * not part of the group (ie does not have a valid member id, is in the UNJOINED state, or the coordinator is unknown).
     * <p>
     *     发送 LeaveGroupRequest 并记录 leaveReason，除非此成员正在使用静态成员身份或已经不再是该消费者组的一部分
     *     （即没有有效的 member ID，处于 UNJOINED 状态，或 coordinator 未知）。
     *
     * @param leaveReason the reason to leave the group for logging
     * @throws KafkaException if the rebalance callback throws exception
     */
    public synchronized RequestFuture<Void> maybeLeaveGroup(String leaveReason) {
        RequestFuture<Void> future = null;

        // Starting from 2.3, only dynamic members will send LeaveGroupRequest to the broker,
        // consumer with valid group.instance.id is viewed as static member that never sends LeaveGroup,
        // and the membership expiration is only controlled by session timeout.
        // 从 2.3 版本开始，只有 dynamic member 才会向 broker 发送 LeaveGroupRequest，
        // 具有有效 group.instance.id 的消费者被视为永远不会发送 LeaveGroup 的 static member，
        // 并且成员资格到期仅由会话超时控制。
        if (isDynamicMember() && // 是动态成员
                !coordinatorUnknown() && // 能找到 coordinator
                state != MemberState.UNJOINED && // 成员状态不是 UNJOINED
                generation.hasMemberId()) { // 能够获取 member ID

            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            // 这是一个最小的尝试离开组。如果请求失败或超时，我们不会尝试重新发送。
            log.info("Member {} sending LeaveGroup request to coordinator {} due to {}",
                generation.memberId, coordinator, leaveReason);
            // 构造一个 leave group 请求
            LeaveGroupRequest.Builder request = new LeaveGroupRequest.Builder(
                rebalanceConfig.groupId,
                Collections.singletonList(new MemberIdentity().setMemberId(generation.memberId))
            );

            // 将该请求放到缓冲区中
            future = client.send(coordinator, request).compose(new LeaveGroupResponseHandler(generation));
            // 触发一次 io
            client.pollNoWakeup();
        }

        // 重置 generation，并且重新加入 group
        resetGenerationOnLeaveGroup();

        return future;
    }

    protected boolean isDynamicMember() {
        return !rebalanceConfig.groupInstanceId.isPresent();
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        private LeaveGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            final List<MemberResponse> members = leaveResponse.memberResponses();
            if (members.size() > 1) {
                future.raise(new IllegalStateException("The expected leave group response " +
                                                           "should only contain no more than one member info, however get " + members));
            }

            final Errors error = leaveResponse.error();
            if (error == Errors.NONE) {
                log.debug("LeaveGroup response with {} returned successfully: {}", sentGeneration, response);
                future.complete(null);
            } else {
                log.error("LeaveGroup request with {} failed with error: {}", sentGeneration, error.message());
                future.raise(error);
            }
        }
    }

    // visible for testing
    /**
     * 向 coordinator 发送心跳请求
     * @return 心跳请求的 future
     */
    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        log.debug("Sending Heartbeat request with generation {} and member id {} to coordinator {}",
            generation.generationId, generation.memberId, coordinator);
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(new HeartbeatRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setGenerationId(this.generation.generationId));
        // 向 coordinator 发送心跳请求
        return client.send(coordinator, requestBuilder)
                .compose(new HeartbeatResponseHandler(generation));
    }

    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        private HeartbeatResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatSensor.record(response.requestLatencyMs());
            Errors error = heartbeatResponse.error();
            
            // 收到心跳响应成功
            if (error == Errors.NONE) {
                log.debug("Received successful Heartbeat response");
                future.complete(null);

                // 如果 coordinator 不可用
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                log.info("Attempt to heartbeat failed since coordinator {} is either not started or not valid",
                        coordinator());
                // 标记 coordinator 未知
                markCoordinatorUnknown(error);
                // 抛出异常
                future.raise(error);

                // 如果正在重新平衡
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                // since we may be sending the request during rebalance, we should check
                // this case and ignore the REBALANCE_IN_PROGRESS error
                // 由于我们可能正在重新平衡期间发送请求，因此我们应该检查这种情况并忽略 REBALANCE_IN_PROGRESS 错误
                synchronized (AbstractCoordinator.this) {
                    // 如果状态是 STABLE
                    if (state == MemberState.STABLE) {
                        // 请求重新加入
                        requestRejoin("group is already rebalancing");
                        // 抛出异常
                        future.raise(error);

                        // 否则，如果状态是其他值，忽略心跳响应
                    } else {
                        log.debug("Ignoring heartbeat response with error {} during {} state", error, state);
                        future.complete(null);
                    }
                }

                // 如果 generation 发生变化、未知成员 ID 或被实例 ID 包围
            } else if (error == Errors.ILLEGAL_GENERATION ||
                       error == Errors.UNKNOWN_MEMBER_ID ||
                       error == Errors.FENCED_INSTANCE_ID) {
                // 如果 generation 没有变化
                if (generationUnchanged()) {
                    log.info("Attempt to heartbeat with {} and group instance id {} failed due to {}, resetting generation",
                        sentGeneration, rebalanceConfig.groupInstanceId, error);
                    // 重置 generation
                    resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
                    // 抛出异常
                    future.raise(error);
                } else {
                    // if the generation has changed, then ignore this error
                    log.info("Attempt to heartbeat with stale {} and group instance id {} failed due to {}, ignoring the error",
                        sentGeneration, rebalanceConfig.groupInstanceId, error);
                    // 完成请求
                    future.complete(null);
                }
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        CoordinatorResponseHandler(final Generation generation) {
            this.sentGeneration = generation;
        }

        final Generation sentGeneration;
        ClientResponse response;

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                markCoordinatorUnknown(true, e.getMessage());
            }
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = (R) clientResponse.responseBody();
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

        boolean generationUnchanged() {
            synchronized (AbstractCoordinator.this) {
                return generation.equals(sentGeneration);
            }
        }
    }

    protected Meter createMeter(Metrics metrics, String groupName, String baseName, String descriptiveName) {
        return new Meter(new WindowedCount(),
                metrics.metricName(baseName + "-rate", groupName,
                        String.format("The number of %s per second", descriptiveName)),
                metrics.metricName(baseName + "-total", groupName,
                        String.format("The total number of %s", descriptiveName)));
    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;

        public final Sensor heartbeatSensor;
        public final Sensor joinSensor;
        public final Sensor syncSensor;
        public final Sensor successfulRebalanceSensor;
        public final Sensor failedRebalanceSensor;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatSensor = metrics.sensor("heartbeat-latency");
            this.heartbeatSensor.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatSensor.add(createMeter(metrics, metricGrpName, "heartbeat", "heartbeats"));

            this.joinSensor = metrics.sensor("join-latency");
            this.joinSensor.add(metrics.metricName("join-time-avg",
                this.metricGrpName,
                "The average time taken for a group rejoin"), new Avg());
            this.joinSensor.add(metrics.metricName("join-time-max",
                this.metricGrpName,
                "The max time taken for a group rejoin"), new Max());
            this.joinSensor.add(createMeter(metrics, metricGrpName, "join", "group joins"));

            this.syncSensor = metrics.sensor("sync-latency");
            this.syncSensor.add(metrics.metricName("sync-time-avg",
                this.metricGrpName,
                "The average time taken for a group sync"), new Avg());
            this.syncSensor.add(metrics.metricName("sync-time-max",
                this.metricGrpName,
                "The max time taken for a group sync"), new Max());
            this.syncSensor.add(createMeter(metrics, metricGrpName, "sync", "group syncs"));

            this.successfulRebalanceSensor = metrics.sensor("rebalance-latency");
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-avg",
                this.metricGrpName,
                "The average time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Avg());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-max",
                this.metricGrpName,
                "The max time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Max());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-total",
                this.metricGrpName,
                "The total number of milliseconds this consumer has spent in successful rebalances since creation"),
                new CumulativeSum());
            this.successfulRebalanceSensor.add(
                metrics.metricName("rebalance-total",
                    this.metricGrpName,
                    "The total number of successful rebalance events, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new CumulativeCount()
            );
            this.successfulRebalanceSensor.add(
                metrics.metricName(
                    "rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of successful rebalance events per hour, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            this.failedRebalanceSensor = metrics.sensor("failed-rebalance");
            this.failedRebalanceSensor.add(
                metrics.metricName("failed-rebalance-total",
                    this.metricGrpName,
                    "The total number of failed rebalance events"),
                new CumulativeCount()
            );
            this.failedRebalanceSensor.add(
                metrics.metricName(
                    "failed-rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of failed rebalance events per hour"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            Measurable lastRebalance = (config, now) -> {
                if (lastRebalanceEndMs == -1L)
                    // if no rebalance is ever triggered, we just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-rebalance-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last successful rebalance event"),
                lastRebalance);

            Measurable lastHeartbeat = (config, now) -> {
                if (heartbeat.lastHeartbeatSend() == 0L)
                    // if no heartbeat is ever triggered, just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last coordinator heartbeat was sent"),
                lastHeartbeat);
        }
    }

    /**
     * 心跳线程
     */
    private class HeartbeatThread extends KafkaThread implements AutoCloseable {
        // 是否启用
        private boolean enabled = false;
        // 是否关闭
        private boolean closed = false;
        // 失败原因
        private final AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        private HeartbeatThread() {
            super(HEARTBEAT_THREAD_PREFIX + (rebalanceConfig.groupId.isEmpty() ? "" : " | " + rebalanceConfig.groupId), true);
        }

        /**
         * 启用心跳线程
         */
        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Enabling heartbeat thread");
                this.enabled = true;
                heartbeat.resetTimeouts();
                AbstractCoordinator.this.notify();
            }
        }

        /**
         * 禁用心跳线程
         */
        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Disabling heartbeat thread");
                this.enabled = false;
            }
        }

        /**
         * 关闭心跳线程
         */
        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }

        /**
         * 是否失败
         * @return 失败返回 true，否则返回 false
         */
        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread started");
                // 持续循环
                while (true) {
                    // 锁住 AbstractCoordinator 对象
                    synchronized (AbstractCoordinator.this) {
                        // 如果关闭了，直接返回
                        if (closed)
                            return;

                        // 如果未启用，等待
                        if (!enabled) {
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        // we do not need to heartbeat we are not part of a group yet;
                        // also if we already have fatal error, the client will be
                        // crashed soon, hence we do not need to continue heartbeating either
                        // 我们还没有加入组，或者已经失败了，不需要继续心跳
                        if (state.hasNotJoinedGroup() || hasFailed()) {
                            // 禁用心跳线程
                            disable();
                            // 继续循环
                            continue;
                        }

                        // 触发 consumerNetworkClient 的 poll 方法
                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        // 如果 coordinator 未知
                        if (coordinatorUnknown()) {
                            // 如果 findCoordinatorFuture 不为空
                            if (findCoordinatorFuture != null) {
                                // clear the future so that after the backoff, if the hb still sees coordinator unknown in
                                // the next iteration it will try to re-discover the coordinator in case the main thread cannot
                                // 清除 findCoordinatorFuture，如果下次迭代中仍然未知，将尝试重新发现 coordinator
                                clearFindCoordinatorFuture();

                                // backoff properly
                                // 等待
                                AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                            } else {
                                // 查找 coordinator
                                lookupCoordinator();
                            }

                            // 如果 session 超时
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            // session 超时，没有收到心跳响应，所以我们应该确保 coordinator 仍然健康
                            markCoordinatorUnknown("session timed out without receiving a "
                                    + "heartbeat response");

                            // 如果 poll 超时
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll().
                            // poll 超时，意味着两次 poll 之间的时间间隔超过了 max.poll.interval.ms
                            log.warn("consumer poll timeout has expired. This means the time between subsequent calls to poll() " +
                                "was longer than the configured max.poll.interval.ms, which typically implies that " +
                                "the poll loop is spending too much time processing messages. You can address this " +
                                "either by increasing max.poll.interval.ms or by reducing the maximum size of batches " +
                                "returned in poll() with max.poll.records.");

                            // 离开组
                            maybeLeaveGroup("consumer poll timeout has expired.");

                            // 如果不需要心跳
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            // 等待
                            AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else {
                            // 发送心跳请求
                            heartbeat.sentHeartbeat(now);
                            // 构造心跳请求，并放到发送缓冲区中
                            final RequestFuture<Void> heartbeatFuture = sendHeartbeatRequest();
                            // 添加回调
                            heartbeatFuture.addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    // 收到心跳响应
                                    synchronized (AbstractCoordinator.this) {
                                        heartbeat.receiveHeartbeat();
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    // 收到心跳响应失败
                                    synchronized (AbstractCoordinator.this) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            // 在组重新平衡期间，继续发送心跳是有效的。这确保了 coordinator 在重新平衡超时持续时间内将成员保持在组中。
                                            // 如果我们停止发送心跳，那么会话超时可能会在我们重新加入之前到期。
                                            heartbeat.receiveHeartbeat();
                                        } else if (e instanceof FencedInstanceIdException) {
                                            log.error("Caught fenced group.instance.id {} error in heartbeat thread", rebalanceConfig.groupInstanceId);
                                            heartbeatThread.failed.set(e);
                                        } else {
                                            heartbeat.failHeartbeat();
                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (AuthenticationException e) {
                log.error("An authentication error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (GroupAuthorizationException e) {
                log.error("A group authorization error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread", e);
                this.failed.set(new RuntimeException(e));
            } catch (Throwable e) {
                log.error("Heartbeat thread failed due to unexpected error", e);
                if (e instanceof RuntimeException)
                    this.failed.set((RuntimeException) e);
                else
                    this.failed.set(new RuntimeException(e));
            } finally {
                log.debug("Heartbeat thread has closed");
            }
        }

    }

    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null);

        public final int generationId;
        public final String memberId;
        public final String protocolName;

        public Generation(int generationId, String memberId, String protocolName) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocolName = protocolName;
        }

        /**
         * @return true if this generation has a valid member id, false otherwise. A member might have an id before
         * it becomes part of a group generation.
         */
        public boolean hasMemberId() {
            return !memberId.isEmpty();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Generation that = (Generation) o;
            return generationId == that.generationId &&
                    Objects.equals(memberId, that.memberId) &&
                    Objects.equals(protocolName, that.protocolName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generationId, memberId, protocolName);
        }

        @Override
        public String toString() {
            return "Generation{" +
                    "generationId=" + generationId +
                    ", memberId='" + memberId + '\'' +
                    ", protocol='" + protocolName + '\'' +
                    '}';
        }
    }

    @SuppressWarnings("serial")
    private static class UnjoinedGroupException extends RetriableException {

    }

    // For testing only below
    final Heartbeat heartbeat() {
        return heartbeat;
    }

    final synchronized void setLastRebalanceTime(final long timestamp) {
        lastRebalanceEndMs = timestamp;
    }

    /**
     * Check whether given generation id is matching the record within current generation.
     *
     * @param generationId generation id
     * @return true if the two ids are matching.
     */
    final boolean hasMatchingGenerationId(int generationId) {
        return !generation.equals(Generation.NO_GENERATION) && generation.generationId == generationId;
    }

    final boolean hasUnknownGeneration() {
        return generation.equals(Generation.NO_GENERATION);
    }

    /**
     * @return true if the current generation's member ID is valid, false otherwise
     */
    final boolean hasValidMemberId() {
        return !hasUnknownGeneration() && generation.hasMemberId();
    }

    final synchronized void setNewGeneration(final Generation generation) {
        this.generation = generation;
    }

    final synchronized void setNewState(final MemberState state) {
        this.state = state;
    }
}
