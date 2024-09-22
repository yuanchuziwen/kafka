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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CorrelationIdMismatchException;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * 一个用于异步请求/响应网络 I/O 的网络客户端。
 * 这是一个内部类，用于实现用户侧的 producer 和 consumer 客户端。
 * <p>
 * This class is not thread-safe!
 * 这个类不是线程安全的！
 */
public class NetworkClient implements KafkaClient {

    /* Logger for logging */
    /* 日志记录器 */
    private final Logger log;
    /* the selector used to perform network i/o */
    /* 用于执行网络 I/O 的选择器 */
    private final Selectable selector;
    /* Metadata updater */
    /* 元数据更新器 */
    private final MetadataUpdater metadataUpdater;
    /* Random offset generator */
    /* 随机偏移量生成器 */
    private final Random randOffset;
    /* the state of each node's connection */
    /* 每个节点连接状态 */
    private final ClusterConnectionStates connectionStates;
    /* the set of requests currently being sent or awaiting a response */
    /* 当前正在发送或等待响应的请求集合 */
    private final InFlightRequests inFlightRequests;
    /* the socket send buffer size in bytes */
    /* 套接字发送缓冲区大小（字节） */
    private final int socketSendBuffer;
    /* the socket receive size buffer in bytes */
    /* 套接字接收缓冲区大小（字节） */
    private final int socketReceiveBuffer;
    /* the client id used to identify this client in requests to the server */
    /* 用于标识此客户端在向服务器发送请求时的客户端 ID */
    private final String clientId;
    /* default timeout for individual requests to await acknowledgement from servers */
    /* 单个请求等待服务器确认的默认超时时间（毫秒） */
    private final int defaultRequestTimeoutMs;
    /* time in ms to wait before retrying to create connection to a server */
    /* 在重试创建与服务器的连接之前等待的时间（毫秒） */
    private final long reconnectBackoffMs;
    /* Time utility class */
    /* 时间工具类 */
    private final Time time;
    /**
     * True if we should send an ApiVersionRequest when first connecting to a broker.
     * 如果在首次连接到代理时应发送 ApiVersionRequest，则为 true
     */
    private final boolean discoverBrokerVersions;
    /* API versions information */
    /* API 版本信息 */
    private final ApiVersions apiVersions;
    /* Map of nodes needing API versions fetch */
    /* 需要获取 API 版本的节点映射 */
    private final Map<String, ApiVersionsRequest.Builder> nodesNeedingApiVersionsFetch = new HashMap<>();
    /* List of aborted sends */
    /* 已中止发送的响应列表 */
    private final List<ClientResponse> abortedSends = new LinkedList<>();
    /* Throttle time sensor */
    /* 节流时间传感器 */
    private final Sensor throttleTimeSensor;
    /* State of the network client */
    /* 网络客户端的状态 */
    private final AtomicReference<State> state;
    /* the current correlation id to use when sending requests to servers */
    /* 发送请求到服务器时使用的当前关联 ID */
    private int correlation;

    public NetworkClient(MetadataUpdater metadataUpdater,
                         Metadata metadata,
                         Selectable selector,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext,
                         HostResolver hostResolver) {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         * <p>
         * 如果我们可以从公共构造函数传递 `DefaultMetadataUpdater`，那将更好，但这是不可能的，
         * 因为 `DefaultMetadataUpdater` 是一个内部类，只能在调用超级构造函数之后实例化。
         */

        // 为成员变量 metadataUpdater 赋值
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(
                reconnectBackoffMs, reconnectBackoffMax,
                connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, logContext, hostResolver);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.time = time;
        this.discoverBrokerVersions = discoverBrokerVersions;
        this.apiVersions = apiVersions;
        this.throttleTimeSensor = throttleTimeSensor;
        this.log = logContext.logger(NetworkClient.class);
        this.state = new AtomicReference<>(State.ACTIVE);
    }

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(selector,
             metadata,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext);
    }

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext) {
        this(null,
             metadata,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             throttleTimeSensor,
             logContext,
             new DefaultHostResolver());
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(metadataUpdater,
             null,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext,
             new DefaultHostResolver());
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     * <p>
     *     开始连接给定的节点，如果我们已经连接并准备好向该节点发送请求，则返回 true。
     *
     * @param node The node to check
     *             要检查的节点
     * @param now The current timestamp
     *            当前时间戳
     * @return True if we are ready to send to the given node
     *        如果我们准备好向给定节点发送请求，则返回 true
     */
    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        // 判断当前节点是否已经准备好
        if (isReady(node, now))
            return true;

        // 如果 state 表示可以链接这个节点，那么就尝试连接这个节点
        // 内部考虑到 backoff，可能会返回 false
        if (connectionStates.canConnect(node.idString(), now))
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            // 如果我们有兴趣向节点发送请求，但没有与其建立连接，则初始化连接
            initiateConnect(node, now);

        return false;
    }

    /**
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     * <p>
     *     断开与特定节点的连接（如果有的话）。
     *     此连接的任何挂起的 ClientRequest 将收到断开连接的通知。
     *
     * @param nodeId The id of the node
     */
    @Override
    public void disconnect(String nodeId) {
        // 如果这个节点已经断开连接，直接返回
        if (connectionStates.isDisconnected(nodeId))
            return;

        // 调用 selector 的 close 方法关闭连接
        selector.close(nodeId);
        long now = time.milliseconds();

        // 取消所有与这个节点相关的请求
        cancelInFlightRequests(nodeId, now, abortedSends);

        // 更新这个节点的连接状态
        connectionStates.disconnected(nodeId, now);

        if (log.isTraceEnabled()) {
            log.trace("Manually disconnected from {}. Aborted in-flight requests: {}.", nodeId, inFlightRequests);
        }
    }

    // Visible for testing
    boolean canConnect(Node node, long now) {
        return connectionStates.canConnect(node.idString(), now);
    }

    private void cancelInFlightRequests(String nodeId, long now, Collection<ClientResponse> responses) {
        // 从 inFlightRequests 中清除所有与这个节点相关的请求
        Iterable<InFlightRequest> inFlightRequests = this.inFlightRequests.clearAll(nodeId);
        for (InFlightRequest request : inFlightRequests) {
            log.trace("Cancelled request {} {} with correlation id {} due to node {} being disconnected",
                    request.header.apiKey(), request.request, request.header.correlationId(), nodeId);

            // 为这些 request 增加一个 disconnected 的 ClientResponse
            if (!request.isInternalRequest) {
                if (responses != null)
                    responses.add(request.disconnected(now, null));

                // 如果存在 metadata 相关的更新请求，那么就触发 metadata 更新失败的处理
            } else if (request.header.apiKey() == ApiKeys.METADATA) {
                metadataUpdater.handleFailedRequest(now, Optional.empty());
            }
        }
    }

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     * <p>
     *     关闭与特定节点的连接（如果有的话）。
     *     该连接上的所有请求将被清除。对于已清除的请求，不会调用 ClientRequest 回调，也不会从 poll() 返回。
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        // 这个方法好像在 scala 代码中被用到
        // selector 关闭连接
        selector.close(nodeId);
        long now = time.milliseconds();
        // 取消所有与这个节点相关的请求
        cancelInFlightRequests(nodeId, now, null);
        // 移除这个节点的连接状态
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     * <p>
     *     根据连接状态返回等待的毫秒数，然后尝试发送数据。
     *     当断开连接时，这会尊重重新连接的回退时间。
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    /**
     * Return the poll delay in milliseconds based on both connection and throttle delay.
     * <p>
     *     根据连接和节流延迟返回轮询延迟（毫秒）。
     *
     * @param node the connection to check
     * @param now the current time in ms
     */
    @Override
    public long pollDelayMs(Node node, long now) {
        return connectionStates.pollDelayMs(node.idString(), now);
    }

    // Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
    // This is for testing.
    public long throttleDelayMs(Node node, long now) {
        return connectionStates.throttleDelayMs(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     * <p>
     *     根据连接状态检查节点的连接是否失败。
     *     这种连接失败通常是短暂的，可以在下一次 {@link #ready(org.apache.kafka.common.Node, long)} 调用中恢复，
     *     但也有一些情况下，需要捕获短暂的失败并重新处理。
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.isDisconnected(node.idString());
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     * <p>
     *     检查具有给定 ID 的节点是否准备好发送更多请求。
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString(), now);
    }

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    @Override
    public AuthenticationException authenticationException(Node node) {
        return connectionStates.authenticationException(node.idString());
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     * <p>
     *     我们是否已连接并准备好，能够向给定连接发送更多请求？
     *
     * @param node The node
     * @param now the current timestamp
     */
    private boolean canSendRequest(String node, long now) {
        return connectionStates.isReady(node, now) && // 这个 node 的连接状态为 READY
                selector.isChannelReady(node) && // selector 针对这个节点的 channel 是准备好的
                inFlightRequests.canSendMore(node); // 针对这个 node 还能发送更多的未完成请求
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * <p>
     *     将给定请求排队等待发送。请求只能发送到准备好的节点。
     * @param request The request
     * @param now The current timestamp
     */
    @Override
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    /**
     * 网络客户端的状态
     */
    private enum State {
        ACTIVE,
        CLOSING,
        CLOSED
    }

    // package-private for testing
    // 由 MetadataUpdater 调用，用于发送元数据请求
    void sendInternalMetadataRequest(MetadataRequest.Builder builder, String nodeConnectionId, long now) {
        // 创建元数据请求
        ClientRequest clientRequest = newClientRequest(nodeConnectionId, builder, now, true);
        // 发送元数据请求
        doSend(clientRequest, true, now);
    }

    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        // 确保客户端处于活动状态
        ensureActive();
        // 获取目标 nodeId
        String nodeId = clientRequest.destination();
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            // 如果请求来自 NetworkClient 外部，请验证我们是否可以发送数据。
            // 如果请求是内部的，我们相信内部代码已经进行了此验证。
            // 对于某些内部请求，验证将略有不同（例如，ApiVersionsRequests 可以在 READY 状态之前发送）。
            if (!canSendRequest(nodeId, now))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            NodeApiVersions versionInfo = apiVersions.get(nodeId);
            short version;
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            // 注意：如果 versionInfo 为 null，则没有服务器版本信息。
            // 这种情况发生在发送初始的 ApiVersionRequest 时，该请求会获取版本信息本身。
            // 还有一种情况是当 discoverBrokerVersions 设置为 false 时。
            if (versionInfo == null) {
                version = builder.latestAllowedVersion();
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending {} with correlation id {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), clientRequest.correlationId(), nodeId, version);
            } else {
                version = versionInfo.latestUsableVersion(clientRequest.apiKey(), builder.oldestAllowedVersion(),
                        builder.latestAllowedVersion());
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.
            // 如果有一些必要字段无法在所选版本中表示，则调用 build 也可能抛出 UnsupportedVersionException。
            doSend(clientRequest, isInternalRequest, now, builder.build(version));
        } catch (UnsupportedVersionException unsupportedVersionException) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            // 如果不支持版本，则跳过通过网络发送请求。
            // 相反，只需将其添加到已中止请求的本地队列中。
            log.debug("Version mismatch when attempting to send {} with correlation id {} to {}", builder,
                    clientRequest.correlationId(), clientRequest.destination(), unsupportedVersionException);
            // 这里直接构造一个 ClientResponse 对象，然后添加到 abortedSends 列表中
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, unsupportedVersionException, null, null);

            if (!isInternalRequest)
                abortedSends.add(clientResponse);
            else if (clientRequest.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleFailedRequest(now, Optional.of(unsupportedVersionException));
        }
    }

    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
        String destination = clientRequest.destination();
        // 构造一个消息的 header
        // 其实有两个 header，有一个是 requestHeader，另一个是 recordHeader
        RequestHeader header = clientRequest.makeHeader(request.version());
        if (log.isDebugEnabled()) {
            log.debug("Sending {} request with header {} and timeout {} to node {}: {}",
                clientRequest.apiKey(), header, clientRequest.requestTimeoutMs(), destination, request);
        }
        // 将 `request` 封装为 `Send` 对象
        Send send = request.toSend(header);
        // 封装为 `InFlightRequest` 对象
        InFlightRequest inFlightRequest = new InFlightRequest(
                clientRequest,
                header,
                isInternalRequest,
                request,
                send,
                now);
        this.inFlightRequests.add(inFlightRequest);
        // 交给 selector 来发送请求
        selector.send(new NetworkSend(clientRequest.destination(), send));
    }

    /**
     * Do actual reads and writes to sockets.
     * <p>
     *     执行实际的读写操作。
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        // 确保客户端处于活动状态
        ensureActive();

        // 处理已中止的发送请求
        // 检查 `abortedSends` 列表是否为空，如果不为空，则立即处理这些请求并返回响应。
        if (!abortedSends.isEmpty()) {
            // If there are aborted sends because of unsupported version exceptions or disconnects,
            // handle them immediately without waiting for Selector#poll.
            // 如果有因为不支持的版本异常或断开而中止的发送请求，则立即处理它们，而不需要等待 Selector#poll。
            List<ClientResponse> responses = new ArrayList<>();
            // 将 `abortedSends` 添加到 `responses` 列表中，并清空 `abortedSends` 列表。
            handleAbortedSends(responses);
            // 触发 `ClientResponse` 的 `onComplete` 回调
            completeResponses(responses);
            // 注意：这里会直接返回
            return responses;
        }

        // 更新元数据
        // 调用 `metadataUpdater.maybeUpdate(now)` 方法获取元数据更新的超时时间
        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            // 执行网络 I/O 操作
            // 调用 `selector.poll()` 方法进行实际的读写操作，等待响应
            this.selector.poll(Utils.min(timeout, metadataTimeout, defaultRequestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        // 处理完成的操作
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        // 处理已完成的发送请求
        handleCompletedSends(responses, updatedNow);
        // 处理已完成的接收请求
        handleCompletedReceives(responses, updatedNow);
        // 处理断开的连接
        handleDisconnections(responses, updatedNow);
        // 处理新的连接
        handleConnections();
        // 处理 API 版本请求的初始化
        handleInitiateApiVersionRequests(updatedNow);
        // 处理超时的连接
        handleTimedOutConnections(responses, updatedNow);
        // 处理超时的请求
        handleTimedOutRequests(responses, updatedNow);
        // 触发 `ClientResponse` 的 `onComplete` 方法
        completeResponses(responses);

        return responses;
    }

    private void completeResponses(List<ClientResponse> responses) {
        for (ClientResponse response : responses) {
            try {
                response.onComplete();
            } catch (Exception e) {
                log.error("Uncaught error in request completion:", e);
            }
        }
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.count();
    }

    @Override
    public boolean hasInFlightRequests() {
        return !this.inFlightRequests.isEmpty();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.count(node);
    }

    @Override
    public boolean hasInFlightRequests(String node) {
        return !this.inFlightRequests.isEmpty(node);
    }

    @Override
    public boolean hasReadyNodes(long now) {
        return connectionStates.hasReadyNodes(now);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    @Override
    public void initiateClose() {
        if (state.compareAndSet(State.ACTIVE, State.CLOSING)) {
            wakeup();
        }
    }

    @Override
    public boolean active() {
        return state.get() == State.ACTIVE;
    }

    private void ensureActive() {
        if (!active())
            throw new DisconnectException("NetworkClient is no longer active, state is " + state);
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        // cas 更新状态为 `CLOSING`
        state.compareAndSet(State.ACTIVE, State.CLOSING);
        // cas 更新状态为 `CLOSED`
        if (state.compareAndSet(State.CLOSING, State.CLOSED)) {
            this.selector.close();
            this.metadataUpdater.close();
        } else {
            log.warn("Attempting to close NetworkClient that has already been closed.");
        }
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. If no connection exists, this method will prefer a node
     * with least recent connection attempts. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period, or an active
     * connection which is being throttled.
     * <p>
     * 选择具有最少未完成请求的节点，该节点至少具有连接的资格。
     * 此方法将优先选择具有现有连接的节点，但如果所有现有连接都已使用，则会选择一个至少具有连接资格的节点。
     * 如果没有连接，此方法将优先选择最近连接尝试次数最少的节点。
     * 此方法永远不会选择没有现有连接的节点，或者从该节点断开连接的时间在重连回退期内，或者一个正在被限速的活跃连接。
     * <p>
     *     这个方法主要是发送 metadata 请求、寻找 transactionCoordinator 请求等时使用
     * 
     * @return The node with the fewest in-flight requests.
     */
    @Override
    public Node leastLoadedNode(long now) {
        // 获取当前集群节点
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        // 如果没有节点，则抛出异常
        if (nodes.isEmpty())
            throw new IllegalStateException("There are no nodes in the Kafka cluster");
        int inflight = Integer.MAX_VALUE;

        // 正在连接的节点
        Node foundConnecting = null;
        // 可以连接的节点
        Node foundCanConnect = null;
        // 已连接的节点
        Node foundReady = null;

        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);
            // 如果可以发送请求，则处理
            if (canSendRequest(node.idString(), now)) {
                int currInflight = this.inFlightRequests.count(node.idString());
                if (currInflight == 0) {
                    // if we find an established connection with no in-flight requests we can stop right away
                    // 如果找到一个已建立连接且没有未完成请求的节点，则立即返回
                    log.trace("Found least loaded node {} connected with no in-flight requests", node);
                    return node;
                } else if (currInflight < inflight) {
                    // otherwise if this is the best we have found so far, record that
                    // 如果这是迄今为止找到的最佳节点，则记录
                    inflight = currInflight;
                    foundReady = node;
                }
            } else if (connectionStates.isPreparingConnection(node.idString())) {
                // 如果正在准备连接，则记录
                foundConnecting = node;
            } else if (canConnect(node, now)) {
                // 如果可以连接，则记录
                if (foundCanConnect == null ||
                        this.connectionStates.lastConnectAttemptMs(foundCanConnect.idString()) >
                                this.connectionStates.lastConnectAttemptMs(node.idString())) {
                    foundCanConnect = node;
                }
            } else {
                log.trace("Removing node {} from least loaded node selection since it is neither ready " +
                        "for sending or connecting", node);
            }
        }

        // We prefer established connections if possible. Otherwise, we will wait for connections
        // which are being established before connecting to new nodes.
        if (foundReady != null) {
            log.trace("Found least loaded node {} with {} inflight requests", foundReady, inflight);
            return foundReady;
        } else if (foundConnecting != null) {
            log.trace("Found least loaded connecting node {}", foundConnecting);
            return foundConnecting;
        } else if (foundCanConnect != null) {
            log.trace("Found least loaded node {} with no active connection", foundCanConnect);
            return foundCanConnect;
        } else {
            log.trace("Least loaded node selection failed to find an available node");
            return null;
        }
    }

    public static AbstractResponse parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        try {
            return AbstractResponse.parseResponse(responseBuffer, requestHeader);
        } catch (BufferUnderflowException e) {
            throw new SchemaException("Buffer underflow while parsing response for request with header " + requestHeader, e);
        } catch (CorrelationIdMismatchException e) {
            if (SaslClientAuthenticator.isReserved(requestHeader.correlationId())
                && !SaslClientAuthenticator.isReserved(e.responseCorrelationId()))
                throw new SchemaException("The response is unrelated to Sasl request since its correlation id is "
                    + e.responseCorrelationId() + " and the reserved range for Sasl request is [ "
                    + SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + ","
                    + SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + "]");
            else {
                throw e;
            }
        }
    }

    /**
     * Post process disconnection of a node
     * <p>
     *     处理节点的断开连接
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     * @param disconnectState The state of the disconnected channel
     */
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState) {
        connectionStates.disconnected(nodeId, now);
        apiVersions.remove(nodeId);
        nodesNeedingApiVersionsFetch.remove(nodeId);
        switch (disconnectState.state()) {
            case AUTHENTICATION_FAILED:
                AuthenticationException exception = disconnectState.exception();
                connectionStates.authenticationFailed(nodeId, now, exception);
                log.error("Connection to node {} ({}) failed authentication due to: {}", nodeId,
                    disconnectState.remoteAddress(), exception.getMessage());
                break;
            case AUTHENTICATE:
                log.warn("Connection to node {} ({}) terminated during authentication. This may happen " +
                    "due to any of the following reasons: (1) Authentication failed due to invalid " +
                    "credentials with brokers older than 1.0.0, (2) Firewall blocking Kafka TLS " +
                    "traffic (eg it may only allow HTTPS traffic), (3) Transient network issue.",
                    nodeId, disconnectState.remoteAddress());
                break;
            case NOT_CONNECTED:
                log.warn("Connection to node {} ({}) could not be established. Broker may not be available.", nodeId, disconnectState.remoteAddress());
                break;
            default:
                break; // Disconnections in other states are logged at debug level in Selector
        }

        cancelInFlightRequests(nodeId, now, responses);
        metadataUpdater.handleServerDisconnect(now, nodeId, Optional.ofNullable(disconnectState.exception()));
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     * <p>
     *     遍历所有未完成的请求，并处理超时的请求。
     *     与请求关联的节点的连接将被终止，并将被视为断开连接。
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        List<String> nodeIds = this.inFlightRequests.nodesWithTimedOutRequests(now);
        for (String nodeId : nodeIds) {
            // close connection to the node
            this.selector.close(nodeId);
            log.debug("Disconnecting from node {} due to request timeout.", nodeId);
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE);
        }
    }

    private void handleAbortedSends(List<ClientResponse> responses) {
        responses.addAll(abortedSends);
        abortedSends.clear();
    }

    /**
     * Handle socket channel connection timeout. The timeout will hit iff a connection
     * stays at the ConnectionState.CONNECTING state longer than the timeout value,
     * as indicated by ClusterConnectionStates.NodeConnectionState.
     * <p>
     *     处理套接字通道连接超时。
     *     如果连接保持在 ConnectionState.CONNECTING 状态的时间超过超时值，则超时会发生，
     *     这是由 ClusterConnectionStates.NodeConnectionState 指示的。
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutConnections(List<ClientResponse> responses, long now) {
        List<String> nodes = connectionStates.nodesWithConnectionSetupTimeout(now);
        for (String nodeId : nodes) {
            this.selector.close(nodeId);
            log.debug(
                "Disconnecting from node {} due to socket connection setup timeout. " +
                "The timeout value is {} ms.",
                nodeId,
                connectionStates.connectionSetupTimeoutMs(nodeId));
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE);
        }
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     * <p>
     *     处理任何已完成的请求发送。特别是如果不需要响应，则认为请求已完成。
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        // 如果不需要响应，则在发送完成时返回
        for (NetworkSend send : this.selector.completedSends()) {
            InFlightRequest request = this.inFlightRequests.lastSent(send.destinationId());
            if (!request.expectResponse) {
                this.inFlightRequests.completeLastSent(send.destinationId());
                responses.add(request.completed(null, now));
            }
        }
    }

    /**
     * If a response from a node includes a non-zero throttle delay and client-side throttling has been enabled for
     * the connection to the node, throttle the connection for the specified delay.
     *
     * @param response the response
     * @param apiVersion the API version of the response
     * @param nodeId the id of the node
     * @param now The current time
     */
    private void maybeThrottle(AbstractResponse response, short apiVersion, String nodeId, long now) {
        int throttleTimeMs = response.throttleTimeMs();
        if (throttleTimeMs > 0 && response.shouldClientThrottle(apiVersion)) {
            connectionStates.throttle(nodeId, now + throttleTimeMs);
            log.trace("Connection to node {} is throttled for {} ms until timestamp {}", nodeId, throttleTimeMs,
                      now + throttleTimeMs);
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     * <p>
     *     处理任何已完成的接收，并使用接收到的响应更新响应列表。
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        for (NetworkReceive receive : this.selector.completedReceives()) {
            String source = receive.source();
            InFlightRequest req = inFlightRequests.completeNext(source);

            AbstractResponse response = parseResponse(receive.payload(), req.header);
            if (throttleTimeSensor != null)
                throttleTimeSensor.record(response.throttleTimeMs(), now);

            if (log.isDebugEnabled()) {
                log.debug("Received {} response from node {} for request with header {}: {}",
                    req.header.apiKey(), req.destination, req.header, response);
            }

            // If the received response includes a throttle delay, throttle the connection.
            maybeThrottle(response, req.header.apiVersion(), req.destination, now);
            if (req.isInternalRequest && response instanceof MetadataResponse)
                metadataUpdater.handleSuccessfulResponse(req.header, now, (MetadataResponse) response);
            else if (req.isInternalRequest && response instanceof ApiVersionsResponse)
                handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) response);
            else
                responses.add(req.completed(response, now));
        }
    }

    private void handleApiVersionsResponse(List<ClientResponse> responses,
                                           InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
        final String node = req.destination;
        if (apiVersionsResponse.data().errorCode() != Errors.NONE.code()) {
            if (req.request.version() == 0 || apiVersionsResponse.data().errorCode() != Errors.UNSUPPORTED_VERSION.code()) {
                log.warn("Received error {} from node {} when making an ApiVersionsRequest with correlation id {}. Disconnecting.",
                        Errors.forCode(apiVersionsResponse.data().errorCode()), node, req.header.correlationId());
                this.selector.close(node);
                processDisconnection(responses, node, now, ChannelState.LOCAL_CLOSE);
            } else {
                // Starting from Apache Kafka 2.4, ApiKeys field is populated with the supported versions of
                // the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
                // If not provided, the client falls back to version 0.
                short maxApiVersion = 0;
                if (apiVersionsResponse.data().apiKeys().size() > 0) {
                    ApiVersion apiVersion = apiVersionsResponse.data().apiKeys().find(ApiKeys.API_VERSIONS.id);
                    if (apiVersion != null) {
                        maxApiVersion = apiVersion.maxVersion();
                    }
                }
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder(maxApiVersion));
            }
            return;
        }
        NodeApiVersions nodeVersionInfo = new NodeApiVersions(apiVersionsResponse.data().apiKeys());
        apiVersions.update(node, nodeVersionInfo);
        this.connectionStates.ready(node);
        log.debug("Node {} has finalized features epoch: {}, finalized features: {}, supported features: {}, API versions: {}.",
                node, apiVersionsResponse.data().finalizedFeaturesEpoch(), apiVersionsResponse.data().finalizedFeatures(),
                apiVersionsResponse.data().supportedFeatures(), nodeVersionInfo);
    }

    /**
     * Handle any disconnected connections
     * <p>
     *     处理任何断开的连接
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        for (Map.Entry<String, ChannelState> entry : this.selector.disconnected().entrySet()) {
            String node = entry.getKey();
            log.debug("Node {} disconnected.", node);
            processDisconnection(responses, node, now, entry.getValue());
        }
    }

    /**
     * Record any newly completed connections
     * <p>
     *     记录任何新完成的连接
     */
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            // We are now connected.  Note that we might not still be able to send requests. For instance,
            // if SSL is enabled, the SSL handshake happens after the connection is established.
            // Therefore, it is still necessary to check isChannelReady before attempting to send on this
            // connection.
            // 我们现在已连接。请注意，我们可能仍无法发送请求。
            // 例如，如果启用了 SSL，则 SSL 握手会在连接建立后进行。
            // 因此，在尝试在此连接上发送之前，仍然需要检查 isChannelReady。
            if (discoverBrokerVersions) {
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder());
                log.debug("Completed connection to node {}. Fetching API versions.", node);
            } else {
                this.connectionStates.ready(node);
                log.debug("Completed connection to node {}. Ready.", node);
            }
        }
    }

    /**
     * 处理发起的 API 版本请求
     * 
     * @param now 当前时间
     */
    private void handleInitiateApiVersionRequests(long now) {
        Iterator<Map.Entry<String, ApiVersionsRequest.Builder>> iter = nodesNeedingApiVersionsFetch.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, ApiVersionsRequest.Builder> entry = iter.next();
            String node = entry.getKey();
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node);
                // We transition the connection to the CHECKING_API_VERSIONS state only when
                // the ApiVersionsRequest is queued up to be sent out. Without this, the client
                // could remain in the CHECKING_API_VERSIONS state forever if the channel does
                // not before ready.
                // 仅当 ApiVersionsRequest 排队等待发送时，我们才将连接转换为 CHECKING_API_VERSIONS 状态。
                // 否则，如果通道在准备好之前不会保持 CHECKING_API_VERSIONS 状态，客户端可能会永远保持 CHECKING_API_VERSIONS 状态。
                this.connectionStates.checkingApiVersions(node);
                // 这里会发送一个 ApiVersionsRequest 请求
                ApiVersionsRequest.Builder apiVersionRequestBuilder = entry.getValue();
                ClientRequest clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true);
                doSend(clientRequest, true, now);
                iter.remove();
            }
        }
    }

    /**
     * Initiate a connection to the given node
     * 初始化到给定节点的连接
     * 
     * @param node the node to connect to
     * @param now current time in epoch milliseconds
     */
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            // 将节点连接状态设置为连接中
            connectionStates.connecting(nodeConnectionId, now, node.host());
            // 获取节点的地址
            InetAddress address = connectionStates.currentAddress(nodeConnectionId);
            // 记录日志
            log.debug("Initiating connection to node {} using address {}", node, address);
            // 连接到节点
            selector.connect(nodeConnectionId,
                    new InetSocketAddress(address, node.port()),
                    this.socketSendBuffer,
                    this.socketReceiveBuffer);
        } catch (IOException e) {
            log.warn("Error connecting to node {}", node, e);
            // Attempt failed, we'll try again after the backoff
            connectionStates.disconnected(nodeConnectionId, now);
            // Notify metadata updater of the connection failure
            metadataUpdater.handleServerDisconnect(now, nodeConnectionId, Optional.empty());
        }
    }

    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        // 当前集群元数据
        private final Metadata metadata;

        // Defined if there is a request in progress, null otherwise
        // 如果正在进行请求，则定义；否则为 null
        private InProgressData inProgress;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.inProgress = null;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            // 没有正在进行的请求，且当前时间距离下一次元数据更新时间小于等于 0
            return !hasFetchInProgress() && this.metadata.timeToNextUpdate(now) == 0;
        }

        private boolean hasFetchInProgress() {
            return inProgress != null;
        }

        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            // 判断是否需要更新元数据
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            long waitForMetadataFetch = hasFetchInProgress() ? defaultRequestTimeoutMs : 0;

            long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);
            if (metadataTimeout > 0) {
                return metadataTimeout;
            }

            // Beware that the behavior of this method and the computation of timeouts for poll() are
            // highly dependent on the behavior of leastLoadedNode.
            // 注意：leastLoadedNode 的行为与 poll() 的超时计算高度相关。
            Node node = leastLoadedNode(now);
            if (node == null) {
                // 如果没有节点可用，则返回重连退避时间
                log.debug("Give up sending metadata request since no node is available");
                return reconnectBackoffMs;
            }

            // 更新元数据
            return maybeUpdate(now, node);
        }

        @Override
        public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeFatalException) {
            Cluster cluster = metadata.fetch();
            // 'processDisconnection' generates warnings for misconfigured bootstrap server configuration
            // resulting in 'Connection Refused' and misconfigured security resulting in authentication failures.
            // The warning below handles the case where a connection to a broker was established, but was disconnected
            // before metadata could be obtained.
            if (cluster.isBootstrapConfigured()) {
                int nodeId = Integer.parseInt(destinationId);
                Node node = cluster.nodeById(nodeId);
                if (node != null)
                    log.warn("Bootstrap broker {} disconnected", node);
            }

            // If we have a disconnect while an update is due, we treat it as a failed update
            // so that we can backoff properly
            if (isUpdateDue(now))
                handleFailedRequest(now, Optional.empty());

            maybeFatalException.ifPresent(metadata::fatalError);

            // The disconnect may be the result of stale metadata, so request an update
            metadata.requestUpdate();
        }

        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            maybeFatalException.ifPresent(metadata::fatalError);
            metadata.failedUpdate(now);
            inProgress = null;
        }

        @Override
        public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
            // If any partition has leader with missing listeners, log up to ten of these partitions
            // for diagnosing broker configuration issues.
            // This could be a transient issue if listeners were added dynamically to brokers.
            // 如果任何分区具有缺少监听器的领导者，则记录多达十个这样的分区，以诊断配置问题。
            // 如果监听器动态添加到 broker，这可能是一个短暂的问题。
            List<TopicPartition> missingListenerPartitions = response.topicMetadata().stream().flatMap(topicMetadata ->
                topicMetadata.partitionMetadata().stream()
                    .filter(partitionMetadata -> partitionMetadata.error == Errors.LISTENER_NOT_FOUND)
                    .map(partitionMetadata -> new TopicPartition(topicMetadata.topic(), partitionMetadata.partition())))
                .collect(Collectors.toList());
            // 如果缺少监听器的分区不为空，则记录日志
            if (!missingListenerPartitions.isEmpty()) {
                int count = missingListenerPartitions.size();
                log.warn("{} partitions have leader brokers without a matching listener, including {}",
                        count, missingListenerPartitions.subList(0, Math.min(10, count)));
            }

            // Check if any topic's metadata failed to get updated
            // 检查是否有任何主题的元数据未更新
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", requestHeader.correlationId(), errors);

            // When talking to the startup phase of a broker, it is possible to receive an empty metadata set, which
            // we should retry later.
            // 当与启动阶段的 broker 通信时，可能收到一个空的元数据集，我们应该稍后再重试。
            if (response.brokers().isEmpty()) {
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
                this.metadata.failedUpdate(now);
            } else {
                this.metadata.update(inProgress.requestVersion, response, inProgress.isPartialUpdate, now);
            }

            inProgress = null;
        }

        @Override
        public void close() {
            this.metadata.close();
        }

        /**
         * Return true if there's at least one connection establishment is currently underway
         * 
         * @return 如果至少有一个连接建立正在进行，则返回 true；否则返回 false
         */
        private boolean isAnyNodeConnecting() {
            for (Node node : fetchNodes()) {
                if (connectionStates.isConnecting(node.idString())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         * 将元数据请求添加到发送列表中，如果可以发送
         * 
         * @param now 当前时间
         * @param node 节点
         * @return 如果可以发送请求，则返回请求超时时间；否则返回 Long.MAX_VALUE
         */
        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            // 如果可以发送请求，则将元数据请求添加到发送列表中
            if (canSendRequest(nodeConnectionId, now)) {
                // 创建元数据请求
                Metadata.MetadataRequestAndVersion requestAndVersion = metadata.newMetadataRequestAndVersion(now);
                MetadataRequest.Builder metadataRequest = requestAndVersion.requestBuilder;
                log.debug("Sending metadata request {} to node {}", metadataRequest, node);
                // 发送元数据请求
                sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now);
                // 设置正在进行的请求数据
                inProgress = new InProgressData(requestAndVersion.requestVersion, requestAndVersion.isPartialUpdate);
                return defaultRequestTimeoutMs;
            }

            // If there's any connection establishment underway, wait until it completes. This prevents
            // the client from unnecessarily connecting to additional nodes while a previous connection
            // attempt has not been completed.
            // 如果任何节点正在连接，则等待直到连接完成。这可以防止客户端在不必要地连接到额外的节点时，连接到多个节点。
            if (isAnyNodeConnecting()) {
                // Strictly the timeout we should return here is "connect timeout", but as we don't
                // have such application level configuration, using reconnect backoff instead.
                // 由于我们没有这样的应用程序级配置，因此使用重连退避时间。
                return reconnectBackoffMs;
            }

            // 如果可以连接到节点，则初始化连接
            if (connectionStates.canConnect(nodeConnectionId, now)) {
                // We don't have a connection to this node right now, make one
                // 我们当前没有到这个节点的连接，现在建立一个
                log.debug("Initialize connection to node {} for sending metadata request", node);
                initiateConnect(node, now);
                // 返回重连退避时间
                return reconnectBackoffMs;
            }

            // connected, but can't send more OR connecting
            // In either case, we just need to wait for a network event to let us know the selected
            // connection might be usable again.
            return Long.MAX_VALUE;
        }

        public class InProgressData {
            public final int requestVersion;
            public final boolean isPartialUpdate;

            private InProgressData(int requestVersion, boolean isPartialUpdate) {
                this.requestVersion = requestVersion;
                this.isPartialUpdate = isPartialUpdate;
            }
        }

    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, defaultRequestTimeoutMs, null);
    }

    // visible for testing
    int nextCorrelationId() {
        if (SaslClientAuthenticator.isReserved(correlation)) {
            // the numeric overflow is fine as negative values is acceptable
            correlation = SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + 1;
        }
        return correlation++;
    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse,
                                          int requestTimeoutMs,
                                          RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, nextCorrelationId(), clientId, createdTimeMs, expectResponse,
                requestTimeoutMs, callback);
    }

    public boolean discoverBrokerVersions() {
        return discoverBrokerVersions;
    }

    static class InFlightRequest {
        final RequestHeader header;
        final String destination;
        final RequestCompletionHandler callback;
        final boolean expectResponse;
        final AbstractRequest request;
        final boolean isInternalRequest; // used to flag requests which are initiated internally by NetworkClient
        final Send send;
        final long sendTimeMs;
        final long createdTimeMs;
        final long requestTimeoutMs;

        public InFlightRequest(ClientRequest clientRequest,
                               RequestHeader header,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this(header,
                 clientRequest.requestTimeoutMs(),
                 clientRequest.createdTimeMs(),
                 clientRequest.destination(),
                 clientRequest.callback(),
                 clientRequest.expectResponse(),
                 isInternalRequest,
                 request,
                 send,
                 sendTimeMs);
        }

        public InFlightRequest(RequestHeader header,
                               int requestTimeoutMs,
                               long createdTimeMs,
                               String destination,
                               RequestCompletionHandler callback,
                               boolean expectResponse,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this.header = header;
            this.requestTimeoutMs = requestTimeoutMs;
            this.createdTimeMs = createdTimeMs;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.isInternalRequest = isInternalRequest;
            this.request = request;
            this.send = send;
            this.sendTimeMs = sendTimeMs;
        }

        public ClientResponse completed(AbstractResponse response, long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    false, null, null, response);
        }

        public ClientResponse disconnected(long timeMs, AuthenticationException authenticationException) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, null, authenticationException, null);
        }

        @Override
        public String toString() {
            return "InFlightRequest(header=" + header +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", createdTimeMs=" + createdTimeMs +
                    ", sendTimeMs=" + sendTimeMs +
                    ", isInternalRequest=" + isInternalRequest +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", send=" + send + ")";
        }
    }

}
