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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * The interface used by `NetworkClient` to request cluster metadata info to be updated and to retrieve the cluster nodes
 * from such metadata. This is an internal class.
 * <p>
 * 该接口用于 `NetworkClient` 请求更新集群元数据信息并从这些元数据中检索集群节点。这是一个内部类。
 * 
 * This class is not thread-safe!
 * 这个类不是线程安全的！
 */
public interface MetadataUpdater extends Closeable {

    /**
     * Gets the current cluster info without blocking.
     * 获取当前集群信息而不阻塞。
     */
    List<Node> fetchNodes();

    /**
     * Returns true if an update to the cluster metadata info is due.
     * 如果集群元数据信息需要更新，则返回 true。
     */
    boolean isUpdateDue(long now);

    /**
     * Starts a cluster metadata update if needed and possible. Returns the time until the metadata update (which would
     * be 0 if an update has been started as a result of this call).
     * 
     * 如果需要且可能，启动集群元数据更新。返回元数据更新的时间（如果由于此调用而启动了更新，则为 0）。
     *
     * If the implementation relies on `NetworkClient` to send requests, `handleSuccessfulResponse` will be
     * invoked after the metadata response is received.
     * 
     * 如果实现依赖于 `NetworkClient` 发送请求，则在收到元数据响应后将调用 `handleSuccessfulResponse`。
     *
     * The semantics of `needed` and `possible` are implementation-dependent and may take into account a number of
     * factors like node availability, how long since the last metadata update, etc.
     * 
     * `needed` 和 `possible` 的语义取决于实现，可能会考虑许多因素，如节点可用性、自上次元数据更新以来的时间等。
     */
    long maybeUpdate(long now);

    /**
     * Handle a server disconnect.
     * 处理服务器断开连接。
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for disconnections of such requests.
     * 
     * 这为 `MetadataUpdater` 实现提供了一种机制，以便使用 NetworkClient 实例进行自己的请求，并对这些请求的断开连接进行特殊处理。
     *
     * @param now Current time in milliseconds
     *            当前时间（毫秒）
     * @param nodeId The id of the node that disconnected
     *               断开连接的节点的 ID
     * @param maybeAuthException Optional authentication error
     *                           可选的身份验证错误
     */
    void handleServerDisconnect(long now, String nodeId, Optional<AuthenticationException> maybeAuthException);

    /**
     * Handle a metadata request failure.
     * 处理元数据请求失败。
     *
     * @param now Current time in milliseconds
     *            当前时间（毫秒）
     * @param maybeFatalException Optional fatal error (e.g. {@link UnsupportedVersionException})
     *                            可选的致命错误（例如 {@link UnsupportedVersionException}）
     */
    void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException);

    /**
     * Handle responses for metadata requests.
     * 处理元数据请求的响应。
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for completed receives of such requests.
     * 
     * 这为 `MetadataUpdater` 实现提供了一种机制，以便使用 NetworkClient 实例进行自己的请求，并对这些请求的完成接收进行特殊处理。
     */
    void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse);

    /**
     * Close this updater.
     * 关闭此更新器。
     */
    @Override
    void close();
}
