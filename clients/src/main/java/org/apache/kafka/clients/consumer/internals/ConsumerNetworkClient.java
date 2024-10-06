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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Higher level consumer access to the network layer with basic support for request futures. This class
 * is thread-safe, but provides no synchronization for response callbacks. This guarantees that no locks
 * are held when they are invoked.
 * <p>
 * 高级消费者访问网络层，具有基本的支持请求未来的功能。
 * 这个类是线程安全的，但不会为响应回调提供同步。
 * 这保证了在调用时不会持有锁。
 */
public class ConsumerNetworkClient implements Closeable {
    // 最大轮询超时时间
    private static final int MAX_POLL_TIMEOUT_MS = 5000;

    // the mutable state of this class is protected by the object's monitor (excluding the wakeup
    // flag and the request completion queue below).
    // 该类的可变状态受对象的监视器保护（不包括下面的唤醒标志和请求完成队列）。

    // 记录日志的 Logger 实例
    private final Logger log;
    // Kafka 客户端实例
    private final KafkaClient client;
    // 未发送请求的集合，Map<Node, List<ClientRequest>>，用于存储未发送的请求
    private final UnsentRequests unsent = new UnsentRequests();
    // 元数据实例
    private final Metadata metadata;
    // 时间工具类实例
    private final Time time;
    // 重试退避时间
    private final long retryBackoffMs;
    // 最大轮询超时时间
    private final int maxPollTimeoutMs;
    // 请求超时时间
    private final int requestTimeoutMs;
    // 用于禁用唤醒的标志
    private final AtomicBoolean wakeupDisabled = new AtomicBoolean();

    // We do not need high throughput, so use a fair lock to try to avoid starvation
    // 使用公平锁以避免饥饿
    private final ReentrantLock lock = new ReentrantLock(true);

    // when requests complete, they are transferred to this queue prior to invocation. The purpose
    // is to avoid invoking them while holding this object's monitor which can open the door for deadlocks.
    // 请求完成后，将它们转移到此队列中，以避免在持有对象监视器时调用它们，从而避免死锁
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();

    // 挂起的断开连接队列
    private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

    // this flag allows the client to be safely woken up without waiting on the lock above. It is
    // atomic to avoid the need to acquire the lock above in order to enable it concurrently.
    // 允许客户端在不等待上述锁的情况下安全唤醒的标志
    private final AtomicBoolean wakeup = new AtomicBoolean(false);

    public ConsumerNetworkClient(LogContext logContext,
                                 KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 int requestTimeoutMs,
                                 int maxPollTimeoutMs) {
        this.log = logContext.logger(ConsumerNetworkClient.class);
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.maxPollTimeoutMs = Math.min(maxPollTimeoutMs, MAX_POLL_TIMEOUT_MS);
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int defaultRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    /**
     * Send a request with the default timeout. See {@link #send(Node, AbstractRequest.Builder, int)}.
     * <p>
     *     使用默认超时时间发送请求。
     */
    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        return send(node, requestBuilder, requestTimeoutMs);
    }

    /**
     * Send a new request. Note that the request is not actually transmitted on the
     * network until one of the {@link #poll(Timer)} variants is invoked. At this
     * point the request will either be transmitted successfully or will fail.
     * Use the returned future to obtain the result of the send. Note that there is no
     * need to check for disconnects explicitly on the {@link ClientResponse} object;
     * instead, the future will be failed with a {@link DisconnectException}.
     * <p>
     *     发送一个新的请求。
     * 注意，请求实际上不会在网络上发送，直到调用其中一个 {@link #poll(Timer)} 变体。
     * 此时，请求将要么成功发送，要么失败。
     * 使用返回的 future 获取发送结果。
     * 注意，不需要在 {@link ClientResponse} 对象上显式检查断开连接；
     * 相反，future 将使用 {@link DisconnectException} 失败。
     *
     * @param node The destination of the request
     * @param requestBuilder A builder for the request payload
     * @param requestTimeoutMs Maximum time in milliseconds to await a response before disconnecting the socket and
     *                         cancelling the request. The request may be cancelled sooner if the socket disconnects
     *                         for any reason.
     * @return A future which indicates the result of the send.
     */
    public RequestFuture<ClientResponse> send(Node node,
                                              AbstractRequest.Builder<?> requestBuilder,
                                              int requestTimeoutMs) {
        long now = time.milliseconds();
        // 创建一个 RequestFutureCompletionHandler 实例
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        // 创建一个 ClientRequest 实例
        ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,
            requestTimeoutMs, completionHandler);
        // 将请求添加到未发送请求集合中
        unsent.put(node, clientRequest);

        // wakeup the client in case it is blocking in poll so that we can send the queued request
        // 唤醒 client，以便我们可以发送排队的请求
        client.wakeup();
        // 返回请求的 future
        return completionHandler.future;
    }

    public Node leastLoadedNode() {
        lock.lock();
        try {
            // 获取最小的负载节点
            return client.leastLoadedNode(time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    public boolean hasReadyNodes(long now) {
        lock.lock();
        try {
            // 检查是否有可用的节点
            return client.hasReadyNodes(now);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block waiting on the metadata refresh with a timeout.
     * <p>
     *     阻塞等待元数据刷新，直到超时。
     *
     * @return true if update succeeded, false otherwise.
     */
    public boolean awaitMetadataUpdate(Timer timer) {
        // 请求更新元数据
        int version = this.metadata.requestUpdate();
        // 持续轮询，直到元数据更新完成或超时
        do {
            // 轮询
            poll(timer);
        } while (this.metadata.updateVersion() == version && timer.notExpired());
        return this.metadata.updateVersion() > version;
    }

    /**
     * Ensure our metadata is fresh (if an update is expected, this will block
     * until it has completed).
     * <p>
     *     确保我们的元数据是最新的（如果需要更新，这将阻塞直到它完成）。
     *
     * @param timer 计时器
     * @return 如果元数据已更新，则返回 true，否则返回 false
     */
    boolean ensureFreshMetadata(Timer timer) {
        // 如果元数据需要更新，或者下一次更新时间等于 0，则等待元数据更新
        if (this.metadata.updateRequested() || 
                this.metadata.timeToNextUpdate(timer.currentTimeMs()) == 0) {
            return awaitMetadataUpdate(timer);
        } else {
            // the metadata is already fresh
            // 元数据已经是最新的
            return true;
        }
    }

    /**
     * Wakeup an active poll. This will cause the polling thread to throw an exception either
     * on the current poll if one is active, or the next poll.
     * <p>
     *     唤醒一个活跃的轮询。
     *     这将导致轮询线程在当前轮询中抛出异常，或者在下一个轮询中抛出异常。
     */
    public void wakeup() {
        // wakeup should be safe without holding the client lock since it simply delegates to
        // Selector's wakeup, which is thread-safe
        // 唤醒应该是线程安全的，因为它只是委托给 Selector 的唤醒，而 Selector 是线程安全的
        log.debug("Received user wakeup");
        // 设置唤醒标志
        this.wakeup.set(true);
        // 唤醒 Kafka 客户端
        this.client.wakeup();
    }

    /**
     * Block indefinitely until the given request future has finished.
     * <p>
     *     阻塞直到给定的请求 future 完成。
     * 
     * @param future The request future to await.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(RequestFuture<?> future) {
        // 持续轮询，直到 future 完成
        while (!future.isDone())
            poll(time.timer(Long.MAX_VALUE), future);
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     * <p>
     *     阻塞直到提供的请求 future 请求完成或超时过期。
     *
     * @param future The request future to wait for
     * @param timer Timer bounding how long this method can block
     * @return true if the future is done, false otherwise
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public boolean poll(RequestFuture<?> future, Timer timer) {
        do {
            poll(timer, future);
            // 如果 future 没有完成，并且 timer 没有过期，则继续轮询
        } while (!future.isDone() && timer.notExpired());
        return future.isDone();
    }

    /**
     * Poll for any network IO.
     * <p>
     *     轮询网络 IO。
     * 
     * @param timer Timer bounding how long this method can block
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(Timer timer) {
        poll(timer, null);
    }

    /**
     * Poll for any network IO.
     * <p>
     *     轮询网络 IO。
     * 
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition
     *                      可空的阻塞条件
     */
    public void poll(Timer timer, PollCondition pollCondition) {
        poll(timer, pollCondition, false);
    }

    /**
     * Poll for any network IO.
     * <p>
     *     轮询网络 IO。
     * 
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition
     * @param disableWakeup If TRUE disable triggering wake-ups
     */
    public void poll(Timer timer, PollCondition pollCondition, boolean disableWakeup) {
        // there may be handlers which need to be invoked if we woke up the previous call to poll
        // 可能有一些处理程序需要在我们上一次调用 poll 时调用
        firePendingCompletedRequests();

        lock.lock();
        try {
            // Handle async disconnects prior to attempting any sends
            // 在尝试发送任何请求之前处理异步断开连接
            handlePendingDisconnects();

            // send all the requests we can send now
            // 尝试发送所有可以发送的请求，即循环处理 unsent 中缓存的请求
            long pollDelayMs = trySend(timer.currentTimeMs());

            // check whether the poll is still needed by the caller. Note that if the expected completion
            // condition becomes satisfied after the call to shouldBlock() (because of a fired completion
            // handler), the client will be woken up.
            // 检查是否仍然需要轮询。
            // 如果预期的完成条件在调用 shouldBlock() 之后变得满足（因为完成处理程序已触发），
            // 则客户端将被唤醒。
            if (pendingCompletion.isEmpty() && // pendingCompletion 队列为空，即所有待完成的请求已完成
                    (pollCondition == null || // 没有 pollCondition，即不需要阻塞
                            pollCondition.shouldBlock())) { // 有 pollCondition，且需要阻塞

                // 计算 poll 的超时时间
                long pollTimeout = Math.min(timer.remainingMs(), pollDelayMs);
                // if there are no requests in flight, do not block longer than the retry backoff
                // 如果没有正在进行的请求，则不要阻塞超过重试回退时间
                if (client.inFlightRequestCount() == 0)
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);
                // 调用 networkClient 的 poll 方法，进行实际的读写操作
                client.poll(pollTimeout, timer.currentTimeMs());
            } else {
                // 轮询
                client.poll(0, timer.currentTimeMs());
            }
            timer.update();

            // handle any disconnects by failing the active requests. note that disconnects must
            // be checked immediately following poll since any subsequent call to client.ready()
            // will reset the disconnect status
            // 通过检查连接断开情况来失败活动的请求。
            // 注意，断开连接必须在轮询后立即检查，因为任何后续对 client.ready() 的调用都会重置断开连接状态
            checkDisconnects(timer.currentTimeMs());
            // 如果没有禁用唤醒，则触发唤醒的检查
            if (!disableWakeup) {
                // trigger wakeups after checking for disconnects so that the callbacks will be ready
                // to be fired on the next call to poll()
                // 在检查断开连接后触发唤醒，以便在下次调用 poll() 时准备调用回调
                maybeTriggerWakeup();
            }
            // throw InterruptException if this thread is interrupted
            // 抛出 InterruptException 如果这个线程被中断
            maybeThrowInterruptException();

            // try again to send requests since buffer space may have been
            // cleared or a connect finished in the poll
            // 尝试再次发送请求，因为 poll 中可能已清除缓冲区空间或连接已完成
            trySend(timer.currentTimeMs());

            // fail requests that couldn't be sent if they have expired
            // 处理 unsent 中超时的请求
            failExpiredRequests(timer.currentTimeMs());

            // clean unsent requests collection to keep the map from growing indefinitely
            // 清理 unsent 请求集合，以防止 map 无限增长
            unsent.clean();
        } finally {
            lock.unlock();
        }

        // called without the lock to avoid deadlock potential if handlers need to acquire locks
        firePendingCompletedRequests();

        metadata.maybeThrowAnyException();
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups.
     * <p>
     *     轮询网络 IO 并立即返回。这不会触发唤醒。
     */
    public void pollNoWakeup() {
        poll(time.timer(0), null, true);
    }

    /**
     * Poll for network IO in best-effort only trying to transmit the ready-to-send request
     * Do not check any pending requests or metadata errors so that no exception should ever
     * be thrown, also no wakeups be triggered and no interrupted exception either.
     * <p>
     *     仅尝试传输准备发送的请求，不检查任何挂起的请求或元数据错误，
     * 因此不会抛出任何异常，也不会触发唤醒，也不会抛出中断异常。
     */
    public void transmitSends() {
        Timer timer = time.timer(0);

        // do not try to handle any disconnects, prev request failures, metadata exception etc;
        // just try once and return immediately
        // 不处理任何断开连接、先前请求失败、元数据异常等；
        // 只尝试一次并立即返回
        lock.lock();
        try {
            // send all the requests we can send now
            // 尝试发送所有可以发送的请求
            trySend(timer.currentTimeMs());

            // 立即返回，不等待
            client.poll(0, timer.currentTimeMs());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block until all pending requests from the given node have finished.
     * <p>
     *     阻塞直到给定节点的所有挂起请求完成。
     *     由 coordinator 调用
     *
     * @param node The node to await requests from
     * @param timer Timer bounding how long this method can block
     * @return true If all requests finished, false if the timeout expired first
     */
    public boolean awaitPendingRequests(Node node, Timer timer) {
        // 持续轮询，直到给定节点的所有挂起请求完成
        while (hasPendingRequests(node) && timer.notExpired()) {
            poll(timer);
        }
        return !hasPendingRequests(node);
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * <p>
     *     获取给定节点的挂起请求计数。
     *     这包括已经发送的请求（即正在进行的请求）和等待发送的请求。
     *
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        lock.lock();
        try {
            return unsent.requestCount(node) + client.inFlightRequestCount(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * <p>
     *     检查是否有挂起的请求到给定的节点。
     *     这包括已经发送的请求（即正在进行的请求）和等待发送的请求。
     *
     * @param node The node in question
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests(Node node) {
        // 检查 unsent 中是否有给定节点的请求
        if (unsent.hasRequests(node))
            return true;
        lock.lock();
        try {
            // 检查 networkClient 中是否有给定节点的 in-flight 请求
            return client.hasInFlightRequests(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * <p>
     *     获取所有节点的挂起请求总数。
     *     这包括已经发送的请求（即正在进行的请求）和等待发送的请求。
     *
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        lock.lock();
        try {
            return unsent.requestCount() + client.inFlightRequestCount();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * <p>
     *     检查是否有挂起的请求。这包括已经发送的请求（即正在进行的请求）和等待发送的请求。
     *
     * @return 一个布尔值，表示是否有挂起的请求
     */
    public boolean hasPendingRequests() {
        // 检查是否有挂起的请求
        if (unsent.hasRequests())
            return true;

        // 检查是否有正在进行的请求
        lock.lock();
        try {
            return client.hasInFlightRequests();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 触发所有待完成的请求。
     */
    private void firePendingCompletedRequests() {
        boolean completedRequestsFired = false;
        // 遍历 pendingCompletion 队列
        for (;;) {
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null)
                break;

            // 触发每个 completionHandler 的完成
            completionHandler.fireCompletion();
            completedRequestsFired = true;
        }

        // wakeup the client in case it is blocking in poll for this future's completion
        // 如果 future 完成，则唤醒 client
        if (completedRequestsFired)
            client.wakeup();
    }

    private void checkDisconnects(long now) {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        // 任何影响已经发送的请求的断开连接都将由 NetworkClient 处理，
        // 因此我们只需要检查是否已断开未发送请求的任何连接；
        // 如果是，则我们完成相应的 future，并在 ClientResponse 中设置断开标志

        // 遍历每个需要发送的请求相关的节点
        for (Node node : unsent.nodes()) {
            // 检查当前 client 和 node 之间的连接是否失败
            if (client.connectionFailed(node)) {
                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
                // 在调用请求回调之前删除条目，以避免回调处理协调器失败再次遍历未发送列表
                Collection<ClientRequest> requests = unsent.remove(node);
                for (ClientRequest request : requests) {
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    AuthenticationException authenticationException = client.authenticationException(node);
                    handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
                            request.callback(), request.destination(), request.createdTimeMs(), now, true,
                            null, authenticationException, null));
                }
            }
        }
    }

    private void handlePendingDisconnects() {
        lock.lock();
        try {
            // 遍历 pendingDisconnects 队列
            while (true) {
                // 取出需要断开连接的 node
                Node node = pendingDisconnects.poll();
                // 如果节点为空，则退出循环
                if (node == null)
                    break;

                // 处理与这个 node 关联的 unsent 的请求
                failUnsentRequests(node, DisconnectException.INSTANCE);
                // 断开与节点的连接
                client.disconnect(node.idString());
            }
        } finally {
            lock.unlock();
        }
    }

    public void disconnectAsync(Node node) {
        // 将节点添加到 pendingDisconnects 队列中
        pendingDisconnects.offer(node);
        // 唤醒 client 线程
        client.wakeup();
    }

    private void failExpiredRequests(long now) {
        // clear all expired unsent requests and fail their corresponding futures
        // 清除所有过期的未发送请求，并失触发这些请求的 onFailure 方法
        Collection<ClientRequest> expiredRequests = unsent.removeExpiredRequests(now);
        for (ClientRequest request : expiredRequests) {
            RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
            handler.onFailure(new TimeoutException("Failed to send request after " + request.requestTimeoutMs() + " ms."));
        }
    }

    private void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
        // 清除未发送到节点的请求，并失败它们对应的 futures
        lock.lock();
        try {
            // 获取该 node 下的所有未发送的请求
            Collection<ClientRequest> unsentRequests = unsent.remove(node);
            // 遍历每个未发送的请求
            for (ClientRequest unsentRequest : unsentRequests) {
                // 触发 onFailure 方法
                RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) unsentRequest.callback();
                handler.onFailure(e);
            }
        } finally {
            lock.unlock();
        }
    }

    // Visible for testing
    long trySend(long now) {
        long pollDelayMs = maxPollTimeoutMs;

        // send any requests that can be sent now
        // 遍历每个 node
        for (Node node : unsent.nodes()) {
            // 获取这个 node 下的所有未发送的请求
            Iterator<ClientRequest> iterator = unsent.requestIterator(node);
            if (iterator.hasNext())
                pollDelayMs = Math.min(pollDelayMs, client.pollDelayMs(node, now));

            // 遍历每个 ClientRequest 请求
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                // 调用 ready 方法判断 client 和 node 之间的连接是否可用
                if (client.ready(node, now)) {
                    // 如果连接可用，则调用 send 方法将请求放入 InFlightRequests 集合中（也放入 KafkaChannel 的 send 字段中等待发送）
                    client.send(request, now);
                    // 将此消息从列表中删除
                    iterator.remove();
                } else {
                    // try next node when current node is not ready
                    // 如果当前 channel 不可用，则 break 对 requests 的遍历，然后尝试下一个 node
                    break;
                }
            }
        }
        return pollDelayMs;
    }

    public void maybeTriggerWakeup() {
        // 如果没有禁用唤醒，并且 wakeup 标志为 true，则抛出 WakeupException
        if (!wakeupDisabled.get() && wakeup.get()) {
            log.debug("Raising WakeupException in response to user wakeup");
            wakeup.set(false);
            throw new WakeupException();
        }
    }

    private void maybeThrowInterruptException() {
        if (Thread.interrupted()) {
            throw new InterruptException(new InterruptedException());
        }
    }

    public void disableWakeups() {
        wakeupDisabled.set(true);
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            client.close();
        } finally {
            lock.unlock();
        }
    }


    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     * <p>
     *     检查代码是否断开连接并不可用于立即重新连接（即如果在断开连接后的重新连接回退窗口中）。
     */
    public boolean isUnavailable(Node node) {
        lock.lock();
        try {
            return client.connectionFailed(node) && // client 和 node 之间的连接失败
                    client.connectionDelay(node, time.milliseconds()) > 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check for an authentication error on a given node and raise the exception if there is one.
     */
    public void maybeThrowAuthFailure(Node node) {
        lock.lock();
        try {
            AuthenticationException exception = client.authenticationException(node);
            if (exception != null)
                throw exception;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the failed
     * status of a socket. If there is an actual request to send, then {@link #send(Node, AbstractRequest.Builder)}
     * should be used.
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        lock.lock();
        try {
            client.ready(node, time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    private class RequestFutureCompletionHandler implements RequestCompletionHandler {
        private final RequestFuture<ClientResponse> future;
        private ClientResponse response;
        private RuntimeException e;

        private RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        public void fireCompletion() {
            if (e != null) {
                future.raise(e);
            } else if (response.authenticationException() != null) {
                future.raise(response.authenticationException());
            } else if (response.wasDisconnected()) {
                log.debug("Cancelled request with header {} due to node {} being disconnected",
                        response.requestHeader(), response.destination());
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                future.raise(response.versionMismatch());
            } else {
                future.complete(response);
            }
        }

        public void onFailure(RuntimeException e) {
            this.e = e;
            pendingCompletion.add(this);
        }

        @Override
        public void onComplete(ClientResponse response) {
            this.response = response;
            pendingCompletion.add(this);
        }
    }

    /**
     * When invoking poll from a multi-threaded environment, it is possible that the condition that
     * the caller is awaiting has already been satisfied prior to the invocation of poll. We therefore
     * introduce this interface to push the condition checking as close as possible to the invocation
     * of poll. In particular, the check will be done while holding the lock used to protect concurrent
     * access to {@link org.apache.kafka.clients.NetworkClient}, which means implementations must be
     * very careful about locking order if the callback must acquire additional locks.
     * <p>
     *     当从多线程环境调用 poll 时，可能存在这样的情况：
     *     调用 poll 之前，调用者正在等待的条件已经满足。
     *     因此，我们引入这个接口，将条件检查尽可能接近 poll 的调用。
     *     特别是，检查将在持有用于保护 {@link org.apache.kafka.clients.NetworkClient} 的锁时进行，
     *     这意味着实现必须非常小心地锁定顺序，如果回调必须获取额外的锁。
     */
    public interface PollCondition {
        /**
         * Return whether the caller is still awaiting an IO event.
         * <p>
         *     返回调用者是否仍在等待 IO 事件。
         * 
         * @return true if so, false otherwise.
         */
        boolean shouldBlock();
    }

    /*
     * A thread-safe helper class to hold requests per node that have not been sent yet
     */
    private static final class UnsentRequests {
        private final ConcurrentMap<Node, ConcurrentLinkedQueue<ClientRequest>> unsent;

        private UnsentRequests() {
            unsent = new ConcurrentHashMap<>();
        }

        public void put(Node node, ClientRequest request) {
            // the lock protects the put from a concurrent removal of the queue for the node
            // 锁保护 put 不会被并发删除节点的队列
            synchronized (unsent) {
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.computeIfAbsent(node, key -> new ConcurrentLinkedQueue<>());
                requests.add(request);
            }
        }

        public int requestCount(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? 0 : requests.size();
        }

        public int requestCount() {
            int total = 0;
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                total += requests.size();
            return total;
        }

        public boolean hasRequests(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests != null && !requests.isEmpty();
        }

        public boolean hasRequests() {
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                if (!requests.isEmpty())
                    return true;
            return false;
        }

        private Collection<ClientRequest> removeExpiredRequests(long now) {
            List<ClientRequest> expiredRequests = new ArrayList<>();
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) {
                Iterator<ClientRequest> requestIterator = requests.iterator();
                while (requestIterator.hasNext()) {
                    ClientRequest request = requestIterator.next();
                    long elapsedMs = Math.max(0, now - request.createdTimeMs());
                    if (elapsedMs > request.requestTimeoutMs()) {
                        expiredRequests.add(request);
                        requestIterator.remove();
                    } else
                        break;
                }
            }
            return expiredRequests;
        }

        public void clean() {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                Iterator<ConcurrentLinkedQueue<ClientRequest>> iterator = unsent.values().iterator();
                while (iterator.hasNext()) {
                    ConcurrentLinkedQueue<ClientRequest> requests = iterator.next();
                    if (requests.isEmpty())
                        iterator.remove();
                }
            }
        }

        public Collection<ClientRequest> remove(Node node) {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.remove(node);
                return requests == null ? Collections.<ClientRequest>emptyList() : requests;
            }
        }

        public Iterator<ClientRequest> requestIterator(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? Collections.<ClientRequest>emptyIterator() : requests.iterator();
        }

        public Collection<Node> nodes() {
            return unsent.keySet();
        }
    }

}
