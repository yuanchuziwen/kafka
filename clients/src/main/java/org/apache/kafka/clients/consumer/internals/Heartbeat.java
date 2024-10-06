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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

/**
 * A helper class for managing the heartbeat to the coordinator
 * <p>
 *     用于管理与 coordinator 之间的心跳的辅助类
 */
public final class Heartbeat {
    private final int maxPollIntervalMs;
    private final GroupRebalanceConfig rebalanceConfig;
    private final Time time;
    private final Timer heartbeatTimer;
    private final Timer sessionTimer;
    private final Timer pollTimer;
    private final Logger log;

    // 上次发送心跳的时间
    private volatile long lastHeartbeatSend = 0L;
    // 标记是否有已发出但未响应的心跳请求
    private volatile boolean heartbeatInFlight = false;

    /**
     * 心跳相关配置的构造函数，只由 AbstractCoordinator 调用
     *
     * @param config
     * @param time
     */
    public Heartbeat(GroupRebalanceConfig config,
                     Time time) {
        // 如果心跳间隔大于会话超时时间，则抛出异常
        if (config.heartbeatIntervalMs >= config.sessionTimeoutMs)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");
        this.rebalanceConfig = config;
        this.time = time;
        this.heartbeatTimer = time.timer(config.heartbeatIntervalMs);
        this.sessionTimer = time.timer(config.sessionTimeoutMs);
        this.maxPollIntervalMs = config.rebalanceTimeoutMs;
        this.pollTimer = time.timer(maxPollIntervalMs);

        final LogContext logContext = new LogContext("[Heartbeat groupID=" + config.groupId + "] ");
        this.log = logContext.logger(getClass());
    }

    private void update(long now) {
        heartbeatTimer.update(now);
        sessionTimer.update(now);
        pollTimer.update(now);
    }

    public void poll(long now) {
        update(now);
        pollTimer.reset(maxPollIntervalMs);
    }

    boolean hasInflight() {
        return heartbeatInFlight;
    }

    // 发出心跳后，更新 Heartbeat 维护的各项数据；方便下一次的判断
    void sentHeartbeat(long now) {
        lastHeartbeatSend = now;
        heartbeatInFlight = true;
        update(now);
        heartbeatTimer.reset(rebalanceConfig.heartbeatIntervalMs);

        if (log.isTraceEnabled()) {
            log.trace("Sending heartbeat request with {}ms remaining on timer", heartbeatTimer.remainingMs());
        }
    }

    // 心跳响应失败后回调
    void failHeartbeat() {
        update(time.milliseconds());
        heartbeatInFlight = false;
        heartbeatTimer.reset(rebalanceConfig.retryBackoffMs);

        log.trace("Heartbeat failed, reset the timer to {}ms remaining", heartbeatTimer.remainingMs());
    }

    // 心跳成功接受到响应后回调
    void receiveHeartbeat() {
        update(time.milliseconds());
        heartbeatInFlight = false;
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs);
    }

    // 判断是否需要发送心跳
    boolean shouldHeartbeat(long now) {
        update(now);
        return heartbeatTimer.isExpired();
    }
    
    long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    long timeToNextHeartbeat(long now) {
        update(now);
        return heartbeatTimer.remainingMs();
    }

    boolean sessionTimeoutExpired(long now) {
        update(now);
        return sessionTimer.isExpired();
    }

    void resetTimeouts() {
        update(time.milliseconds());
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs);
        pollTimer.reset(maxPollIntervalMs);
        heartbeatTimer.reset(rebalanceConfig.heartbeatIntervalMs);
    }

    void resetSessionTimeout() {
        update(time.milliseconds());
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs);
    }

    boolean pollTimeoutExpired(long now) {
        update(now);
        return pollTimer.isExpired();
    }

    long lastPollTime() {
        return pollTimer.currentTimeMs();
    }
}
