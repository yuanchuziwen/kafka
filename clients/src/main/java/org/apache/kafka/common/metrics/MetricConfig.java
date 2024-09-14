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
package org.apache.kafka.common.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 指标配置的配置值
 */
public class MetricConfig {

    private Quota quota; // 配额
    private int samples; // 样本数
    private long eventWindow; // 事件窗口
    private long timeWindowMs; // 时间窗口（毫秒）
    private Map<String, String> tags; // 标签
    private Sensor.RecordingLevel recordingLevel; // 记录级别

    /**
     * 构造函数，初始化默认值
     */
    public MetricConfig() {
        this.quota = null;
        this.samples = 2;
        this.eventWindow = Long.MAX_VALUE;
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
        this.tags = new LinkedHashMap<>();
        this.recordingLevel = Sensor.RecordingLevel.INFO;
    }

    /**
     * 获取配额
     */
    public Quota quota() {
        return this.quota;
    }

    /**
     * 设置配额
     */
    public MetricConfig quota(Quota quota) {
        this.quota = quota;
        return this;
    }

    /**
     * 获取事件窗口
     */
    public long eventWindow() {
        return eventWindow;
    }

    /**
     * 设置事件窗口
     */
    public MetricConfig eventWindow(long window) {
        this.eventWindow = window;
        return this;
    }

    /**
     * 获取时间窗口（毫秒）
     */
    public long timeWindowMs() {
        return timeWindowMs;
    }

    /**
     * 设置时间窗口
     */
    public MetricConfig timeWindow(long window, TimeUnit unit) {
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(window, unit);
        return this;
    }

    /**
     * 获取标签
     */
    public Map<String, String> tags() {
        return this.tags;
    }

    /**
     * 设置标签
     */
    public MetricConfig tags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    /**
     * 获取样本数
     */
    public int samples() {
        return this.samples;
    }

    /**
     * 设置样本数
     */
    public MetricConfig samples(int samples) {
        if (samples < 1)
            throw new IllegalArgumentException("The number of samples must be at least 1.");
        this.samples = samples;
        return this;
    }

    /**
     * 获取记录级别
     */
    public Sensor.RecordingLevel recordLevel() {
        return this.recordingLevel;
    }

    /**
     * 设置记录级别
     */
    public MetricConfig recordLevel(Sensor.RecordingLevel recordingLevel) {
        this.recordingLevel = recordingLevel;
        return this;
    }

}
