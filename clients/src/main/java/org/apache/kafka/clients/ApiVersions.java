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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Maintains node api versions for access outside of NetworkClient (which is where the information is derived).
 * The pattern is akin to the use of {@link Metadata} for topic metadata.
 * 维护节点 API 版本信息，用于在 NetworkClient 之外访问。
 * 该类的设计意图仅限于在 Kafka 内部使用。
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 * 注意：此类仅限于在 Kafka 内部使用。
 */
public class ApiVersions {

    /**
     * 存储节点ID和对应的API版本信息
     */
    private final Map<String, NodeApiVersions> nodeApiVersions = new HashMap<>();

    /**
     * 当前可用的最大生产者魔数
     * 目前是 v2
     */
    private byte maxUsableProduceMagic = RecordBatch.CURRENT_MAGIC_VALUE;

    /**
     * 更新指定节点的API版本信息
     * @param nodeId 节点ID
     * @param nodeApiVersions 节点API版本信息
     */
    public synchronized void update(String nodeId, NodeApiVersions nodeApiVersions) {
        this.nodeApiVersions.put(nodeId, nodeApiVersions);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    /**
     * 移除指定节点的API版本信息
     * @param nodeId 要移除的节点ID
     */
    public synchronized void remove(String nodeId) {
        this.nodeApiVersions.remove(nodeId);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    /**
     * 获取指定节点的API版本信息
     * @param nodeId 节点ID
     * @return 节点的API版本信息
     */
    public synchronized NodeApiVersions get(String nodeId) {
        return this.nodeApiVersions.get(nodeId);
    }

    /**
     * 计算当前可用的最大生产者魔数
     * @return 计算得到的最大生产者魔数
     */
    private byte computeMaxUsableProduceMagic() {
        // use a magic version which is supported by all brokers to reduce the chance that
        // we will need to convert the messages when they are ready to be sent.
        Optional<Byte> knownBrokerNodesMinRequiredMagicForProduce = this.nodeApiVersions.values().stream()
            .filter(versions -> versions.apiVersion(ApiKeys.PRODUCE) != null) // filter out Raft controller nodes
            .map(versions -> ProduceRequest.requiredMagicForVersion(versions.latestUsableVersion(ApiKeys.PRODUCE)))
            .min(Byte::compare);
        return (byte) Math.min(RecordBatch.CURRENT_MAGIC_VALUE,
            knownBrokerNodesMinRequiredMagicForProduce.orElse(RecordBatch.CURRENT_MAGIC_VALUE));
    }

    /**
     * 获取当前可用的最大生产者魔数
     * @return 当前可用的最大生产者魔数
     */
    public synchronized byte maxUsableProduceMagic() {
        return maxUsableProduceMagic;
    }

}
