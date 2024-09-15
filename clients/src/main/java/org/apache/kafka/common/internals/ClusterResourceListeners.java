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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;

import java.util.ArrayList;
import java.util.List;

public class ClusterResourceListeners {

    // 封装了 ClusterResourceListener 的列表，方便实现触发所有 ClusterResourceListener 的 onUpdate 方法
    private final List<ClusterResourceListener> clusterResourceListeners;

    public ClusterResourceListeners() {
        this.clusterResourceListeners = new ArrayList<>();
    }

    /**
     * Add only if the candidate implements {@link ClusterResourceListener}.
     * 当且仅当 candidate 实现了 ClusterResourceListener 接口时才添加
     *
     * @param candidate Object which might implement {@link ClusterResourceListener}
     */
    public void maybeAdd(Object candidate) {
        if (candidate instanceof ClusterResourceListener) {
            clusterResourceListeners.add((ClusterResourceListener) candidate);
        }
    }

    /**
     * Add all items who implement {@link ClusterResourceListener} from the list.
     * 将列表中实现了 ClusterResourceListener 接口的所有项添加
     *
     * @param candidateList List of objects which might implement {@link ClusterResourceListener}
     */
    public void maybeAddAll(List<?> candidateList) {
        for (Object candidate : candidateList) {
            this.maybeAdd(candidate);
        }
    }

    /**
     * Send the updated cluster metadata to all {@link ClusterResourceListener}.
     * 向所有 ClusterResourceListener 发送更新的集群元数据
     *
     * @param cluster Cluster metadata
     */
    public void onUpdate(ClusterResource cluster) {
        for (ClusterResourceListener clusterResourceListener : clusterResourceListeners) {
            clusterResourceListener.onUpdate(cluster);
        }
    }
}