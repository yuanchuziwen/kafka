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
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * A thread-safe helper class to hold batches that haven't been acknowledged yet (including those
 * which have and have not been sent).
 * 一个线程安全的辅助类，用于保存尚未被确认的批次（包括已发送和未发送的批次）。
 */
class IncompleteBatches {
    // 内部是一个普通的 HashSet
    // 但是所有的操作都是在 synchronized 块中进行的
    private final Set<ProducerBatch> incomplete;

    public IncompleteBatches() {
        this.incomplete = new HashSet<>();
    }

    // RecordAccumulator#append() 方法中新建 ProducerBatch 对象后，会调用此方法将其添加到 incomplete 集合中
    // 或者将一个太大的 batch 进行 split 后，也会调用此方法将新的 batch 添加到 incomplete 集合中
    public void add(ProducerBatch batch) {
        synchronized (incomplete) {
            this.incomplete.add(batch);
        }
    }

    // 当 batch 需要被 deallocate 时，会调用此方法将其从 incomplete 集合中移除
    // 能被 deallocate 的 batch 有三种情况：
    // 1. batch 已经被成功发送到 broker，且 broker 已经返回了 ack（然后 batch 被调用了 complete 方法）
    // 2. batch 过大，需要被 split
    // 3. batch 被人为的 abort 了
    public void remove(ProducerBatch batch) {
        synchronized (incomplete) {
            boolean removed = this.incomplete.remove(batch);
            if (!removed)
                throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
        }
    }

    public Iterable<ProducerBatch> copyAll() {
        synchronized (incomplete) {
            return new ArrayList<>(this.incomplete);
        }
    }

    public Iterable<ProduceRequestResult> requestResults() {
        synchronized (incomplete) {
            return incomplete.stream().map(batch -> batch.produceFuture).collect(Collectors.toList());
        }
    }

    public boolean isEmpty() {
        synchronized (incomplete) {
            return incomplete.isEmpty();
        }
    }
}
