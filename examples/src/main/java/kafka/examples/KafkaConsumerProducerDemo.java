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
package kafka.examples;

import org.apache.kafka.common.errors.TimeoutException;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerProducerDemo {
    public static void main(String[] args) throws InterruptedException {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        // 会将 latch 传入 Producer 和 Consumer，Producer 和 Consumer 会在完成后调用 latch.countDown()
        // 然后主线程会 wait，一直到 latch 的 count 为 0；即 Producer 和 Consumer 都完成后，主线程才会继续执行
        CountDownLatch latch = new CountDownLatch(2);
        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync, null, false, 10000, -1, latch);
        producerThread.start();

        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC, "DemoConsumer", Optional.empty(), false, 10000, latch);
        consumerThread.start();

        // 阻塞等待 producer 和 consumer 的完成（调用 countdown），或者等到超时 5 min
        if (!latch.await(5, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for demo producer and consumer to finish");
        }

        consumerThread.shutdown();
        System.out.println("All finished!");
    }
}
