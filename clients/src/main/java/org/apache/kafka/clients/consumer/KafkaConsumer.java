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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.FetcherMetricsRegistry;
import org.apache.kafka.clients.consumer.internals.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * A client that consumes records from a Kafka cluster.
 * <p>
 * This client transparently handles the failure of Kafka brokers, and transparently adapts as topic partitions
 * it fetches migrate within the cluster. This client also interacts with the broker to allow groups of
 * consumers to load balance consumption using <a href="#consumergroups">consumer groups</a>.
 * <p>
 * The consumer maintains TCP connections to the necessary brokers to fetch data.
 * Failure to close the consumer after use will leak these connections.
 * The consumer is not thread-safe. See <a href="#multithreaded">Multi-threaded Processing</a> for more details.
 *
 * 一个从 Kafka 集群消费记录的客户端。
 * <p>
 * 这个客户端透明地处理 Kafka 代理的故障，并在其获取的主题分区在集群内迁移时透明地适应。
 * 这个客户端还与代理交互，允许消费者组使用<a href="#consumergroups">消费者组</a>来平衡消费。
 * <p>
 * 消费者维护与必要代理的TCP连接以获取数据。
 * 使用后未能关闭消费者将泄漏这些连接。
 * 消费者不是线程安全的。有关更多详细信息，请参阅<a href="#multithreaded">多线程处理</a>。
 *
 * <h3>Cross-Version Compatibility</h3>
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers may not support
 * certain features. For example, 0.10.0 brokers do not support offsetsForTimes, because this feature was added
 * in version 0.10.1. You will receive an {@link org.apache.kafka.common.errors.UnsupportedVersionException}
 * when invoking an API that is not available on the running broker version.
 * <p>
 *
 * <h3>跨版本兼容性</h3>
 * 这个客户端可以与 0.10.0 或更新版本的代理通信。较旧或较新的代理可能不支持某些功能。
 * 例如，0.10.0 代理不支持 offsetsForTimes，因为这个功能是在 0.10.1 版本中添加的。
 * 当调用运行中的代理版本上不可用的 API 时，你将收到一个{@link org.apache.kafka.common.errors.UnsupportedVersionException}。
 * <p>
 *
 * <h3>Offsets and Consumer Position</h3>
 * Kafka maintains a numerical offset for each record in a partition. This offset acts as a unique identifier of
 * a record within that partition, and also denotes the position of the consumer in the partition. For example, a consumer
 * which is at position 5 has consumed records with offsets 0 through 4 and will next receive the record with offset 5. There
 * are actually two notions of position relevant to the user of the consumer:
 * <p>
 * The {@link #position(TopicPartition) position} of the consumer gives the offset of the next record that will be given
 * out. It will be one larger than the highest offset the consumer has seen in that partition. It automatically advances
 * every time the consumer receives messages in a call to {@link #poll(Duration)}.
 * <p>
 * The {@link #commitSync() committed position} is the last offset that has been stored securely. Should the
 * process fail and restart, this is the offset that the consumer will recover to. The consumer can either automatically commit
 * offsets periodically; or it can choose to control this committed position manually by calling one of the commit APIs
 * (e.g. {@link #commitSync() commitSync} and {@link #commitAsync(OffsetCommitCallback) commitAsync}).
 * <p>
 * This distinction gives the consumer control over when a record is considered consumed. It is discussed in further
 * detail below.
 *
 * <h3>偏移量和消费者位置</h3>
 * Kafka 为分区中的每条记录维护一个数字偏移量。这个偏移量作为该分区内记录的唯一标识符，
 * 同时也表示消费者在分区中的位置。例如，位于位置 5 的消费者已经消费了偏移量 0 到 4 的记录，
 * 下一个将接收偏移量为 5 的记录。对消费者用户来说，实际上有两个相关的位置概念：
 * <p>
 * 消费者的{@link #position(TopicPartition) 位置}给出了下一条将被给出的记录的偏移量。
 * 它将比消费者在该分区中看到的最高偏移量大一。每次消费者在调用{@link #poll(Duration)}时接收消息，它都会自动前进。
 * <p>
 * {@link #commitSync() 已提交位置}是最后一个已安全存储的偏移量。如果进程失败并重启，
 * 这是消费者将恢复到的偏移量。消费者可以选择定期自动提交偏移量；
 * 或者可以选择通过调用其中一个提交 API（例如{@link #commitSync() commitSync}和
 * {@link #commitAsync(OffsetCommitCallback) commitAsync}）来手动控制这个已提交位置。
 * <p>
 * 这种区别使消费者可以控制何时认为记录已被消费。下面将进一步详细讨论这一点。
 *
 * <h3><a name="consumergroups">Consumer Groups and Topic Subscriptions</a></h3>
 *
 * Kafka uses the concept of <i>consumer groups</i> to allow a pool of processes to divide the work of consuming and
 * processing records. These processes can either be running on the same machine or they can be
 * distributed over many machines to provide scalability and fault tolerance for processing. All consumer instances
 * sharing the same {@code group.id} will be part of the same consumer group.
 * <p>
 * Each consumer in a group can dynamically set the list of topics it wants to subscribe to through one of the
 * {@link #subscribe(Collection, ConsumerRebalanceListener) subscribe} APIs. Kafka will deliver each message in the
 * subscribed topics to one process in each consumer group. This is achieved by balancing the partitions between all
 * members in the consumer group so that each partition is assigned to exactly one consumer in the group. So if there
 * is a topic with four partitions, and a consumer group with two processes, each process would consume from two partitions.
 * <p>
 * Membership in a consumer group is maintained dynamically: if a process fails, the partitions assigned to it will
 * be reassigned to other consumers in the same group. Similarly, if a new consumer joins the group, partitions will be moved
 * from existing consumers to the new one. This is known as <i>rebalancing</i> the group and is discussed in more
 * detail <a href="#failuredetection">below</a>. Group rebalancing is also used when new partitions are added
 * to one of the subscribed topics or when a new topic matching a {@link #subscribe(Pattern, ConsumerRebalanceListener) subscribed regex}
 * is created. The group will automatically detect the new partitions through periodic metadata refreshes and
 * assign them to members of the group.
 * <p>
 * Conceptually you can think of a consumer group as being a single logical subscriber that happens to be made up of
 * multiple processes. As a multi-subscriber system, Kafka naturally supports having any number of consumer groups for a
 * given topic without duplicating data (additional consumers are actually quite cheap).
 * <p>
 * This is a slight generalization of the functionality that is common in messaging systems. To get semantics similar to
 * a queue in a traditional messaging system all processes would be part of a single consumer group and hence record
 * delivery would be balanced over the group like with a queue. Unlike a traditional messaging system, though, you can
 * have multiple such groups. To get semantics similar to pub-sub in a traditional messaging system each process would
 * have its own consumer group, so each process would subscribe to all the records published to the topic.
 * <p>
 * In addition, when group reassignment happens automatically, consumers can be notified through a {@link ConsumerRebalanceListener},
 * which allows them to finish necessary application-level logic such as state cleanup, manual offset
 * commits, etc. See <a href="#rebalancecallback">Storing Offsets Outside Kafka</a> for more details.
 * <p>
 * It is also possible for the consumer to <a href="#manualassignment">manually assign</a> specific partitions
 * (similar to the older "simple" consumer) using {@link #assign(Collection)}. In this case, dynamic partition
 * assignment and consumer group coordination will be disabled.
 *
 * <h3><a name="consumergroups">消费者组和主题订阅</a></h3>
 *
 * Kafka 使用<i>消费者组</i>的概念来允许一组进程分担消费和处理记录的工作。
 * 这些进程可以在同一台机器上运行，也可以分布在多台机器上，以提供处理的可扩展性和容错性。
 * 所有共享相同{@code group.id}的消费者实例将成为同一个消费者组的一部分。
 * <p>
 * 组中的每个消费者可以通过{@link #subscribe(Collection, ConsumerRebalanceListener) subscribe} API 之一
 * 动态设置它想要订阅的主题列表。Kafka 将把订阅主题中的每条消息传递给每个消费者组中的一个进程。
 * 这是通过在消费者组的所有成员之间平衡分区来实现的，使每个分区恰好分配给组中的一个消费者。
 * 因此，如果有一个包含四个分区的主题，和一个包含两个进程的消费者组，每个进程将从两个分区消费。
 * <p>
 * 消费者组的成员资格是动态维护的：如果一个进程失败，分配给它的分区将被重新分配给同一组中的其他消费者。
 * 同样，如果一个新的消费者加入组，分区将从现有消费者移动到新的消费者。
 * 这被称为组的<i>重平衡</i>，在<a href="#failuredetection">下面</a>会有更详细的讨论。
 * 当向其中一个订阅的主题添加新分区，或创建一个匹配{@link #subscribe(Pattern, ConsumerRebalanceListener) 订阅正则表达式}的新主题时，
 * 也会使用组重平衡。组将通过定期的元数据刷新自动检测新分区，并将它们分配给组的成员。
 * <p>
 * 从概念上讲，你可以将消费者组视为由多个进程组成的单个逻辑订阅者。
 * 作为一个多订阅者系统，Kafka 自然支持对给定主题有任意数量的消费者组，而无需复制数据（额外的消费者实际上非常便宜）。
 * <p>
 * 这是对消息系统中常见功能的轻微泛化。要获得类似于传统消息系统中队列的语义，
 * 所有进程都将是单个消费者组的一部分，因此记录传递将像队列一样在组内平衡。
 * 然而，与传统的消息系统不同，你可以有多个这样的组。
 * 要获得类似于传统消息系统中发布-订阅的语义，每个进程将有自己的消费者组，
 * 因此每个进程都将订阅发布到该主题的所有记录。
 * <p>
 * 此外，当组重新分配自动发生时，消费者可以通过{@link ConsumerRebalanceListener}得到通知，
 * 这允许他们完成必要的应用程序级逻辑，如状态清理、手动偏移量提交等。
 * 有关更多详细信息，请参阅<a href="#rebalancecallback">在Kafka外存储偏移量</a>。
 * <p>
 * 消费者也可以使用{@link #assign(Collection)}来<a href="#manualassignment">手动分配</a>特定的分区
 * （类似于较旧的"简单"消费者）。在这种情况下，动态分区分配和消费者组协调将被禁用。
 *
 * <h3><a name="failuredetection">Detecting Consumer Failures</a></h3>
 *
 * After subscribing to a set of topics, the consumer will automatically join the group when {@link #poll(Duration)} is
 * invoked. The poll API is designed to ensure consumer liveness. As long as you continue to call poll, the consumer
 * will stay in the group and continue to receive messages from the partitions it was assigned. Underneath the covers,
 * the consumer sends periodic heartbeats to the server. If the consumer crashes or is unable to send heartbeats for
 * a duration of {@code session.timeout.ms}, then the consumer will be considered dead and its partitions will
 * be reassigned.
 * <p>
 * It is also possible that the consumer could encounter a "livelock" situation where it is continuing
 * to send heartbeats, but no progress is being made. To prevent the consumer from holding onto its partitions
 * indefinitely in this case, we provide a liveness detection mechanism using the {@code max.poll.interval.ms}
 * setting. Basically if you don't call poll at least as frequently as the configured max interval,
 * then the client will proactively leave the group so that another consumer can take over its partitions. When this happens,
 * you may see an offset commit failure (as indicated by a {@link CommitFailedException} thrown from a call to {@link #commitSync()}).
 * This is a safety mechanism which guarantees that only active members of the group are able to commit offsets.
 * So to stay in the group, you must continue to call poll.
 * <p>
 * The consumer provides two configuration settings to control the behavior of the poll loop:
 * <ol>
 *     <li><code>max.poll.interval.ms</code>: By increasing the interval between expected polls, you can give
 *     the consumer more time to handle a batch of records returned from {@link #poll(Duration)}. The drawback
 *     is that increasing this value may delay a group rebalance since the consumer will only join the rebalance
 *     inside the call to poll. You can use this setting to bound the time to finish a rebalance, but
 *     you risk slower progress if the consumer cannot actually call {@link #poll(Duration) poll} often enough.</li>
 *     <li><code>max.poll.records</code>: Use this setting to limit the total records returned from a single
 *     call to poll. This can make it easier to predict the maximum that must be handled within each poll
 *     interval. By tuning this value, you may be able to reduce the poll interval, which will reduce the
 *     impact of group rebalancing.</li>
 * </ol>
 * <p>
 * For use cases where message processing time varies unpredictably, neither of these options may be sufficient.
 * The recommended way to handle these cases is to move message processing to another thread, which allows
 * the consumer to continue calling {@link #poll(Duration) poll} while the processor is still working.
 * Some care must be taken to ensure that committed offsets do not get ahead of the actual position.
 * Typically, you must disable automatic commits and manually commit processed offsets for records only after the
 * thread has finished handling them (depending on the delivery semantics you need).
 * Note also that you will need to {@link #pause(Collection) pause} the partition so that no new records are received
 * from poll until after thread has finished handling those previously returned.
 *
 * <h3><a name="failuredetection">检测消费者故障</a></h3>
 *
 * 在订阅一组主题后，当调用{@link #poll(Duration)}时，消费者将自动加入组。
 * poll API 的设计是为了确保消费者的活跃性。只要你继续调用 poll，消费者就会留在组中，
 * 并继续从分配给它的分区接收消息。在底层，消费者向服务器发送定期心跳。
 * 如果消费者崩溃或在{@code session.timeout.ms}的持续时间内无法发送心跳，
 * 那么消费者将被视为死亡，其分区将被重新分配。
 * <p>
 * 消费者也可能遇到"活锁"情况，即它继续发送心跳，但没有取得进展。
 * 为了防止消费者在这种情况下无限期地保持其分区，我们提供了一个使用{@code max.poll.interval.ms}
 * 设置的活跃性检测机制。基本上，如果你不至少以配置的最大间隔频率调用 poll，
 * 那么客户端将主动离开组，以便另一个消费者可以接管其分区。当这种情况发生时，
 * 你可能会看到偏移量提交失败（由调用{@link #commitSync()}时抛出的{@link CommitFailedException}指示）。
 * 这是一种安全机制，保证只有组的活跃成员能够提交偏移量。
 * 因此，要留在组中，你必须继续调用 poll。
 * <p>
 * 消费者提供了两个配置设置来控制 poll 循环的行为：
 * <ol>
 *     <li><code>max.poll.interval.ms</code>：通过增加预期 poll 之间的间隔，
 *     你可以给消费者更多时间来处理从{@link #poll(Duration)}返回的一批记录。
 *     缺点是增加这个值可能会延迟组重平衡，因为消费者只会在调用 poll 内部加入重平衡。
 *     你可以使用这个设置来限制完成重平衡的时间，但如果消费者实际上不能足够频繁地调用{@link #poll(Duration) poll}，
 *     你就有可能进展更慢。</li>
 *     <li><code>max.poll.records</code>：使用这个设置来限制单次调用 poll 返回的总记录数。
 *     这可以使预测每个 poll 间隔内必须处理的最大值变得更容易。
 *     通过调整这个值，你可能能够减少 poll 间隔，这将减少组重平衡的影响。</li>
 * </ol>
 * <p>
 * 对于消息处理时间变化不可预测的用例，这两个选项都可能不够。
 * 处理这些情况的推荐方法是将消息处理移到另一个线程，这允许消费者继续调用{@link #poll(Duration) poll}，
 * 而处理器仍在工作。必须注意确保已提交的偏移量不会超过实际位置。
 * 通常，你必须禁用自动提交，并且只有在线程完成处理记录后才手动提交已处理的偏移量
 * （取决于你需要的传递语义）。
 * 还要注意，你需要{@link #pause(Collection) 暂停}分区，以便在线程完成处理之前返回的那些记录之前，
 * 不会从 poll 接收到新的记录。
 *
 * <h3>Usage Examples</h3>
 * The consumer APIs offer flexibility to cover a variety of consumption use cases. Here are some examples to
 * demonstrate how to use them.
 *
 * <h3>使用示例</h3>
 * 消费者 API 提供了灵活性，以覆盖各种消费用例。这里有一些示例来演示如何使用它们。
 *
 * <h4>Automatic Offset Committing</h4>
 * This example demonstrates a simple usage of Kafka's consumer api that relies on automatic offset committing.
 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;enable.auto.commit&quot;, &quot;true&quot;);
 *     props.setProperty(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records)
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *     }
 * </pre>
 *
 * <h4>自动偏移量提交</h4>
 * 这个例子演示了 Kafka 消费者 API 的一个简单用法，它依赖于自动偏移量提交。
 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;enable.auto.commit&quot;, &quot;true&quot;);
 *     props.setProperty(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records)
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *     }
 * </pre>
 *
 * The connection to the cluster is bootstrapped by specifying a list of one or more brokers to contact using the
 * configuration {@code >bootstrap.servers}. This list is just used to discover the rest of the brokers in the
 * cluster and need not be an exhaustive list of servers in the cluster (though you may want to specify more than one in
 * case there are servers down when the client is connecting).
 * <p>
 * Setting {@code enable.auto.commit} means that offsets are committed automatically with a frequency controlled by
 * the config {@code auto.commit.interval.ms}.
 * <p>
 * In this example the consumer is subscribing to the topics <i>foo</i> and <i>bar</i> as part of a group of consumers
 * called <i>test</i> as configured with {@code group.id}.
 * <p>
 * The deserializer settings specify how to turn bytes into objects. For example, by specifying string deserializers, we
 * are saying that our record's key and value will just be simple strings.
 *
 * 通过使用配置{@code >bootstrap.servers}指定一个或多个要联系的代理列表来引导与集群的连接。
 * 这个列表只用于发现集群中的其余代理，不需要是集群中服务器的详尽列表
 * （尽管你可能想指定多个，以防客户端连接时有服务器宕机）。
 * <p>
 * 设置{@code enable.auto.commit}意味着偏移量会自动提交，频率由配置{@code auto.commit.interval.ms}控制。
 * <p>
 * 在这个例子中，消费者作为一个名为<i>test</i>的消费者组的一部分订阅了主题<i>foo</i>和<i>bar</i>，
 * 这是通过{@code group.id}配置的。
 * <p>
 * 反序列化器设置指定如何将字节转换为对象。例如，通过指定字符串反序列化器，
 * 我们表示我们的记录的键和值将只是简单的字符串。
 *
 * <h4>Manual Offset Control</h4>
 *
 * Instead of relying on the consumer to periodically commit consumed offsets, users can also control when records
 * should be considered as consumed and hence commit their offsets. This is useful when the consumption of the messages
 * is coupled with some processing logic and hence a message should not be considered as consumed until it is completed processing.

 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;enable.auto.commit&quot;, &quot;false&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     final int minBatchSize = 200;
 *     List&lt;ConsumerRecord&lt;String, String&gt;&gt; buffer = new ArrayList&lt;&gt;();
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             buffer.add(record);
 *         }
 *         if (buffer.size() &gt;= minBatchSize) {
 *             insertIntoDb(buffer);
 *             consumer.commitSync();
 *             buffer.clear();
 *         }
 *     }
 * </pre>
 *
 * <h4>手动偏移量控制</h4>
 *
 * 除了依赖消费者定期提交已消费的偏移量外，用户还可以控制何时应将记录视为已消费，从而提交它们的偏移量。
 * 当消息的消费与某些处理逻辑相关联时，这很有用，因此在完成处理之前，不应将消息视为已消费。
 *
 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;enable.auto.commit&quot;, &quot;false&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     final int minBatchSize = 200;
 *     List&lt;ConsumerRecord&lt;String, String&gt;&gt; buffer = new ArrayList&lt;&gt;();
 *     while (true) {
 * In this example we will consume a batch of records and batch them up in memory. When we have enough records
 * batched, we will insert them into a database. If we allowed offsets to auto commit as in the previous example, records
 * would be considered consumed after they were returned to the user in {@link #poll(Duration) poll}. It would then be
 * possible
 * for our process to fail after batching the records, but before they had been inserted into the database.
 * 
 * 在这个例子中，我们将消费一批记录并将它们批量存储在内存中。
 * 当我们有足够多的记录批量处理时，我们将它们插入到数据库中。
 * 如果我们像前面的例子那样允许偏移量自动提交，那么记录在通过 {@link #poll(Duration) poll} 返回给用户后就会被视为已消费。
 * 这样，我们的进程可能在批量处理记录后但在将它们插入数据库之前失败。
 *
 * <p>
 * To avoid this, we will manually commit the offsets only after the corresponding records have been inserted into the
 * database. This gives us exact control of when a record is considered consumed. This raises the opposite possibility:
 * the process could fail in the interval after the insert into the database but before the commit (even though this
 * would likely just be a few milliseconds, it is a possibility). In this case the process that took over consumption
 * would consume from last committed offset and would repeat the insert of the last batch of data. Used in this way
 * Kafka provides what is often called "at-least-once" delivery guarantees, as each record will likely be delivered one
 * time but in failure cases could be duplicated.
 * 
 * 为了避免这种情况，我们将只在相应的记录被插入到数据库之后才手动提交偏移量。
 * 这使我们能够精确控制何时将记录视为已消费。
 * 这引发了相反的可能性：进程可能在将数据插入数据库之后但在提交之前的间隔中失败（尽管这可能只是几毫秒的时间，但这是一种可能性）。
 * 在这种情况下，接管消费的进程将从最后提交的偏移量开始消费，并重复插入最后一批数据。
 * 以这种方式使用时，Kafka 提供了所谓的"至少一次"交付保证，因为每条记录很可能会被交付一次，但在失败的情况下可能会重复。
 *
 * <p>
 * <b>Note: Using automatic offset commits can also give you "at-least-once" delivery, but the requirement is that
 * you must consume all data returned from each call to {@link #poll(Duration)} before any subsequent calls, or before
 * {@link #close() closing} the consumer. If you fail to do either of these, it is possible for the committed offset
 * to get ahead of the consumed position, which results in missing records. The advantage of using manual offset
 * control is that you have direct control over when a record is considered "consumed."</b>
 * 
 * <b>注意：使用自动偏移量提交也可以给你"至少一次"交付，
 * 但要求是你必须在任何后续调用之前或在 {@link #close() 关闭} 消费者之前消费从每次 {@link #poll(Duration)} 调用返回的所有数据。
 * 如果你未能做到这两点中的任何一个，提交的偏移量可能会超前于已消费的位置，从而导致丢失记录。
 * 使用手动偏移量控制的优势在于你可以直接控制何时将记录视为"已消费"。</b>
 *
 * <p>
 * The above example uses {@link #commitSync() commitSync} to mark all received records as committed. In some cases
 * you may wish to have even finer control over which records have been committed by specifying an offset explicitly.
 * In the example below we commit offset after we finish handling the records in each partition.
 * 
 * 上面的例子使用 {@link #commitSync() commitSync} 来标记所有接收到的记录为已提交。
 * 在某些情况下，你可能希望通过明确指定偏移量来更精细地控制哪些记录已被提交。
 * 在下面的例子中，我们在处理完每个分区中的记录后提交偏移量。
 *
 * <p>
 * <pre>
 *     try {
 *         while(running) {
 *             ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
 *             for (TopicPartition partition : records.partitions()) {
 *                 List&lt;ConsumerRecord&lt;String, String&gt;&gt; partitionRecords = records.records(partition);
 *                 for (ConsumerRecord&lt;String, String&gt; record : partitionRecords) {
 *                     System.out.println(record.offset() + &quot;: &quot; + record.value());
 *                 }
 *                 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
 *                 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
 *             }
 *         }
 *     } finally {
 *       consumer.close();
 *     }
 * </pre>
 *
 * <b>Note: The committed offset should always be the offset of the next message that your application will read.</b>
 * Thus, when calling {@link #commitSync(Map) commitSync(offsets)} you should add one to the offset of the last message processed.
 * 
 * <b>注意：提交的偏移量应该始终是你的应用程序将要读取的下一条消息的偏移量。</b>
 * 因此，当调用 {@link #commitSync(Map) commitSync(offsets)} 时，你应该将最后处理的消息的偏移量加一。
 *
 * <h4><a name="manualassignment">Manual Partition Assignment</a></h4>
 * 
 * <h4><a name="manualassignment">手动分区分配</a></h4>
 *
 * In the previous examples, we subscribed to the topics we were interested in and let Kafka dynamically assign a
 * fair share of the partitions for those topics based on the active consumers in the group. However, in
 * some cases you may need finer control over the specific partitions that are assigned. For example:
 * 
 * 在前面的例子中，我们订阅了我们感兴趣的主题，并让 Kafka 根据组中的活跃消费者动态分配这些主题的分区。
 * 然而，在某些情况下，你可能需要对分配的特定分区进行更精细的控制。例如：
 *
 * <p>
 * <ul>
 * <li>If the process is maintaining some kind of local state associated with that partition (like a
 * local on-disk key-value store), then it should only get records for the partition it is maintaining on disk.
 * <li>If the process itself is highly available and will be restarted if it fails (perhaps using a
 * cluster management framework like YARN, Mesos, or AWS facilities, or as part of a stream processing framework). In
 * this case there is no need for Kafka to detect the failure and reassign the partition since the consuming process
 * will be restarted on another machine.
 * </ul>
 * 
 * <ul>
 * <li>如果进程维护着与该分区相关的某种本地状态（如本地磁盘上的键值存储），那么它应该只获取它在磁盘上维护的分区的记录。
 * <li>如果进程本身具有高可用性，并且在失败时会重新启动（可能使用像 YARN、Mesos 或 AWS 设施这样的集群管理框架，或作为流处理框架的一部分）。
 *      在这种情况下，Kafka 不需要检测失败并重新分配分区，因为消费进程将在另一台机器上重新启动。
 * </ul>
 *
 * <p>
 * To use this mode, instead of subscribing to the topic using {@link #subscribe(Collection) subscribe}, you just call
 * {@link #assign(Collection)} with the full list of partitions that you want to consume.
 * 
 * 要使用这种模式，你不需要使用 {@link #subscribe(Collection) subscribe} 来订阅主题，而是直接调用 {@link #assign(Collection)}，
 * 并传入你想要消费的完整分区列表。
 *
 * <pre>
 *     String topic = &quot;foo&quot;;
 *     TopicPartition partition0 = new TopicPartition(topic, 0);
 *     TopicPartition partition1 = new TopicPartition(topic, 1);
 *     consumer.assign(Arrays.asList(partition0, partition1));
 * </pre>
 *
 * Once assigned, you can call {@link #poll(Duration) poll} in a loop, just as in the preceding examples to consume
 * records. The group that the consumer specifies is still used for committing offsets, but now the set of partitions
 * will only change with another call to {@link #assign(Collection) assign}. Manual partition assignment does
 * not use group coordination, so consumer failures will not cause assigned partitions to be rebalanced. Each consumer
 * acts independently even if it shares a groupId with another consumer. To avoid offset commit conflicts, you should
 * usually ensure that the groupId is unique for each consumer instance.
 * 
 * 一旦分配完成，你可以像前面的例子一样在循环中调用 {@link #poll(Duration) poll} 来消费记录。
 * 消费者指定的组仍然用于提交偏移量，但现在分区集只会通过另一次调用 {@link #assign(Collection) assign} 来改变。
 * 手动分区分配不使用组协调，所以消费者失败不会导致已分配的分区被重新平衡。每个消费者都独立行动，即使它与另一个消费者共享一个 groupId。
 * 为了避免偏移量提交冲突，你通常应该确保每个消费者实例的 groupId 是唯一的。
 *
 * <p>
 * Note that it isn't possible to mix manual partition assignment (i.e. using {@link #assign(Collection) assign})
 * with dynamic partition assignment through topic subscription (i.e. using {@link #subscribe(Collection) subscribe}).
 * 
 * 请注意，不可能将手动分区分配（即使用 {@link #assign(Collection) assign}）
 * 与通过主题订阅的动态分区分配（即使用 {@link #subscribe(Collection) subscribe}）混合使用。
 *
 * <h4><a name="rebalancecallback">Storing Offsets Outside Kafka</h4>
 * 
 * <h4><a name="rebalancecallback">在 Kafka 外存储偏移量</h4>
 *
 * The consumer application need not use Kafka's built-in offset storage, it can store offsets in a store of its own
 * choosing. The primary use case for this is allowing the application to store both the offset and the results of the
 * consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
 * possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
 * stronger than the default "at-least once" semantics you get with Kafka's offset commit functionality.
 * 
 * 消费者应用程序不必使用 Kafka 的内置偏移量存储，它可以将偏移量存储在自己选择的存储中。
 * 这种做法的主要用例是允许应用程序以一种方式将偏移量和消费结果同时存储在同一个系统中，
 * 使得结果和偏移量都能原子地存储。这并不总是可能的，但当它可行时，它将使消费完全原子化，
 * 并提供比 Kafka 的偏移量提交功能默认的"至少一次"语义更强的"恰好一次"语义。
 *
 * <p>
 * Here are a couple of examples of this type of usage:
 * 
 * 这里有几个这种用法的例子：
 *
 * <ul>
 * <li>If the results of the consumption are being stored in a relational database, storing the offset in the database
 * as well can allow committing both the results and offset in a single transaction. Thus either the transaction will
 * succeed and the offset will be updated based on what was consumed or the result will not be stored and the offset
 * won't be updated.
 * <li>If the results are being stored in a local store it may be possible to store the offset there as well. For
 * example a search index could be built by subscribing to a particular partition and storing both the offset and the
 * indexed data together. If this is done in a way that is atomic, it is often possible to have it be the case that even
 * if a crash occurs that causes unsync'd data to be lost, whatever is left has the corresponding offset stored as well.
 * This means that in this case the indexing process that comes back having lost recent updates just resumes indexing
 * from what it has ensuring that no updates are lost.
 * </ul>
 * 
 * <ul>
 * <li>如果消费的结果被存储在关系数据库中，将偏移量也存储在数据库中可以允许在单个事务中同时提交结果和偏移量。
 *  因此，要么事务成功，偏移量根据消费的内容更新，要么结果不会被存储，偏移量也不会更新。
 * <li>如果结果被存储在本地存储中，也可能将偏移量存储在那里。例如，可以通过订阅特定分区并同时存储偏移量和索引数据来构建搜索索引。
 *  如果这是以原子方式完成的，即使发生崩溃导致未同步的数据丢失，通常也可能使剩下的数据具有相应存储的偏移量。
 *  这意味着在这种情况下，丢失了最近更新的索引进程恢复时，只需从它所拥有的内容继续索引，确保不会丢失任何更新。
 * </ul>
 *
 * <p>
 * Each record comes with its own offset, so to manage your own offset you just need to do the following:
 * 
 * 每条记录都有自己的偏移量，所以要管理你自己的偏移量，你只需要做以下几点：
 *
 * <ul>
 * <li>Configure <code>enable.auto.commit=false</code>
 * <li>Use the offset provided with each {@link ConsumerRecord} to save your position.
 * <li>On restart restore the position of the consumer using {@link #seek(TopicPartition, long)}.
 * </ul>
 * 
 * <ul>
 * <li>配置 <code>enable.auto.commit=false</code>
 * <li>使用每个 {@link ConsumerRecord} 提供的偏移量来保存你的位置。
 * <li>在重启时使用 {@link #seek(TopicPartition, long)} 恢复消费者的位置。
 * </ul>
 *
 * <p>
 * This type of usage is simplest when the partition assignment is also done manually (this would be likely in the
 * search index use case described above). If the partition assignment is done automatically special care is
 * needed to handle the case where partition assignments change. This can be done by providing a
 * {@link ConsumerRebalanceListener} instance in the call to {@link #subscribe(Collection, ConsumerRebalanceListener)}
 * and {@link #subscribe(Pattern, ConsumerRebalanceListener)}.
 * For example, when partitions are taken from a consumer the consumer will want to commit its offset for those partitions by
 * implementing {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)}. When partitions are assigned to a
 * consumer, the consumer will want to look up the offset for those new partitions and correctly initialize the consumer
 * to that position by implementing {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)}.
 * 
 * 当分区分配也是手动完成时（这在上面描述的搜索索引用例中可能会出现），这种类型的用法最简单。
 * 如果分区分配是自动完成的，则需要特别注意处理分区分配变化的情况。
 * 这可以通过在调用 {@link #subscribe(Collection, ConsumerRebalanceListener)} 和 
 * {@link #subscribe(Pattern, ConsumerRebalanceListener)} 时提供一个 {@link ConsumerRebalanceListener} 实例来完成。
 * 例如，当分区从消费者那里被收回时，消费者会想要通过实现 {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} 来提交这些分区的偏移量。
 * 当分区被分配给消费者时，消费者会想要查找这些新分区的偏移量，
 * 并通过实现 {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)} 正确地将消费者初始化到该位置。
 *
 * <p>
 * Another common use for {@link ConsumerRebalanceListener} is to flush any caches the application maintains for
 * partitions that are moved elsewhere.
 * 
 * {@link ConsumerRebalanceListener} 的另一个常见用途是刷新应用程序为被移动到其他地方的分区维护的任何缓存。
 *
 * <h4>Controlling The Consumer's Position</h4>
 * 
 * <h4>控制消费者的位置</h4>
 *
 * In most use cases the consumer will simply consume records from beginning to end, periodically committing its
 * position (either automatically or manually). However Kafka allows the consumer to manually control its position,
 * moving forward or backwards in a partition at will. This means a consumer can re-consume older records, or skip to
 * the most recent records without actually consuming the intermediate records.
 * 
 * 在大多数用例中，消费者将简单地从头到尾消费记录，定期提交其位置（自动或手动）。
 * 然而，Kafka 允许消费者手动控制其位置，可以随意在分区中前进或后退。
 * 这意味着消费者可以重新消费较旧的记录，或者跳到最新的记录而不实际消费中间的记录。
 *
 * <p>
 * There are several instances where manually controlling the consumer's position can be useful.
 * 
 * 有几种情况下手动控制消费者的位置可能会很有用。
 *
 * <p>
 * One case is for time-sensitive record processing it may make sense for a consumer that falls far enough behind to not
 * attempt to catch up processing all records, but rather just skip to the most recent records.
 * 
 * 一种情况是对于时间敏感的记录处理，对于落后太多的消费者来说，不尝试赶上处理所有记录，而是直接跳到最新的记录可能更有意义。
 *
 * <p>
 * Another use case is for a system that maintains local state as described in the previous section. In such a system
 * the consumer will want to initialize its position on start-up to whatever is contained in the local store. Likewise
 * if the local state is destroyed (say because the disk is lost) the state may be recreated on a new machine by
 * re-consuming all the data and recreating the state (assuming that Kafka is retaining sufficient history).
 * 
 * 另一个用例是对于前一节描述的维护本地状态的系统。
 * 在这样的系统中，消费者在启动时会希望将其位置初始化为本地存储中包含的内容。
 * 同样，如果本地状态被销毁（比如因为磁盘丢失），可以通过重新消费所有数据并重新创建状态来在新机器上重建状态（假设 Kafka 保留了足够的历史记录）。
 *
 * <p>
 * Kafka allows specifying the position using {@link #seek(TopicPartition, long)} to specify the new position. Special
 * methods for seeking to the earliest and latest offset the server maintains are also available (
 * {@link #seekToBeginning(Collection)} and {@link #seekToEnd(Collection)} respectively).
 * 
 * Kafka 允许使用 {@link #seek(TopicPartition, long)} 来指定新的位置。
 * 还提供了寻找服务器维护的最早和最新偏移量的特殊方法（分别是 {@link #seekToBeginning(Collection)} 和 {@link #seekToEnd(Collection)}）。
 *
 * <h4>Consumption Flow Control</h4>
 * 
 * <h4>消费流控制</h4>
 *
 * If a consumer is assigned multiple partitions to fetch data from, it will try to consume from all of them at the same time,
 * effectively giving these partitions the same priority for consumption. However in some cases consumers may want to
 * first focus on fetching from some subset of the assigned partitions at full speed, and only start fetching other partitions
 * when these partitions have few or no data to consume.
 * 
 * 如果一个消费者被分配了多个分区来获取数据，它会尝试同时从所有分区消费，实际上给这些分区相同的消费优先级。
 * 然而，在某些情况下，消费者可能希望首先专注于以全速从某些分配的分区子集获取数据，
 * 只有当这些分区几乎没有或没有数据可消费时，才开始获取其他分区的数据。
 *
 * <p>
 * One of such cases is stream processing, where processor fetches from two topics and performs the join on these two streams.
 * When one of the topics is long lagging behind the other, the processor would like to pause fetching from the ahead topic
 * in order to get the lagging stream to catch up. Another example is bootstraping upon consumer starting up where there are
 * a lot of history data to catch up, the applications usually want to get the latest data on some of the topics before consider
 * fetching other topics.
 * 
 * 这种情况之一是流处理，处理器从两个主题获取数据并对这两个流进行连接。
 * 当其中一个主题远远落后于另一个时，处理器希望暂停从领先的主题获取数据，以便让落后的流赶上。
 * 另一个例子是消费者启动时的引导过程，当有大量历史数据需要赶上时，应用程序通常希望在考虑获取其他主题之前先获取某些主题的最新数据。
 *
 * <p>
 * Kafka supports dynamic controlling of consumption flows by using {@link #pause(Collection)} and {@link #resume(Collection)}
 * to pause the consumption on the specified assigned partitions and resume the consumption
 * on the specified paused partitions respectively in the future {@link #poll(Duration)} calls.
 * 
 * Kafka 支持通过使用 {@link #pause(Collection)} 和 {@link #resume(Collection)} 来动态控制消费流，
 * 分别用于在未来的 {@link #poll(Duration)} 调用中暂停指定已分配分区的消费和恢复指定已暂停分区的消费。
 *
 * <h3>Reading Transactional Messages</h3>
 * 
 * <h3>读取事务性消息</h3>
 *
 * <p>
 * Transactions were introduced in Kafka 0.11.0 wherein applications can write to multiple topics and partitions atomically.
 * In order for this to work, consumers reading from these partitions should be configured to only read committed data.
 * This can be achieved by setting the {@code isolation.level=read_committed} in the consumer's configuration.
 * 
 * 事务在 Kafka 0.11.0 中引入，应用程序可以原子地写入多个主题和分区。
 * 为了使这个功能正常工作，从这些分区读取的消费者应该被配置为只读取已提交的数据。
 * 这可以通过在消费者的配置中设置 {@code isolation.level=read_committed} 来实现。
 *
 * <p>
 * In <code>read_committed</code> mode, the consumer will read only those transactional messages which have been
 * successfully committed. It will continue to read non-transactional messages as before. There is no client-side
 * buffering in <code>read_committed</code> mode. Instead, the end offset of a partition for a <code>read_committed</code>
 * consumer would be the offset of the first message in the partition belonging to an open transaction. This offset
 * is known as the 'Last Stable Offset'(LSO).
 * 
 * 在 <code>read_committed</code> 模式下，消费者将只读取那些已成功提交的事务性消息。
 * 它将继续像以前一样读取非事务性消息。在 <code>read_committed</code> 模式下没有客户端缓冲。
 * 相反，对于 <code>read_committed</code> 消费者来说，分区的结束偏移量将是属于一个开放事务的分区中第一条消息的偏移量。
 * 这个偏移量被称为"最后稳定偏移量"（LSO）。
 *
 * <p>
 * A {@code read_committed} consumer will only read up to the LSO and filter out any transactional
 * messages which have been aborted. The LSO also affects the behavior of {@link #seekToEnd(Collection)} and
 * {@link #endOffsets(Collection)} for {@code read_committed} consumers, details of which are in each method's documentation.
 * Finally, the fetch lag metrics are also adjusted to be relative to the LSO for {@code read_committed} consumers.
 * 
 * {@code read_committed} 消费者将只读取到 LSO，并过滤掉任何已中止的事务性消息。
 * LSO 还会影响 {@code read_committed} 消费者的 {@link #seekToEnd(Collection)} 和 {@link #endOffsets(Collection)} 的行为，
 * 详细信息在每个方法的文档中。最后，对于 {@code read_committed} 消费者，获取滞后指标也会调整为相对于 LSO 的值。
 *
 * <p>
 * Partitions with transactional messages will include commit or abort markers which indicate the result of a transaction.
 * There markers are not returned to applications, yet have an offset in the log. As a result, applications reading from
 * topics with transactional messages will see gaps in the consumed offsets. These missing messages would be the transaction
 * markers, and they are filtered out for consumers in both isolation levels. Additionally, applications using
 * {@code read_committed} consumers may also see gaps due to aborted transactions, since those messages would not
 * be returned by the consumer and yet would have valid offsets.
 * 
 * 包含事务性消息的分区将包括提交或中止标记，这些标记指示事务的结果。
 * 这些标记不会返回给应用程序，但在日志中有一个偏移量。因此，从包含事务性消息的主题读取的应用程序将在消费的偏移量中看到间隙。
 * 这些缺失的消息将是事务标记，它们在两种隔离级别的消费者中都被过滤掉。
 * 此外，使用 {@code read_committed} 消费者的应用程序可能还会因为中止的事务而看到间隙，因为这些消息不会被消费者返回，但却有有效的偏移量。
 *
 * <h3><a name="multithreaded">Multi-threaded Processing</a></h3>
 * 
 * <h3><a name="multithreaded">多线程处理</a></h3>
 *
 * The Kafka consumer is NOT thread-safe. All network I/O happens in the thread of the application
 * making the call. It is the responsibility of the user to ensure that multi-threaded access
 * is properly synchronized. Un-synchronized access will result in {@link ConcurrentModificationException}.
 * 
 * Kafka 消费者不是线程安全的。
 * 所有网络 I/O 都发生在进行调用的应用程序线程中。
 * 用户有责任确保多线程访问得到适当的同步。未同步的访问将导致 {@link ConcurrentModificationException}。
 *
 * <p>
 * The only exception to this rule is {@link #wakeup()}, which can safely be used from an external thread to
 * interrupt an active operation. In this case, a {@link org.apache.kafka.common.errors.WakeupException} will be
 * thrown from the thread blocking on the operation. This can be used to shutdown the consumer from another thread.
 * The following snippet shows the typical pattern:
 * 
 * 这个规则的唯一例外是 {@link #wakeup()}，它可以安全地从外部线程使用来中断活动操作。
 * 在这种情况下，一个 {@link org.apache.kafka.common.errors.WakeupException} 将从阻塞在操作上的线程中抛出。
 * 这可以用于从另一个线程关闭消费者。以下代码片段展示了典型的模式：
 *
 * <pre>
 * public class KafkaConsumerRunner implements Runnable {
 *     private final AtomicBoolean closed = new AtomicBoolean(false);
 *     private final KafkaConsumer consumer;
 *
 *     public KafkaConsumerRunner(KafkaConsumer consumer) {
 *       this.consumer = consumer;
 *     }
 *
 *     {@literal}@Override
 *     public void run() {
 *         try {
 *             consumer.subscribe(Arrays.asList("topic"));
 *             while (!closed.get()) {
 *                 ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
 *                 // Handle new records
 *             }
 *         } catch (WakeupException e) {
 *             // Ignore exception if closing
 *             if (!closed.get()) throw e;
 *         } finally {
 *             consumer.close();
 *         }
 *     }
 *
 *     // Shutdown hook which can be called from a separate thread
 *     public void shutdown() {
 *         closed.set(true);
 *         consumer.wakeup();
 *     }
 * }
 * </pre>
 *
 * Then in a separate thread, the consumer can be shutdown by setting the closed flag and waking up the consumer.
 * 
 * 然后在一个单独的线程中，可以通过设置关闭标志并唤醒消费者来关闭消费者。
 *
 * <p>
 * <pre>
 *     closed.set(true);
 *     consumer.wakeup();
 * </pre>
 *
 * <p>
 * Note that while it is possible to use thread interrupts instead of {@link #wakeup()} to abort a blocking operation
 * (in which case, {@link InterruptException} will be raised), we discourage their use since they may cause a clean
 * shutdown of the consumer to be aborted. Interrupts are mainly supported for those cases where using {@link #wakeup()}
 * is impossible, e.g. when a consumer thread is managed by code that is unaware of the Kafka client.
 * 
 * 请注意，虽然可以使用线程中断而不是 {@link #wakeup()} 来中止阻塞操作（在这种情况下，将引发 {@link InterruptException}），
 * 但我们不鼓励使用它们，因为它们可能导致消费者的干净关闭被中止。
 * 中断主要支持那些无法使用 {@link #wakeup()} 的情况，例如当消费者线程由不知道 Kafka 客户端的代码管理时。
 *
 * <p>
 * We have intentionally avoided implementing a particular threading model for processing. This leaves several
 * options for implementing multi-threaded processing of records.
 * 
 * 我们有意避免为处理实现特定的线程模型。这为实现记录的多线程处理留下了几个选项。
 *
 * <h4>1. One Consumer Per Thread</h4>
 * 
 * <h4>1. 每个线程一个消费者</h4>
 *
 * A simple option is to give each thread its own consumer instance. Here are the pros and cons of this approach:
 * 
 * 一个简单的选项是给每个线程自己的消费者实例。以下是这种方法的优缺点：
 * <ul>
 * <li><b>PRO</b>: It is the easiest to implement
 * <li><b>PRO</b>: It is often the fastest as no inter-thread co-ordination is needed
 * <li><b>PRO</b>: It makes in-order processing on a per-partition basis very easy to implement (each thread just
 * processes messages in the order it receives them).
 * <li><b>CON</b>: More consumers means more TCP connections to the cluster (one per thread). In general Kafka handles
 * connections very efficiently so this is generally a small cost.
 * <li><b>CON</b>: Multiple consumers means more requests being sent to the server and slightly less batching of data
 * which can cause some drop in I/O throughput.
 * <li><b>CON</b>: The number of total threads across all processes will be limited by the total number of partitions.
 * </ul>
 * 
 * <ul>
 * <li><b>优点</b>：最容易实现
 * <li><b>优点</b>：通常是最快的，因为不需要线程间协调
 * <li><b>优点</b>：使得在每个分区的基础上进行有序处理变得非常容易实现（每个线程只按接收顺序处理消息）
 * <li><b>缺点</b>：更多的消费者意味着更多的 TCP 连接到集群（每个线程一个）。一般来说，Kafka 处理连接非常高效，所以这通常是一个小成本
 * <li><b>缺点</b>：多个消费者意味着向服务器发送更多的请求，数据批处理略少，可能导致 I/O 吞吐量有所下降
 * <li><b>缺点</b>：所有进程中的总线程数将受到分区总数的限制
 * </ul>
 *
 * <h4>2. Decouple Consumption and Processing</h4>
 * 
 * <h4>2. 解耦消费和处理</h4>
 *
 * Another alternative is to have one or more consumer threads that do all data consumption and hands off
 * {@link ConsumerRecords} instances to a blocking queue consumed by a pool of processor threads that actually handle
 * the record processing.
 * 
 * 另一种选择是有一个或多个消费者线程进行所有数据消费，并将 {@link ConsumerRecords} 实例传递给一个阻塞队列，
 * 由一组处理器线程池消费，这些线程实际处理记录。
 *
 * This option likewise has pros and cons:
 * 
 * 这个选项同样有优点和缺点：
 * <ul>
 * <li><b>PRO</b>: This option allows independently scaling the number of consumers and processors. This makes it
 * possible to have a single consumer that feeds many processor threads, avoiding any limitation on partitions.
 * <li><b>CON</b>: Guaranteeing order across the processors requires particular care as the threads will execute
 * independently an earlier chunk of data may actually be processed after a later chunk of data just due to the luck of
 * thread execution timing. For processing that has no ordering requirements this is not a problem.
 * <li><b>CON</b>: Manually committing the position becomes harder as it requires that all threads co-ordinate to ensure
 * that processing is complete for that partition.
 * </ul>
 * 
 * <ul>
 * <li><b>优点</b>：这个选项允许独立地扩展消费者和处理器的数量。这使得可以有一个单一的消费者为多个处理器线程提供数据，避免了对分区的任何限制。
 * <li><b>缺点</b>：保证处理器之间的顺序需要特别小心，因为线程将独立执行，较早的数据块实际上可能在较晚的数据块之后处理，这只是由于线程执行时间的运气。对于没有顺序要求的处理，这不是问题。
 * <li><b>缺点</b>：手动提交位置变得更加困难，因为它要求所有线程协调以确保该分区的处理已完成。
 * </ul>
 *
 * There are many possible variations on this approach. For example each processor thread can have its own queue, and
 * the consumer threads can hash into these queues using the TopicPartition to ensure in-order consumption and simplify
 * commit.
 * 
 * 这种方法有许多可能的变体。例如，每个处理器线程可以有自己的队列，
 * 消费者线程可以使用 TopicPartition 对这些队列进行哈希，以确保有序消费并简化提交。
 */
public class KafkaConsumer<K, V> implements Consumer<K, V> {

    // 客户端 ID 的度量标签
    private static final String CLIENT_ID_METRIC_TAG = "client-id";
    // 表示当前没有线程访问KafkaConsumer的常量
    private static final long NO_CURRENT_THREAD = -1L;
    // JMX前缀
    private static final String JMX_PREFIX = "kafka.consumer";
    // 默认关闭超时时间（毫秒）
    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    // 可见性为测试使用
    // 度量指标
    final Metrics metrics;
    // Kafka消费者度量指标
    final KafkaConsumerMetrics kafkaConsumerMetrics;

    // 日志记录器
    private Logger log;
    // 客户端 ID
    private final String clientId;
    // 消费者组 ID（可选）
    private final Optional<String> groupId;
    // 消费者协调器
    private final ConsumerCoordinator coordinator;
    // key 反序列化器
    private final Deserializer<K> keyDeserializer;
    // value 反序列化器
    private final Deserializer<V> valueDeserializer;
    // 数据获取器
    private final Fetcher<K, V> fetcher;
    // 消费者拦截器
    private final ConsumerInterceptors<K, V> interceptors;
    // 隔离级别
    private final IsolationLevel isolationLevel;

    // 时间工具
    private final Time time;
    // 消费者网络客户端
    private final ConsumerNetworkClient client;
    // 订阅状态，这是一个非常重要的对象，它保存了消费者订阅的所有信息
    private final SubscriptionState subscriptions;
    // 消费者元数据
    private final ConsumerMetadata metadata;
    // 重试回退时间（毫秒）
    private final long retryBackoffMs;
    // 请求超时时间（毫秒）
    private final long requestTimeoutMs;
    // 默认API超时时间（毫秒）
    private final int defaultApiTimeoutMs;
    // 消费者是否已关闭的标志
    private volatile boolean closed = false;
    // 分区分配器列表
    private List<ConsumerPartitionAssignor> assignors;

    // currentThread holds the threadId of the current thread accessing KafkaConsumer
    // and is used to prevent multi-threaded access
    // currentThread 保存当前访问 KafkaConsumer 的线程 ID
    // 用于防止多线程访问
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    // refcount 用于允许获取 currentThread 的线程进行重入访问
    private final AtomicInteger refcount = new AtomicInteger(0);

    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    // 为了避免在 poll() 中重复扫描订阅，在元数据更新期间缓存结果
    private boolean cachedSubscriptionHashAllFetchPositions;

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     *     通过提供一组键值对作为配置来实例化消费者。
     *     有效的配置字符串在 <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >这里</a> 中有文档。
     *     值可以是字符串，也可以是适当类型的对象（例如，数值配置可以接受字符串 "42" 或整数 42）。
     *     <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     *     有效的配置字符串在 {@link ConsumerConfig} 中有文档。
     *     <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     * <p>
     *     注意：创建 {@code KafkaConsumer} 后，必须始终 {@link #close()} 它以避免资源泄漏。
     *
     * @param configs The consumer configs
     */
    public KafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     */
    public KafkaConsumer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
     * key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(Utils.propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Map<String, Object> configs,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        try {
            // 创建 GroupRebalanceConfig 对象，用于配置消费者组 rebalance 配置
            // GroupRebalanceConfig 会从 ConsumerConfig 中抽取一些它需要的配置
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                    GroupRebalanceConfig.ProtocolType.CONSUMER);

            this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);

            LogContext logContext;

            // If group.instance.id is set, we will append it to the log context.
            // 创建 LogContext 对象，用于记录日志
            if (groupRebalanceConfig.groupInstanceId.isPresent()) {
                logContext = new LogContext("[Consumer instanceId=" + groupRebalanceConfig.groupInstanceId.get() +
                        ", clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] ");
            } else {
                logContext = new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] ");
            }

            this.log = logContext.logger(getClass());
            // 是否启用自动提交
            boolean enableAutoCommit = config.maybeOverrideEnableAutoCommit();
            // 如果 group.id 为空，则记录警告
            groupId.ifPresent(groupIdStr -> {
                if (groupIdStr.isEmpty()) {
                    log.warn("Support for using the empty group id by consumers is deprecated and will be removed in the next major release.");
                }
            });

            log.debug("Initializing the Kafka consumer");
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.time = Time.SYSTEM;
            this.metrics = buildMetrics(config, time, clientId);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            // 从 consumerConfig 中加载所有的拦截器
            List<ConsumerInterceptor<K, V>> interceptorList = (List) config.getConfiguredInstances(
                    ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ConsumerInterceptor.class,
                    Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
            // 将所有的拦截器封装成 ConsumerInterceptors 对象，方便触发回调
            this.interceptors = new ConsumerInterceptors<>(interceptorList);
            // 实例化 keyDeserializer 和 valueDeserializer
            if (keyDeserializer == null) {
                this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
                this.keyDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
            } else {
                config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
                this.keyDeserializer = keyDeserializer;
            }
            if (valueDeserializer == null) {
                this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
                this.valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
            } else {
                config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
                this.valueDeserializer = valueDeserializer;
            }
            // 从 consumerConfig 中确定 offsetResetStrategy（earliest、latest）
            OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
            this.subscriptions = new SubscriptionState(logContext, offsetResetStrategy);
            // 针对所有实现了 ClusterResourceListener 接口的类，封装一个 ClusterResourceListeners 对象，传给 metadata 方便触发回调
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keyDeserializer,
                    valueDeserializer, metrics.reporters(), interceptorList);
            // 封装一个 metadata 对象用于感知集群元数据的变化
            this.metadata = new ConsumerMetadata(retryBackoffMs,
                    config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                    !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                    config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                    subscriptions, logContext, clusterResourceListeners);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
            this.metadata.bootstrap(addresses);

            String metricGrpPrefix = "consumer";
            FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry(Collections.singleton(CLIENT_ID_METRIC_TAG), metricGrpPrefix);
            // 确定隔离级别
            this.isolationLevel = IsolationLevel.valueOf(
                    config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));
            Sensor throttleTimeSensor = Fetcher.throttleTimeSensor(metrics, metricsRegistry);
            // 确定心跳时间
            int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);

            // 创建一个 networkClient 对象，然后再基于 networkClient 创建一个 ConsumerNetworkClient 对象
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
            ApiVersions apiVersions = new ApiVersions();
            NetworkClient netClient = new NetworkClient(
                    new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, channelBuilder, logContext),
                    this.metadata,
                    clientId,
                    100, // a fixed large enough value will suffice for max in-flight requests
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                    config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                    time,
                    true,
                    apiVersions,
                    throttleTimeSensor,
                    logContext);
            this.client = new ConsumerNetworkClient(
                    logContext,
                    netClient,
                    metadata,
                    time,
                    retryBackoffMs,
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    heartbeatIntervalMs); //Will avoid blocking an extended period of time to prevent heartbeat thread starvation

            // 根据配置创建分区分配器列表，最终会由 ConsumerCoordinator 选择一个分区分配器
            this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );

            // no coordinator will be constructed for the default (null) group id
            // 如果 group.id 为空，则不会构造协调器
            this.coordinator = !groupId.isPresent() ? null :
                new ConsumerCoordinator(groupRebalanceConfig,
                        logContext,
                        this.client,
                        assignors,
                        this.metadata,
                        this.subscriptions,
                        metrics,
                        metricGrpPrefix,
                        this.time,
                        enableAutoCommit,
                        config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
                        this.interceptors,
                        config.getBoolean(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
            // 创建一个 Fetcher 对象，用于从 Kafka 服务器获取数据
            this.fetcher = new Fetcher<>(
                    logContext,
                    this.client,
                    config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG),
                    config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                    config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG),
                    config.getString(ConsumerConfig.CLIENT_RACK_CONFIG),
                    this.keyDeserializer,
                    this.valueDeserializer,
                    this.metadata,
                    this.subscriptions,
                    metrics,
                    metricsRegistry,
                    this.time,
                    this.retryBackoffMs,
                    this.requestTimeoutMs,
                    isolationLevel,
                    apiVersions);

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, metricGrpPrefix);

            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                close(0, true);
            }
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    // visible for testing
    KafkaConsumer(LogContext logContext,
                  String clientId,
                  ConsumerCoordinator coordinator,
                  Deserializer<K> keyDeserializer,
                  Deserializer<V> valueDeserializer,
                  Fetcher<K, V> fetcher,
                  ConsumerInterceptors<K, V> interceptors,
                  Time time,
                  ConsumerNetworkClient client,
                  Metrics metrics,
                  SubscriptionState subscriptions,
                  ConsumerMetadata metadata,
                  long retryBackoffMs,
                  long requestTimeoutMs,
                  int defaultApiTimeoutMs,
                  List<ConsumerPartitionAssignor> assignors,
                  String groupId) {
        this.log = logContext.logger(getClass());
        this.clientId = clientId;
        this.coordinator = coordinator;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.fetcher = fetcher;
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.interceptors = Objects.requireNonNull(interceptors);
        this.time = time;
        this.client = client;
        this.metrics = metrics;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.assignors = assignors;
        this.groupId = Optional.ofNullable(groupId);
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, "consumer");
    }

    private static Metrics buildMetrics(ConsumerConfig config, Time time, String clientId) {
        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricsTags);
        List<MetricsReporter> reporters = config.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class, Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
        JmxReporter jmxReporter = new JmxReporter();
        jmxReporter.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)));
        reporters.add(jmxReporter);
        MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
        return new Metrics(metricConfig, reporters, time, metricsContext);
    }

    /**
     * Get the set of partitions currently assigned to this consumer. If subscription happened by directly assigning
     * partitions using {@link #assign(Collection)} then this will simply return the same partitions that
     * were assigned. If topic subscription was used, then this will give the set of topic partitions currently assigned
     * to the consumer (which may be none if the assignment hasn't happened yet, or the partitions are in the
     * process of getting reassigned).
     * <p>
     * 获取当前分配给此消费者的分区集。
     * 如果通过直接分配分区使用 {@link #assign(Collection)} 订阅，则此方法将简单地返回分配的相同分区。
     * 如果使用主题订阅，则此方法将返回当前分配给消费者的主题分区集（可能为空，如果分配尚未发生，或者分区正在重新分配过程中）。
     *
     * @return The set of partitions currently assigned to this consumer
     */
    public Set<TopicPartition> assignment() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
        } finally {
            release();
        }
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
     * <p>
     * 获取当前订阅的主题集。
     * 将返回最近一次调用 {@link #subscribe(Collection, ConsumerRebalanceListener)} 时使用的相同主题集，
     * 如果没有这样的调用，则返回一个空集。
     *
     * @return 当前订阅的主题集
     */
    public Set<String> subscription() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.subscription()));
        } finally {
            release();
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     * <p>
     * 订阅给定主题列表以获取动态分配的分区。
     * <b>主题订阅不是增量式的。此列表将替换当前分配（如果存在）。</b>
     * 注意，不能同时使用 topic subscription 和 partition assignment。
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * 如果给定的主题列表为空，则将其视为与 {@link #unsubscribe()} 相同。
     *
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if any one of the following events are triggered:
     * <ul>
     * <li>Number of partitions change for any of the subscribed topics
     * <li>A subscribed topic is created or deleted
     * <li>An existing member of the consumer group is shutdown or fails
     * <li>A new member is added to the consumer group
     * </ul>
     * 
     * <p>
     * 作为组管理的一部分，消费者将跟踪属于特定组的所有消费者的列表，
     * 如果触发以下任何事件之一，将触发重新平衡操作：
     * <ul>
     * <li>任何订阅主题的分区数量发生变化
     * <li>订阅的主题被创建或删除
     * <li>消费者组中的现有成员关闭或失败
     * <li>消费者组中新成员的添加
     * </ul>
     *
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that rebalances will only occur during an active call to {@link #poll(Duration)}, so callbacks will
     * also only be invoked during that time.
     * <p>
     * 当触发这些事件之一时，提供的监听器将首先被调用，以指示消费者的分配已被撤销，然后再次在接收到新分配时被调用。
     * 请注意，重新平衡仅在调用 {@link #poll(Duration)} 期间发生，因此回调也仅在此期间调用。
     *
     * The provided listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the partitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     * <p>
     * 提供的监听器将立即覆盖任何在之前调用 subscribe 时设置的监听器。
     * 然而，可以保证通过此接口撤销/分配的分区是从在此调用中订阅的主题派生的。
     * 请参阅 {@link ConsumerRebalanceListener} 了解更多详细信息。
     *
     * @param topics The list of topics to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If topics is null or contains null or empty elements, or if listener is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        acquireAndEnsureOpen();
        try {
            // 如果 group.id 为空，则抛出异常
            maybeThrowInvalidGroupIdException();
            if (topics == null)
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            if (topics.isEmpty()) {
                // treat subscribing to empty topic list as the same as unsubscribing
                // 将订阅空主题列表视为与取消订阅相同
                this.unsubscribe();
            } else {
                // 遍历主题列表，如果主题为空，则抛出异常
                for (String topic : topics) {
                    if (Utils.isBlank(topic))
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }
                // 如果没有配置分区分配器，则抛出异常
                throwIfNoAssignorsConfigured();
                // 清除未分配主题的缓冲数据
                fetcher.clearBufferedDataForUnassignedTopics(topics);
                log.info("Subscribed to topic(s): {}", Utils.join(topics, ", "));
                // 订阅主题，并更新元数据
                if (this.subscriptions.subscribe(new HashSet<>(topics), listener))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> It is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * This is a short-hand for {@link #subscribe(Collection, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param topics The list of topics to subscribe to
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Collection<String> topics) {
        // 传递一个空操作的 rebalance listener
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against all topics existing at the time of check.
     * This can be controlled through the {@code metadata.max.age.ms} configuration: by lowering
     * the max metadata age, the consumer will refresh metadata more often and check for matching topics.
     * <p>
     * 订阅与给定模式匹配的所有主题，以获取动态分配的分区。
     * 模式匹配将定期与所有存在的主题进行匹配。
     * 这可以通过 {@code metadata.max.age.ms} 配置进行控制：通过降低最大元数据年龄，消费者会更频繁地刷新元数据并检查匹配的主题。
     * <p>
     * See {@link #subscribe(Collection, ConsumerRebalanceListener)} for details on the
     * use of the {@link ConsumerRebalanceListener}. Generally rebalances are triggered when there
     * is a change to the topics matching the provided pattern and when consumer group membership changes.
     * Group rebalances only take place during an active call to {@link #poll(Duration)}.
     * <p>
     * 请参阅 {@link #subscribe(Collection, ConsumerRebalanceListener)} 了解有关 {@link ConsumerRebalanceListener} 使用的详细信息。
     * 一般情况下，重新平衡仅在匹配提供的模式的主题发生变化和消费者组成员发生变化时触发。
     * 仅在调用 {@link #poll(Duration)} 期间发生重新平衡。
     *
     * @param pattern Pattern to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If pattern or listener is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        // 如果 group.id 为空，则抛出异常
        maybeThrowInvalidGroupIdException();
        // 如果 pattern 为空，则抛出异常
        if (pattern == null || pattern.toString().equals(""))
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));

        acquireAndEnsureOpen();
        try {
            // 如果没有配置 partition asignor，则抛出异常
            throwIfNoAssignorsConfigured();
            log.info("Subscribed to pattern: '{}'", pattern);
            // subscribe pattern
            this.subscriptions.subscribe(pattern, listener);
            // 交给 group coordinator 来根据 pattern 订阅对应匹配的 topics
            this.coordinator.updatePatternSubscription(metadata.fetch());
            // 更新元数据
            this.metadata.requestUpdateForNewTopics();
        } finally {
            release();
        }
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against topics existing at the time of check.
     * <p>
     * This is a short-hand for {@link #subscribe(Pattern, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Pattern, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param pattern Pattern to subscribe to
     * @throws IllegalArgumentException If pattern is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Pattern pattern) {
        // 传递一个空操作的 rebalance listener
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)} or {@link #subscribe(Pattern)}.
     * This also clears any partitions directly assigned through {@link #assign(Collection)}.
     * <p>
     * 取消订阅当前通过 {@link #subscribe(Collection)} 或 {@link #subscribe(Pattern)} 订阅的主题。
     * 这也将清除任何通过 {@link #assign(Collection)} 直接分配的分区。
     *
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. rebalance callback errors)
     */
    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            // 清除未分配分区的缓冲数据
            fetcher.clearBufferedDataForUnassignedPartitions(Collections.emptySet());
            // 如果 coordinator 不为空，则离开组
            if (this.coordinator != null) {
                this.coordinator.onLeavePrepare();
                this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
            }
            // 取消订阅
            this.subscriptions.unsubscribe();
            log.info("Unsubscribed all topics or patterns and assigned partitions");
        } finally {
            release();
        }
    }

    /**
     * Manually assign a list of partitions to this consumer. This interface does not allow for incremental assignment
     * and will replace the previous assignment (if there is one).
     * <p>
     * 手动分配分区给 consumer。此接口不支持增量分配，并将替换之前的分配（如果存在）。
     * <p>
     * If the given list of topic partitions is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * 如果给定的主题分区列表为空，则将其视为与 {@link #unsubscribe()} 相同。
     * <p>
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(Collection)}
     * and group assignment with {@link #subscribe(Collection, ConsumerRebalanceListener)}.
     * <p>
     * 通过此方法手动分配分区不会使用 consumer 的组管理功能。
     * 因此，当组成员资格或集群和主题元数据变化时不会触发重新平衡操作。
     * 注意，不能同时使用手动分配分区与组管理。
     * <p>
     * If auto-commit is enabled, an async commit (based on the old assignment) will be triggered before the new
     * assignment replaces the old one.
     * <p>
     * 如果 auto-commit 已启用，则在新的分配替换旧的分配之前，将触发基于旧分配的异步提交。
     * <p>
     *
     * @param partitions The list of partitions to assign this consumer
     *                   分配给此消费者的分区列表
     * @throws IllegalArgumentException If partitions is null or contains null or empty topics
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics or pattern
     *                               (without a subsequent call to {@link #unsubscribe()})
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
                // 如果分区为空，则取消订阅
            } else if (partitions.isEmpty()) {
                this.unsubscribe();
            } else {
                // 校验每个分区数据d正确性
                for (TopicPartition tp : partitions) {
                    String topic = (tp != null) ? tp.topic() : null;
                    if (Utils.isBlank(topic))
                        throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                }
                fetcher.clearBufferedDataForUnassignedPartitions(partitions);

                // make sure the offsets of topic partitions the consumer is unsubscribing from
                // are committed since there will be no following rebalance
                // 确保消费者取消订阅的主题分区的偏移量已提交，因为接下来不会有 rebalance
                if (coordinator != null)
                    this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

                log.info("Subscribed to partition(s): {}", Utils.join(partitions, ", "));
                if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
        }
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * 使用一个订阅/分配 API 指定主题或分区。
     * 在 poll 数据之前，不能未订阅任何主题或分区。
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     * <p>
     * 每次 poll，consumer 将尝试使用上次消费的偏移量作为起始偏移量并顺序获取。
     * 上次消费的偏移量可以通过 {@link #seek(TopicPartition, long)} 手动设置，也可以自动设置为订阅列表中分区的最后提交偏移量。
     *
     * @param timeoutMs The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     *            If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
     *            Must not be negative.
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     *
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *             partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs, or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException if the timeout value is negative
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     *
     * @deprecated Since 2.0. Use {@link #poll(Duration)}, which does not block beyond the timeout awaiting partition
     *             assignment. See <a href="https://cwiki.apache.org/confluence/x/5kiHB">KIP-266</a> for more information.
     */
    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return poll(time.timer(timeoutMs), false);
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     *
     * <p>
     * This method returns immediately if there are records available. Otherwise, it will await the passed timeout.
     * If the timeout expires, an empty record set will be returned. Note that this method may block beyond the
     * timeout in order to execute custom {@link ConsumerRebalanceListener} callbacks.
     * <p>
     *     如果有记录可用，则此方法立即返回。否则，它将等待传递的超时。
     *     如果超时到期，将返回一个空记录集。
     *     请注意，为了执行自定义 {@link ConsumerRebalanceListener} 回调，此方法可能会阻塞超过超时时间。
     *
     *
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
     *
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     *
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *             partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs, your rebalance callback thrown exceptions,
     *             or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException if the timeout value is negative
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from
     * @throws java.lang.ArithmeticException if the timeout is greater than {@link Long#MAX_VALUE} milliseconds.
     * @throws org.apache.kafka.common.errors.InvalidTopicException if the current subscription contains any invalid
     *             topic (per {@link org.apache.kafka.common.internals.Topic#validate(String)})
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return poll(time.timer(timeout), true);
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
        acquireAndEnsureOpen();
        try {
            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            // 如果此时没有订阅任何主题或分区，则抛出异常
            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            do {
                client.maybeTriggerWakeup();

                if (includeMetadataInTimeout) {
                    // try to update assignment metadata BUT do not need to block on the timer for join group
                    // 尝试更新分配元数据，但不需要在计时器上阻塞以加入组
                    updateAssignmentMetadataIfNeeded(timer, false);
                } else {
                    while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE), true)) {
                        log.warn("Still waiting for metadata");
                    }
                }

                // 从 Kafka 中拉取数据
                final Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollForFetches(timer);
                if (!records.isEmpty()) {
                    // before returning the fetched records, we can send off the next round of fetches
                    // and avoid block waiting for their responses to enable pipelining while the user
                    // is handling the fetched records.
                    // 在返回获取的记录之前，我们可以发送下一轮获取，避免在用户处理获取的记录时阻塞

                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched records.
                    // 注意：在返回获取的记录之前，我们不能允许任何唤醒或其他错误被触发。
                    if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
                        client.transmitSends();
                    }

                    // 触发拦截器的 onConsume 方法
                    return this.interceptors.onConsume(new ConsumerRecords<>(records));
                }
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            release();
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
        }
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
        // 如果 coordinator 不为空，则调用 poll 方法
        if (coordinator != null &&
                !coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }

        // 更新 fetch 位置
        return updateFetchPositions(timer);
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    /**
     * 从 Kafka 中拉取数据
     * 
     * @param timer 用于控制拉取超时的计时器
     * @return 拉取到的记录，按主题分区组织
     */
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollForFetches(Timer timer) {
        // 计算拉取超时时间
        long pollTimeout = coordinator == null ? timer.remainingMs() :
                Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());

        // if data is available already, return it immediately
        // 如果数据已经可用，则立即返回
        final Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();
        if (!records.isEmpty()) {
            return records;
        }

        // send any new fetches (won't resend pending fetches)
        // 如果数据已经可用，则立即返回
        fetcher.sendFetches();

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHashAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        // 我们不希望在 poll 中阻塞，如果我们缺少一些位置，因为偏移查找可能在失败后退避
        // 注意：cachedSubscriptionHashAllFetchPositions 的使用意味着我们必须在此方法之前调用 updateAssignmentMetadataIfNeeded
        if (!cachedSubscriptionHashAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        // 创建一个新的计时器用于拉取操作
        Timer pollTimer = time.timer(pollTimeout);
        client.poll(pollTimer, () -> {
            // since a fetch might be completed by the background thread, we need this poll condition
            // to ensure that we do not block unnecessarily in poll()
            // 由于拉取可能由后台线程完成，我们需要这个轮询条件来确保我们不会在 poll() 中不必要地阻塞
            return !fetcher.hasAvailableFetches();
        });
        timer.update(pollTimer.currentTimeMs());

        // 返回拉取到的记录
        return fetcher.fetchedRecords();
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * 此方法提交最后一次 {@link #poll(Duration) poll()} 返回的偏移量，用于所有订阅的主题和分区。
     * 
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * 提交的偏移量仅提交到 Kafka。
     * 使用此 API 提交的偏移量将在每次重新平衡后和启动时使用。
     * 因此，如果需要将偏移量存储在 Kafka 之外，则不应使用此 API。
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     * <p>
     * 这是一个同步提交，会阻塞直到提交成功，或者遇到不可恢复的错误（在这种情况下，会抛出错误给调用者），
     * 或者指定的超时时间 {@code default.api.timeout.ms} 过期（在这种情况下，会抛出 {@link org.apache.kafka.common.errors.TimeoutException} 给调用者）。
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     * <p>
     * 注意：使用 {@link #commitAsync(OffsetCommitCallback)} (或类似) 发送的异步偏移提交保证在调用此方法之前会调用其回调。
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This fatal error can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout specified by {@code default.api.timeout.ms} expires
     *            before successful completion of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync() {
        // 默认超时时间为 default.api.timeout.ms
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the passed timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(Duration timeout) {
        // 提交所有已消费的偏移量
        commitSync(subscriptions.allConsumed(), timeout);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1. If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call, so when you retry committing
     *            you should consider updating the passed in {@code offset} parameter.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1. If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @param timeout The maximum amount of time to await completion of the offset commit
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call, so when you retry committing
     *            you should consider updating the passed in {@code offset} parameter.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            // 如果没有配置 group.id，则抛出异常
            maybeThrowInvalidGroupIdException();
            // 如果 offsets 中有更新的 epoch 值，则触发 metadata 的更新
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            // 调用 coordinator 的 commitOffsetsSync 方法提交偏移量
            if (!coordinator.commitOffsetsSync(new HashMap<>(offsets), time.timer(timeout))) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before successfully " +
                        "committing offsets " + offsets);
            }
        } finally {
            release();
        }
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration)} for all the subscribed list of topics and partition.
     * <p>
     * 提交最后一次 {@link #poll(Duration)} 消费的偏移量
     * <p>
     * 与 {@link #commitAsync(OffsetCommitCallback) commitAsync(null)} 相同
     * 
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     * <p>
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same order as
     * the invocations. Corresponding commit callbacks are also invoked in the same order. Additionally note that
     * offsets committed through this API are guaranteed to complete before a subsequent call to {@link #commitSync()}
     * (and variants) returns.
     *
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1. If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     * <p>
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same order as
     * the invocations. Corresponding commit callbacks are also invoked in the same order. Additionally note that
     * offsets committed through this API are guaranteed to complete before a subsequent call to {@link #commitSync()}
     * (and variants) returns.
     *
     * @param offsets A map of offsets by partition with associate metadata. This map will be copied internally, so it
     *                is safe to mutate the map after returning.
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            // 如果没有配置 group.id，则抛出异常
            maybeThrowInvalidGroupIdException();
            log.debug("Committing offsets: {}", offsets);
            // 如果 offsets 中有更新的 epoch 值，则触发 metadata 的更新
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            // 调用 coordinator 的 commitOffsetsAsync 方法提交偏移量
            coordinator.commitOffsetsAsync(new HashMap<>(offsets), callback);
        } finally {
            release();
        }
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(Duration) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
     * <p>
     * 重写 consumer 将会在下次 {@link #poll(Duration) poll(timeout)} 使用的 fetch 偏移量。
     * 如果这个 API 被同一个分区多次调用，那么下一个 poll 将使用最新的偏移量。
     * 注意，如果这个 API 在消费过程中任意使用，以重置 fetch 偏移量，可能会丢失数据
     *
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        // 如果偏移量小于 0，则抛出异常
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        acquireAndEnsureOpen();
        try {
            log.info("Seeking to offset {} for partition {}", offset, partition);
            // 创建一个新的 fetch 位置对象
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offset,
                    Optional.empty(), // This will ensure we skip validation
                    this.metadata.currentLeader(partition));
            // 使用新的 fetch 位置更新订阅状态
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(Duration) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets. This
     * method allows for setting the leaderEpoch along with the desired offset.
     *
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     */
    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        long offset = offsetAndMetadata.offset();
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        acquireAndEnsureOpen();
        try {
            if (offsetAndMetadata.leaderEpoch().isPresent()) {
                log.info("Seeking to offset {} for partition {} with epoch {}",
                        offset, partition, offsetAndMetadata.leaderEpoch().get());
            } else {
                log.info("Seeking to offset {} for partition {}", offset, partition);
            }
            // 从 metadata 中获取 partition 最新的 leader 和 epoch
            Metadata.LeaderAndEpoch currentLeaderAndEpoch = this.metadata.currentLeader(partition);
            // 创建一个新的 fetch 位置对象
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offsetAndMetadata.offset(),
                    offsetAndMetadata.leaderEpoch(),
                    currentLeaderAndEpoch);
            // 更新订阅状态中维护的信息
            this.updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }

    /**
     * Seek to the first offset for each of the given partitions. This function evaluates lazily, seeking to the
     * first offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the first offset for all of the currently assigned partitions.
     * <p>
     * 定位到每个给定分区的第一个偏移量。
     * 这个函数是懒惰的，只有在调用 {@link #poll(Duration)} 或 {@link #position(TopicPartition)} 时才会真正定位到第一个偏移量。
     * 如果未提供分区，则定位到所有当前分配的分区的第一个偏移量。
     *
     * @throws IllegalArgumentException if {@code partitions} is {@code null}
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            // 如果没有提供分区，则使用当前分配的分区
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            // 请求重置偏移量到最早的偏移量
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.EARLIEST);
        } finally {
            release();
        }
    }

    /**
     * Seek to the last offset for each of the given partitions. This function evaluates lazily, seeking to the
     * final offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the final offset for all of the currently assigned partitions.
     * <p>
     * If {@code isolation.level=read_committed}, the end offset will be the Last Stable Offset, i.e., the offset
     * of the first message with an open transaction.
     *
     * @throws IllegalArgumentException if {@code partitions} is {@code null}
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.LATEST);
        } finally {
            release();
        }
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * This method may issue a remote call to the server if there is no current position for the given partition.
     * <p>
     * 获取给定分区的下一个记录的偏移量（如果该偏移量存在）。
     * 这个方法可能会向服务器发起远程调用，如果没有当前的分区位置。
     * <p>
     * This call will block until either the position could be determined or an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     * <p>
     * 这个调用会一直阻塞，直到可以确定位置，或者遇到不可恢复的错误（在这种情况下，会抛出异常），
     * 或者指定的超时时间（通过 {@code default.api.timeout.ms}）过期（在这种情况下，
     * 会抛出 {@link org.apache.kafka.common.errors.TimeoutException} 异常）。
     *
     * @param partition The partition to get the position for
     * @return The current position of the consumer (that is, the offset of the next record to be fetched)
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
     *             the partition
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the position cannot be determined before the
     *             timeout specified by {@code default.api.timeout.ms} expires
     */
    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * This method may issue a remote call to the server if there is no current position
     * for the given partition.
     * <p>
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param partition The partition to get the position for
     * @param timeout The maximum amount of time to await determination of the current position
     * @return The current position of the consumer (that is, the offset of the next record to be fetched)
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
     *             the partition
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the position cannot be determined before the
     *             passed timeout expires
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            // 如果给定的分区没有被分配给这个 consumer，则抛出异常
            if (!this.subscriptions.isAssigned(partition))
                throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

            Timer timer = time.timer(timeout);
            do {
                // 获取订阅状态中给定分区的 fetch 位置
                SubscriptionState.FetchPosition position = this.subscriptions.validPosition(partition);
                // 如果 fetch 位置不为空，则返回该位置的偏移量
                if (position != null)
                    return position.offset;

                // 更新 fetch 位置
                updateFetchPositions(timer);
                // 执行 poll 操作
                client.poll(timer);
            } while (timer.notExpired());

            // 如果超时，则抛出异常
            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                    "for partition " + partition + " could be determined");
        } finally {
            release();
        }
    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call will do a remote call to get the latest committed offset from the server, and will block until the
     * committed offset is gotten successfully, an unrecoverable error is encountered (in which case it is thrown to
     * the caller), or the timeout specified by {@code default.api.timeout.ms} expires (in which case a
     * {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partition The partition to check
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             the timeout specified by {@code default.api.timeout.ms} expires.
     *
     * @deprecated since 2.4 Use {@link #committed(Set)} instead
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return committed(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param partition The partition to check
     * @param timeout  The maximum amount of time to await the current committed offset
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             expiration of the timeout
     *
     * @deprecated since 2.4 Use {@link #committed(Set, Duration)} instead
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, final Duration timeout) {
        return committed(Collections.singleton(partition), timeout).get(partition);
    }

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this process or
     * another). The returned offsets will be used as the position for the consumer in the event of a failure.
     * <p>
     * If any of the partitions requested do not exist, an exception would be thrown.
     * <p>
     * This call will do a remote call to get the latest committed offsets from the server, and will block until the
     * committed offsets are gotten successfully, an unrecoverable error is encountered (in which case it is thrown to
     * the caller), or the timeout specified by {@code default.api.timeout.ms} expires (in which case a
     * {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partitions The partitions to check
     * @return The latest committed offsets for the given partitions; {@code null} will be returned for the
     *         partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             the timeout specified by {@code default.api.timeout.ms} expires.
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this process or
     * another). The returned offsets will be used as the position for the consumer in the event of a failure.
     * <p>
     * If any of the partitions requested do not exist, an exception would be thrown.
     * <p>
     * This call will block to do a remote call to get the latest committed offsets from the server.
     *
     * @param partitions The partitions to check
     * @param timeout  The maximum amount of time to await the latest committed offsets
     * @return The latest committed offsets for the given partitions; {@code null} will be returned for the
     *         partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             expiration of the timeout
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            Map<TopicPartition, OffsetAndMetadata> offsets = coordinator.fetchCommittedOffsets(partitions, time.timer(timeout));
            if (offsets == null) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                    "committed offset for partitions " + partitions + " could be determined. Try tuning default.api.timeout.ms " +
                    "larger to relax the threshold.");
            } else {
                offsets.forEach(this::updateLastSeenEpochIfNewer);
                return offsets;
            }
        } finally {
            release();
        }
    }

    /**
     * Get the metrics kept by the consumer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     *
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     * @param timeout The maximum of time to await topic metadata
     *
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the specified topic. See
     *             the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if topic metadata cannot be fetched before expiration
     *             of the passed timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (!parts.isEmpty())
                return parts;

            Timer timer = time.timer(timeout);
            Map<String, List<PartitionInfo>> topicMetadata = fetcher.getTopicMetadata(
                    new MetadataRequest.Builder(Collections.singletonList(topic), metadata.allowAutoTopicCreation()), timer);
            return topicMetadata.getOrDefault(topic, Collections.emptyList());
        } finally {
            release();
        }
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.

     * @return The map of topics and its partitions
     *
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.
     *
     * @param timeout The maximum time this operation will block to fetch topic metadata
     *
     * @return The map of topics and its partitions
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be fetched before
     *             expiration of the passed timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return fetcher.getAllTopicMetadata(time.timer(timeout));
        } finally {
            release();
        }
    }

    /**
     * Suspend fetching from the requested partitions. Future calls to {@link #poll(Duration)} will not return
     * any records from these partitions until they have been resumed using {@link #resume(Collection)}.
     * Note that this method does not affect partition subscription. In particular, it does not cause a group
     * rebalance when automatic assignment is used.
     * @param partitions The partitions which should be paused
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Pausing partitions {}", partitions);
            for (TopicPartition partition: partitions) {
                subscriptions.pause(partition);
            }
        } finally {
            release();
        }
    }

    /**
     * Resume specified partitions which have been paused with {@link #pause(Collection)}. New calls to
     * {@link #poll(Duration)} will return records from these partitions if there are any to be fetched.
     * If the partitions were not previously paused, this method is a no-op.
     * @param partitions The partitions which should be resumed
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Resuming partitions {}", partitions);
            for (TopicPartition partition: partitions) {
                subscriptions.resume(partition);
            }
        } finally {
            release();
        }
    }

    /**
     * Get the set of partitions that were previously paused by a call to {@link #pause(Collection)}.
     *
     * @return The set of paused partitions
     */
    @Override
    public Set<TopicPartition> paused() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            release();
        }
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     *
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     *         than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     *         such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws IllegalArgumentException if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not support looking up
     *         the offsets by timestamp
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     * @param timeout The maximum amount of time to await retrieval of the offsets
     *
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     *         than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     *         such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws IllegalArgumentException if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         expiration of the passed timeout
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not support looking up
     *         the offsets by timestamp
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
                // we explicitly exclude the earliest and latest offset here so the timestamp in the returned
                // OffsetAndTimestamp is always positive.
                if (entry.getValue() < 0)
                    throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                            entry.getValue() + ". The target time cannot be negative.");
            }
            return fetcher.offsetsForTimes(timestampsToSearch, time.timer(timeout));
        } finally {
            release();
        }
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToBeginning(Collection)
     *
     * @param partitions the partitions to get the earliest offsets.
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         expiration of the configured {@code default.api.timeout.ms}
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToBeginning(Collection)
     *
     * @param partitions the partitions to get the earliest offsets
     * @param timeout The maximum amount of time to await retrieval of the beginning offsets
     *
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         expiration of the passed timeout
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return fetcher.beginningOffsets(partitions, time.timer(timeout));
        } finally {
            release();
        }
    }

    /**
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToEnd(Collection)
     *
     * @param partitions the partitions to get the end offsets.
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToEnd(Collection)
     *
     * @param partitions the partitions to get the end offsets.
     * @param timeout The maximum amount of time to await retrieval of the end offsets
     *
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offsets could not be fetched before
     *         expiration of the passed timeout
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return fetcher.endOffsets(partitions, time.timer(timeout));
        } finally {
            release();
        }
    }

    /**
     * Get the consumer's current lag on the partition. Returns an "empty" {@link OptionalLong} if the lag is not known,
     * for example if there is no position yet, or if the end offset is not known yet.
     *
     * <p>
     * This method uses locally cached metadata and never makes a remote call.
     *
     * @param topicPartition The partition to get the lag for.
     *
     * @return This {@code Consumer} instance's current lag for the given partition.
     *
     * @throws IllegalStateException if the {@code topicPartition} is not assigned
     **/
    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        acquireAndEnsureOpen();
        try {
            final Long lag = subscriptions.partitionLag(topicPartition, isolationLevel);

            // if the log end offset is not known and hence cannot return lag and there is
            // no in-flight list offset requested yet,
            // issue a list offset request for that partition so that next time
            // we may get the answer; we do not need to wait for the return value
            // since we would not try to poll the network client synchronously
            if (lag == null) {
                if (subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null &&
                    !subscriptions.partitionEndOffsetRequested(topicPartition)) {
                    log.info("Requesting the log end offset for {} in order to compute lag", topicPartition);
                    subscriptions.requestPartitionEndOffset(topicPartition);
                    fetcher.endOffsets(Collections.singleton(topicPartition), time.timer(0L));
                }

                return OptionalLong.empty();
            }

            return OptionalLong.of(lag);
        } finally {
            release();
        }
    }

    /**
     * Return the current group metadata associated with this consumer.
     *
     * @return consumer group metadata
     * @throws org.apache.kafka.common.errors.InvalidGroupIdException if consumer does not have a group
     */
    @Override
    public ConsumerGroupMetadata groupMetadata() {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            return coordinator.groupMetadata();
        } finally {
            release();
        }
    }

    /**
     * Alert the consumer to trigger a new rebalance by rejoining the group. This is a nonblocking call that forces
     * the consumer to trigger a new rebalance on the next {@link #poll(Duration)} call. Note that this API does not
     * itself initiate the rebalance, so you must still call {@link #poll(Duration)}. If a rebalance is already in
     * progress this call will be a no-op. If you wish to force an additional rebalance you must complete the current
     * one by calling poll before retrying this API.
     * <p>
     * You do not need to call this during normal processing, as the consumer group will manage itself
     * automatically and rebalance when necessary. However there may be situations where the application wishes to
     * trigger a rebalance that would otherwise not occur. For example, if some condition external and invisible to
     * the Consumer and its group changes in a way that would affect the userdata encoded in the
     * {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription Subscription}, the Consumer
     * will not be notified and no rebalance will occur. This API can be used to force the group to rebalance so that
     * the assignor can perform a partition reassignment based on the latest userdata. If your assignor does not use
     * this userdata, or you do not use a custom
     * {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor ConsumerPartitionAssignor}, you should not
     * use this API.
     *
     * @throws java.lang.IllegalStateException if the consumer does not use group subscription
     */
    @Override
    public void enforceRebalance() {
        acquireAndEnsureOpen();
        try {
            if (coordinator == null) {
                throw new IllegalStateException("Tried to force a rebalance but consumer does not have a group.");
            }
            coordinator.requestRejoin("rebalance enforced by user");
        } finally {
            release();
        }
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * If auto-commit is enabled, this will commit the current offsets if possible within the default
     * timeout. See {@link #close(Duration)} for details. Note that {@link #wakeup()}
     * cannot be used to interrupt close.
     *
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted
     *             before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * {@code timeout} for the consumer to complete pending commits and leave the group.
     * If auto-commit is enabled, this will commit the current offsets if possible within the
     * timeout. If the consumer is unable to complete offset commits and gracefully leave the group
     * before the timeout expires, the consumer is force closed. Note that {@link #wakeup()} cannot be
     * used to interrupt close.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     *
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws InterruptException If the thread is interrupted before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    @Override
    public void close(Duration timeout) {
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        acquire();
        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout.toMillis(), false);
            }
        } finally {
            closed = true;
            release();
        }
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link org.apache.kafka.common.errors.WakeupException}.
     * If no thread is blocking in a method which can throw {@link org.apache.kafka.common.errors.WakeupException}, the next call to such a method will raise it instead.
     */
    @Override
    public void wakeup() {
        this.client.wakeup();
    }

    private ClusterResourceListeners configureClusterResourceListeners(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keyDeserializer);
        clusterResourceListeners.maybeAdd(valueDeserializer);
        return clusterResourceListeners;
    }

    private void close(long timeoutMs, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        try {
            if (coordinator != null)
                coordinator.close(time.timer(Math.min(timeoutMs, requestTimeoutMs)));
        } catch (Throwable t) {
            firstException.compareAndSet(null, t);
            log.error("Failed to close coordinator", t);
        }
        Utils.closeQuietly(fetcher, "fetcher", firstException);
        Utils.closeQuietly(interceptors, "consumer interceptors", firstException);
        Utils.closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        Utils.closeQuietly(metrics, "consumer metrics", firstException);
        Utils.closeQuietly(client, "consumer network client", firstException);
        Utils.closeQuietly(keyDeserializer, "consumer key deserializer", firstException);
        Utils.closeQuietly(valueDeserializer, "consumer value deserializer", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    /**
     * Set the fetch position to the committed position (if there is one)
     * or reset it using the offset reset policy the user has configured.
     * <p>
     * 将 fetch 位置设置为已提交的偏移量（如果有）或使用用户配置的 reset strategy 重置它。
     *
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *             defined
     *
     * @return true iff the operation completed without timing out
     */
    private boolean updateFetchPositions(final Timer timer) {
        // If any partitions have been truncated due to a leader change, we need to validate the offsets
        // 如果任何分区由于领导者的变化而被截断，我们需要验证偏移量
        fetcher.validateOffsetsIfNeeded();

        // 确认此时是否知晓了所有订阅分区的 fetch 位置
        cachedSubscriptionHashAllFetchPositions = subscriptions.hasAllFetchPositions();
        if (cachedSubscriptionHashAllFetchPositions) {
            return true;
        }

        // If there are any partitions which do not have a valid position and are not
        // awaiting reset, then we need to fetch committed offsets. We will only do a
        // coordinator lookup if there are partitions which have missing positions, so
        // a consumer with manually assigned partitions can avoid a coordinator dependence
        // by always ensuring that assigned partitions have an initial position.
        // 如果有任何一个分区没有有效的 position，并且没有在等待 reset，那么我们需要获取已提交的 offsets。
        // 我们只会进行 coordinator lookup，如果存在具有缺失 position 的分区，
        // 则使用手动分配分区的消费者可以避免 coordinator 依赖。
        if (coordinator != null &&
                !coordinator.refreshCommittedOffsetsIfNeeded(timer)) {
            return false;
        }

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise an exception.
        // 如果此时仍有分区需要 position，并且定义了 reset 策略，则使用默认策略请求 reset。
        // 如果没有定义 reset 策略，并且有分区缺少 position，则会引发异常。
        subscriptions.resetInitializingPositions();

        // Finally send an asynchronous request to lookup and update the positions of any
        // partitions which are awaiting reset.
        // 最终，发送一个异步请求来查找和更新任何正在等待 reset 的分区的位置。
        fetcher.resetOffsetsIfNeeded();

        return true;
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     * <p>
     * 获取轻量锁并确保消费者未关闭。
     * 
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        // 获取轻量锁
        acquire();
        // 如果消费者已关闭，释放锁并抛出异常
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this consumer from multi-threaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multi-threaded usage is not
     * supported).
     * <p>
     * 获取轻量锁保护此消费者免受多线程访问。
     * 当锁不可用时，不会阻塞，而是抛出异常（因为不支持多线程访问）。
     * 
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        long threadId = Thread.currentThread().getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
        refcount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    private void throwIfNoAssignorsConfigured() {
        if (assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent())
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
    }

    private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null)
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
    }

    // Functions below are for testing only
    String getClientId() {
        return clientId;
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return updateAssignmentMetadataIfNeeded(timer, true);
    }
}
