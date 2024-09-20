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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A Kafka client that publishes records to the Kafka cluster.
 * 一个向 Kafka 集群发布记录的 Kafka 客户端。
 * <p>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * 生产者是<i>线程安全的</i>，在多个线程之间共享单个生产者实例通常比拥有多个实例更快。
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * 以下是使用生产者发送包含顺序数字作为键/值对的字符串记录的简单示例。
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("linger.ms", 1);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * 生产者由一个缓冲空间池组成，用于保存尚未传输到服务器的记录，以及一个负责将这些记录转换为请求并将其传输到集群的后台 I/O 线程。
 * 使用后未能关闭生产者将导致这些资源泄漏。
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * {@link #send(ProducerRecord) send()} 方法是异步的。调用时，它会将记录添加到待发送记录的缓冲区中并立即返回。
 * 这允许生产者将单个记录批量处理以提高效率。
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The "all" setting
 * we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <code>acks</code> 配置控制请求被视为完成的标准。我们指定的 "all" 设置将导致在记录完全提交时阻塞，这是最慢但最持久的设置。
 * <p>
 * If the request fails, the producer can automatically retry, though since we have specified <code>retries</code>
 * as 0 it won't. Enabling retries also opens up the possibility of duplicates (see the documentation on
 * <a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a> for details).
 * 如果请求失败，生产者可以自动重试，尽管由于我们将 <code>retries</code> 指定为 0，它不会重试。
 * 启用重试也会引入重复的可能性（有关详细信息，请参阅<a href="http://kafka.apache.org/documentation.html#semantics">消息传递语义</a>文档）。
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * 生产者为每个分区维护未发送记录的缓冲区。这些缓冲区的大小由 <code>batch.size</code> 配置指定。
 * 增大此值可以导致更多的批处理，但需要更多的内存（因为我们通常会为每个活动分区保留一个这样的缓冲区）。
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * 默认情况下，即使缓冲区中有额外的未使用空间，缓冲区也可以立即发送。但是，如果您想减少请求数量，可以将 <code>linger.ms</code> 设置为大于 0 的值。
 * 这将指示生产者在发送请求之前等待最多该毫秒数，以期望更多记录到达以填充同一批次。这类似于 TCP 中的 Nagle 算法。
 * 例如，在上面的代码片段中，由于我���将等待时间设置为 1 毫秒，所有 100 条记录可能会在单个请求中发送。
 * 然而，如果我们没有填满缓冲区，这个设置会为我们的请求增加 1 毫秒的延迟，等待更多记录到达。
 * 请注意，即使在 <code>linger.ms=0</code> 的情况下，时间上接近的记录通常也会批量处理在一起，因此在重负载下，无论延迟配置如何，都会发生批处理；
 * 但是，将其设置为大于 0 的值可以在不处于最大负载时导致更少、更高效的请求，代价是少量延迟。
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 * <code>buffer.memory</code> 控制生产者可用于缓冲的总内存量。如果记录的发送速度快于它们可以传输到服务器的速度，则此缓冲空间将耗尽。
 * 当缓冲空间耗尽时，额外的发送调用将被阻塞。阻塞时间的阈值由 <code>max.block.ms</code> 决定，超过该时间将抛出 TimeoutException。
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple string or byte types.
 * <code>key.serializer</code> 和 <code>value.serializer</code> 指示如何将用户在其 <code>ProducerRecord</code> 中提供的键和值对象转换为字节。
 * 对于简单的字符串或字节类型，您可以使用包含的 {@link org.apache.kafka.common.serialization.ByteArraySerializer} 或 
 * {@link org.apache.kafka.common.serialization.StringSerializer}。
 * <p>
 * From Kafka 0.11, the KafkaProducer supports two additional modes: the idempotent producer and the transactional producer.
 * The idempotent producer strengthens Kafka's delivery semantics from at least once to exactly once delivery. In particular
 * producer retries will no longer introduce duplicates. The transactional producer allows an application to send messages
 * to multiple partitions (and topics!) atomically.
 * 从 Kafka 0.11 开始，KafkaProducer 支持两种额外模式：幂等生产者和事务性生产者。
 * 幂等生产者将 Kafka 的传递语义从至少一次传递强化为精确一次传递。特别是，生产者重试将不再引入重复。
 * 事务性生产者允许应用程序以原子方式向多个分区（和主题！）发送消息。
 * </p>
 * <p>
 * To enable idempotence, the <code>enable.idempotence</code> configuration must be set to true. If set, the
 * <code>retries</code> config will default to <code>Integer.MAX_VALUE</code> and the <code>acks</code> config will
 * default to <code>all</code>. There are no API changes for the idempotent producer, so existing applications will
 * not need to be modified to take advantage of this feature.
 * 要启用幂等性，必须将 <code>enable.idempotence</code> 配置设置为 true。如果设置，<code>retries</code> 配置将默认为 <code>Integer.MAX_VALUE</code>，
 * 而 <code>acks</code> 配置将默认为 <code>all</code>。幂等生产者没有 API 更改，因此现有应用程序无需修改即可利用此功能。
 * </p>
 * <p>
 * To take advantage of the idempotent producer, it is imperative to avoid application level re-sends since these cannot
 * be de-duplicated. As such, if an application enables idempotence, it is recommended to leave the <code>retries</code>
 * config unset, as it will be defaulted to <code>Integer.MAX_VALUE</code>. Additionally, if a {@link #send(ProducerRecord)}
 * returns an error even with infinite retries (for instance if the message expires in the buffer before being sent),
 * then it is recommended to shut down the producer and check the contents of the last produced message to ensure that
 * it is not duplicated. Finally, the producer can only guarantee idempotence for messages sent within a single session.
 * 要利用幂等生产者，避免应用程序级重发至关重要，因为这些重发无法去重。因此，如果应用程序启用幂等性，建议不要设置 <code>retries</code> 配置，
 * 因为它将默认为 <code>Integer.MAX_VALUE</code>。此外，如果 {@link #send(ProducerRecord)} 即使在无限重试的情况下也返回错误
 * （例如，如果消息在发送之前在缓冲区中过期），则建议关闭生产者并检查最后生成的消息的内容，以确保它不会重复。
 * 最后，生产者只能保证单个会话内发送的消息的幂等性。
 * </p>
 * <p>To use the transactional producer and the attendant APIs, you must set the <code>transactional.id</code>
 * configuration property. If the <code>transactional.id</code> is set, idempotence is automatically enabled along with
 * the producer configs which idempotence depends on. Further, topics which are included in transactions should be configured
 * for durability. In particular, the <code>replication.factor</code> should be at least <code>3</code>, and the
 * <code>min.insync.replicas</code> for these topics should be set to 2. Finally, in order for transactional guarantees
 * to be realized from end-to-end, the consumers must be configured to read only committed messages as well.
 * 要使用事务性生产者和相关的 API，您必须设置 <code>transactional.id</code> 配置属性。如果设置了 <code>transactional.id</code>，
 * 幂等性将自动启用，同时还会启用幂等性所依赖的生产者配置。此外，包含在事务中的主题应配置为持久性。
 * 特别是，<code>replication.factor</code> 应至少为 <code>3</code>，这些主题的 <code>min.insync.replicas</code> 应设置为 2。
 * 最后，为了实现端到端的事务保证，消费者也必须配置为仅读取已提交的消息。
 * </p>
 * <p>
 * The purpose of the <code>transactional.id</code> is to enable transaction recovery across multiple sessions of a
 * single producer instance. It would typically be derived from the shard identifier in a partitioned, stateful, application.
 * As such, it should be unique to each producer instance running within a partitioned application.
 * <code>transactional.id</code> 的目的是在单个生产者实例的多个会话之间启用事务恢复。它通常来源于分区的、有状态的应用程序中的分片标识符。
 * 因此，它在分区应用程序中运行的每个生产者实例应该是唯一的。
 * </p>
 * <p>All the new transactional APIs are blocking and will throw exceptions on failure. The example
 * below illustrates how the new APIs are meant to be used. It is similar to the example above, except that all
 * 100 messages are part of a single transaction.
 * 所有新的事务 API 都是阻塞的，并且在失败时会抛出异常。下面的示例说明了如何使用新的 API。它类似于上面的示例，
 * </p>
 * <p>
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("transactional.id", "my-transactional-id");
 * Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 * producer.initTransactions();
 *
 * try {
 *     producer.beginTransaction();
 *     for (int i = 0; i < 100; i++)
 *         producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *     producer.commitTransaction();
 * } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *     // We can't recover from these exceptions, so our only option is to close the producer and exit.
 *     producer.close();
 * } catch (KafkaException e) {
 *     // For all other exceptions, just abort the transaction and try again.
 *     producer.abortTransaction();
 * }
 * producer.close();
 * } </pre>
 * </p>
 * <p>
 * As is hinted at in the example, there can be only one open transaction per producer. All messages sent between the
 * {@link #beginTransaction()} and {@link #commitTransaction()} calls will be part of a single transaction. When the
 * <code>transactional.id</code> is specified, all messages sent by the producer must be part of a transaction.
 * 如示例所暗示的，每个生产者只能有一个打开的事务。在 {@link #beginTransaction()} 和 {@link #commitTransaction()} 调用之间发送的所有消息将成为单个事务的一部分。
 * 当指定了 <code>transactional.id</code> 时，生产者发送的所有消息必须是事务的一部分。
 * </p>
 * <p>
 * The transactional producer uses exceptions to communicate error states. In particular, it is not required
 * to specify callbacks for <code>producer.send()</code> or to call <code>.get()</code> on the returned Future: a
 * <code>KafkaException</code> would be thrown if any of the
 * <code>producer.send()</code> or transactional calls hit an irrecoverable error during a transaction. See the {@link #send(ProducerRecord)}
 * documentation for more details about detecting errors from a transactional send.
 * 事务性生产者使用异常来传达错误状态。特别是，不需要为 <code>producer.send()</code> 指定回调或在返回的 Future 上调用 <code>.get()</code>：
 * 如果在事务期间 <code>producer.send()</code> 或事务调用遇到不可恢复的错误，将抛出 <code>KafkaException</code>。
 * 有关从事务性发送中检测错误的更多详细信息，请参阅 {@link #send(ProducerRecord)} 文档。
 * </p>
 * </p>By calling
 * <code>producer.abortTransaction()</code> upon receiving a <code>KafkaException</code> we can ensure that any
 * successful writes are marked as aborted, hence keeping the transactional guarantees.
 * 通过在收到 <code>KafkaException</code> 时调用 <code>producer.abortTransaction()</code>，
 * 我们可以确保任何成功的写入都被标记为中止，从而保持事务保证。
 * </p>
 * <p>
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers may not support
 * certain client features.  For instance, the transactional APIs need broker versions 0.11.0 or later. You will receive an
 * <code>UnsupportedVersionException</code> when invoking an API that is not available in the running broker version.
 * 该客户端可以与版本 0.10.0 或更新版本的代理通信。较旧或较新的代理可能不支持某些客户端功能。
 * 例如，事务 API 需要 0.11.0 或更高版本的代理。当调用运行中的代理版本中不可用的 API 时，
 * 您将收到 <code>UnsupportedVersionException</code>。
 * </p>
 */
public class KafkaProducer<K, V> implements Producer<K, V> {
    /** 用于日志记录的Logger对象 */
    private final Logger log;
    /** JMX指标前缀 */
    private static final String JMX_PREFIX = "kafka.producer";
    /** 网络线程名称前缀 */
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    /** 生产者指标组名称 */
    public static final String PRODUCER_METRIC_GROUP_NAME = "producer-metrics";

    /** 客户端ID */
    private final String clientId;
    /** 指标收集对象，用于测试可见 */
    final Metrics metrics;
    /** 分区器，用于决定消息发送到哪个分区 */
    private final Partitioner partitioner;
    /** 最大请求大小（字节） */
    private final int maxRequestSize;
    /** 总内存大小（字节） */
    private final long totalMemorySize;
    /** 生产者元数据，包含主题分区信息 */
    private final ProducerMetadata metadata;
    /** 记录累加器，用于缓存待发送的消息 */
    private final RecordAccumulator accumulator;
    /** 发送器，负责将消息发送到Kafka集群 */
    private final Sender sender;
    /** IO线程，用于网络通信 */
    private final Thread ioThread;
    /** 压缩类型 */
    private final CompressionType compressionType;
    /** 错误传感器，用于监控错误 */
    private final Sensor errors;
    /** 时间对象，用于获取当前时间 */
    private final Time time;
    /** 键序列化器 */
    private final Serializer<K> keySerializer;
    /** 值序列化器 */
    private final Serializer<V> valueSerializer;
    /** 生产者配置对象 */
    private final ProducerConfig producerConfig;
    /** 最大阻塞时间（毫秒） */
    private final long maxBlockTimeMs;
    /** 生产者拦截器 */
    private final ProducerInterceptors<K, V> interceptors;
    /** API版本信息 */
    private final ApiVersions apiVersions;
    /** 事务管理器 */
    private final TransactionManager transactionManager;

    /*
    TODO
    构造函数解释:
    最底层的是：KafkaProducer(ProducerConfig config,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors<K, V> interceptors,
                  Time time)

    对外开放的有两种方法：
    1. 接收 map 参数
    2. 接收 properties 参数

    properties 会被转为 map；map 会被转为 ProducerConfig 对象，然后调用最底层的构造函数；
     */

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * 一个生产者通过提供一组键值对作为配置来实例化。有效的配置字符串文档在
     * <a href="http://kafka.apache.org/documentation.html#producerconfigs">这里</a>。
     * 值可以是字符串或适当类型的对象（例如，一个数字配置将接受字符串 "42" 或整数 42）。
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * 注意：创建 {@code KafkaProducer} 后，必须始终调用 {@link #close()} 以避免资源泄漏。
     * 
     * @param configs   The producer configs 生产者配置
     *
     */
    public KafkaProducer(final Map<String, Object> configs) {
        // 不指定 keySerializer 和 valueSerializer，使用 configs 中的配置
        this(configs, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * 一个生产者通过提供一组键值对作为配置，一个键和一个值 {@link Serializer} 来实例化。
     * 有效的配置字符串文档在 <a href="http://kafka.apache.org/documentation.html#producerconfigs">这里</a>。
     * 值可以是字符串或适当类型的对象（例如，一个数字配置将接受字符串 "42" 或整数 42）。
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * 注意：创建 {@code KafkaProducer} 后，必须始终调用 {@link #close()} 以避免资源泄漏。
     * 
     * @param configs   The producer configs 生产者配置
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     *                       实现 {@link Serializer} 的键序列化器。当序列化器通过直接传递时，生产者中不会调用 configure() 方法。
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     *                       实现 {@link Serializer} 的值序列化器。当序列化器通过直接传递时，生产者中不会调用 configure() 方法。
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        // 入参中的 keySerializer 和 valueSerializer 的优先级高于 configs 中的序列化器配置
        // 会将入参中的序列化器追加到 ProducerConfig 中
        // new ProducerConfig 会将所有的参数进行解析，然后统一存储管理
        this(new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)),
                keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * 一个生产者通过提供一组键值对作为配置来实例化。有效的配置字符串文档在
     * <a href="http://kafka.apache.org/documentation.html#producerconfigs">这里</a>。
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        // 不指定 keySerializer 和 valueSerializer，使用 properties 中的配置
        this(properties, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * 一个生产者通过提供一组键值对作为配置，一个键和一个值 {@link Serializer} 来实例化。
     * 有效的配置字符串文档在 <a href="http://kafka.apache.org/documentation.html#producerconfigs">这里</a>。
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * 
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        // 将 properties 转为 map，然后调用 KafkaProducer(Map<String, Object> configs) 构造函数
        this(Utils.propsToMap(properties), keySerializer, valueSerializer);
    }

    // 核心构造函数
    // visible for testing
    @SuppressWarnings("unchecked")
    KafkaProducer(ProducerConfig config,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors<K, V> interceptors,
                  Time time) {
        try {
            // 初始化基本配置，即在 producer 对象的内部成员对象中也持有一些对配置信息的引用
            this.producerConfig = config;
            this.time = time;
            this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);

            // 设置日志上下文
            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            LogContext logContext;
            if (transactionalId == null)
                logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            else
                logContext = new LogContext(String.format("[Producer clientId=%s, transactionalId=%s] ", clientId, transactionalId));
            log = logContext.logger(KafkaProducer.class);
            log.trace("Starting the Kafka producer");

            // 配置和初始化度量指标
            // 根据 producerConfig 中的配置项来初始化 MetricConfig 对象，即初始化监控指标的配置
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig()
                    .samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            // 根据 producerConfig 来初始化多个监控报告器
            // 这个 getConfiguredInstances 内部的实现就是先通过反射来实例化对象，然后如果这个对象实现了 Configurable 接口，就调用其 configure 方法
            List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class,
                    Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));
            // 创建 JMX 监控报告器
            JmxReporter jmxReporter = new JmxReporter();
            jmxReporter.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)));
            // 合并两种类型的监控报告器，MetricReporter 和 JmxReporter
            reporters.add(jmxReporter);
            // 初始化一个指标的上下文对象
            MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                    config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            // 基于这些指标配置、reporter 等封装一个门面 metrics 对象；其内部包含了所有指标、sensors 等等
            this.metrics = new Metrics(metricConfig, reporters, time, metricsContext);

            // 根据 producerConfig 来初始化分区器
            this.partitioner = config.getConfiguredInstance(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    Partitioner.class,
                    Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));

            // 配置重试退避时间，这个值在 ProducerConfig 内部会被 postProcessParsedConfig 方法进行清洗处理
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);

            // 配置键值序列化器，优先使用入参的 keySerializer 和 valueSerializer
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                                                                         Serializer.class);
                this.keySerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                                                           Serializer.class);
                this.valueSerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }

            // 根据 producerConfig 初始化拦截器
            // 然后将其配置到 producer 内部的成员变量中，但优先使用入参的 interceptors
            List<ProducerInterceptor<K, V>> interceptorList = (List) config.getConfiguredInstances(
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class,
                    Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));
            if (interceptors != null)
                this.interceptors = interceptors;
            else
                this.interceptors = new ProducerInterceptors<>(interceptorList);

            // 配置集群资源监听器
            // 注意：keySerializer 和 valueSerializer 是来自入参的值；interceptorList 和 reporters 是来自配置文件的值
            // 入参如果实现了 ClusterResourceListener 接口，那么会被 ClusterResourceListeners 管理，并且当集群资源发生变化时会被通知
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keySerializer,
                    valueSerializer, interceptorList, reporters);

            // 设置请求和内存相关的配置
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));

            this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            int deliveryTimeoutMs = configureDeliveryTimeout(config, log);

            // 初始化API版本和事务管理器
            this.apiVersions = new ApiVersions();
            this.transactionManager = configureTransactionState(config, logContext);

            // 重要：创建记录累加器
            this.accumulator = new RecordAccumulator(logContext,
                    config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                    this.compressionType,
                    lingerMs(config),
                    retryBackoffMs,
                    deliveryTimeoutMs,
                    metrics,
                    PRODUCER_METRIC_GROUP_NAME,
                    time,
                    apiVersions,
                    transactionManager,
                    new BufferPool(this.totalMemorySize, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG), metrics, time, PRODUCER_METRIC_GROUP_NAME));

            // 解析并验证 cluster 的地址
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG));
            // 初始化 producer 的元数据;
            // metadata 能用于 producer 线程和 sender 线程的通信
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                // 一般都会新建一个 ProducerMetadata 实例
                this.metadata = new ProducerMetadata(retryBackoffMs,
                        config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                        config.getLong(ProducerConfig.METADATA_MAX_IDLE_CONFIG),
                        logContext,
                        clusterResourceListeners,
                        Time.SYSTEM);
                this.metadata.bootstrap(addresses);
            }

            // 创建错误传感器
            // metrics 内部持有所有的 sensors；然后保证名称唯一；
            this.errors = this.metrics.sensor("errors");

            // 创建和启动发送器线程；然后会交给 ioThread 来启动
            this.sender = newSender(logContext, kafkaClient, this.metadata);
            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();

            // 记录未使用的配置项
            config.logUnused();
            // 注册信息，猜测是为了开启 JMX 监控
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // 如果初始化过程中出现异常，关闭已创建的资源并抛出异常
            close(Duration.ofMillis(0), true);
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    // visible for testing
    /**
     * 创建并返回一个新的Sender实例
     * 
     * @param logContext 日志上下文
     * @param kafkaClient Kafka客户端，如果为null则创建新的NetworkClient
     * @param metadata 生产者元数据
     * @return 新创建的Sender实例
     */
    Sender newSender(LogContext logContext, KafkaClient kafkaClient, ProducerMetadata metadata) {
        // 获取最大并发请求数
        int maxInflightRequests = producerConfig.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        // 获取请求超时时间
        int requestTimeoutMs = producerConfig.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        // 创建通道构建器
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(producerConfig, time, logContext);
        // 创建生产者指标注册表
        ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
        // 创建限流时间传感器
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
        
        // 创建或使用现有的 KafkaClient
        KafkaClient client = kafkaClient != null ? kafkaClient : new NetworkClient(
                new Selector(producerConfig.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                        this.metrics, time, "producer", channelBuilder, logContext),
                metadata,
                clientId,
                maxInflightRequests,
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                producerConfig.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                producerConfig.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                requestTimeoutMs,
                producerConfig.getLong(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                producerConfig.getLong(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                time,
                true,
                apiVersions,
                throttleTimeSensor,
                logContext);

        // 获取 acks 配置
        short acks = Short.parseShort(producerConfig.getString(ProducerConfig.ACKS_CONFIG));
        
        // 创建并返回 Sender 实例
        return new Sender(logContext,
                client,
                metadata,
                this.accumulator,
                maxInflightRequests == 1,
                producerConfig.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                acks,
                producerConfig.getInt(ProducerConfig.RETRIES_CONFIG),
                metricsRegistry.senderMetrics,
                time,
                requestTimeoutMs,
                producerConfig.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                this.transactionManager,
                apiVersions);
    }

    /**
     * 获取 linger.ms 配置值
     * 
     * @param config 生产者配置
     * @return linger.ms 配置值，最大不超过 Integer.MAX_VALUE
     */
    private static int lingerMs(ProducerConfig config) {
        // 获取 linger.ms 配置值，并确保其不超过 Integer.MAX_VALUE
        return (int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), Integer.MAX_VALUE);
    }

    /**
     * 配置消息传递超时时间
     * 
     * @param config 生产者配置
     * @param log 日志记录器
     * @return 配置的消息传递超时时间
     */
    private static int configureDeliveryTimeout(ProducerConfig config, Logger log) {
        // 获取 delivery.timeout.ms 配置值
        int deliveryTimeoutMs = config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        // 获取 linger.ms 配置值
        int lingerMs = lingerMs(config);
        // 获取 request.timeout.ms 配置值
        int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        // 计算 linger.ms 和 request.timeout.ms 之和，并确保其不超过 Integer.MAX_VALUE
        int lingerAndRequestTimeoutMs = (int) Math.min((long) lingerMs + requestTimeoutMs, Integer.MAX_VALUE);

        // 检查 delivery.timeout.ms 是否小于 linger.ms + request.timeout.ms
        if (deliveryTimeoutMs < lingerAndRequestTimeoutMs) {
            if (config.originals().containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                // 如果用户显式设置了不一致的值，则抛出异常
                throw new ConfigException(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                    + " should be equal to or larger than " + ProducerConfig.LINGER_MS_CONFIG
                    + " + " + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            } else {
                // 为了向后兼容，将 deliveryTimeoutMs 的默认值覆盖为 lingerMs + requestTimeoutMs
                deliveryTimeoutMs = lingerAndRequestTimeoutMs;
                log.warn("{} should be equal to or larger than {} + {}. Setting it to {}.",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
            }
        }
        return deliveryTimeoutMs;
    }

    /**
     * 配置事务状态
     * 
     * @param config 生产者配置
     * @param logContext 日志上下文
     * @return 配置的事务管理器
     */
    private TransactionManager configureTransactionState(ProducerConfig config,
                                                         LogContext logContext) {
        TransactionManager transactionManager = null;

        // 检查是否启用了幂等性配置
        if (config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)) {
            // 获取事务 ID
            final String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            // 获取事务超时时间
            final int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            // 获取重试退避时间
            final long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            // 创建事务管理器
            transactionManager = new TransactionManager(
                logContext,
                transactionalId,
                transactionTimeoutMs,
                retryBackoffMs,
                apiVersions
            );

            // 根据事务管理器的状态记录日志
            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        }
        return transactionManager;
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     * 如果配置中设置了 transactional.id，则需要在调用任何其他方法之前调用此方法。
     *
     * This method does the following:
     *   1. Ensures any transactions initiated by previous instances of the producer with the same
     *      transactional.id are completed. If the previous instance had failed with a transaction in
     *      progress, it will be aborted. If the last transaction had begun completion,
     *      but not yet finished, this method awaits its completion.
     *   2. Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.
     * 这个方法执行以下操作：
     * 1. 确保由具有相同 transactional.id 的生产者的先前实例启动的任何事务都已完成。如果前一个实例在进行中的事务失败，它将被中止。
     * 如果最后一个事务已经开始完成，但尚未完成，此方法将等待其完成。
     * 2. 获取内部生产者 ID 和 epoch，用于生产者发出的所有未来事务消息。
     * 
     * Note that this method will raise {@link TimeoutException} if the transactional state cannot
     * be initialized before expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException}
     * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
     * initialized, this method should no longer be used.
     * 注意：如果事务状态不能在 max.block.ms 过期之前初始化，此方法将抛出 TimeoutException。此外，如果线程被中断，此方法将抛出 InterruptedException。
     * 在这两种情况下，都可以安全地重试，但一旦事务状态成功初始化，此方法不应再被使用。
     *
     * @throws IllegalStateException if no transactional.id has been configured 
     *                               如果未配置 transactional.id，则抛出 IllegalStateException
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     *  非常严重的错误，表示 broker 不支持事务（即如果其版本低于 0.11.0.0）
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * 非常严重的错误，表示配置的 transactional.id 未授权。有关更多详细信息，请参阅异常。
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * 非常严重的错误，表示生产者已遇到先前的致命错误或任何其他意外错误
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
     * 如果初始化事务的时间超过了 <code>max.block.ms</code>，则抛出 TimeoutException
     * @throws InterruptException if the thread is interrupted while blocked
     * 如果线程在等待时被中断，则抛出 InterruptedException
     */
    public void initTransactions() {
        // 检查是否配置了事务管理器
        throwIfNoTransactionManager();
        // 检查生产者是否已关闭
        throwIfProducerClosed();
        // 初始化事务
        TransactionalRequestResult result = transactionManager.initializeTransactions();
        // 唤醒发送器
        sender.wakeup();
        // 等待事务初始化完成
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }

    /**`
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke {@link #initTransactions()} exactly one time.
     * 在开始每个新事务之前应调用此方法。注意，在调用此方法之前，必须恰好调用一次 {@link #initTransactions()}。
     *
     * @throws IllegalStateException if no transactional.id has been configured or if {@link #initTransactions()}
     *         has not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void beginTransaction() throws ProducerFencedException {
        // 检查是否配置了事务管理器
        throwIfNoTransactionManager();
        // 检查生产者是否已关闭
        throwIfProducerClosed();
        // 开始事务
        transactionManager.beginTransaction();
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * 发送偏移量到消费者组协调器，并将这些偏移量标记为当前事务的一部分。
     * 只有当事务成功提交时，这些偏移量才会被视为已提交。
     * 提交的偏移量应该是应用程序将要消费的下一个消息，即 lastProcessedMessageOffset + 1。
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
     * {@link KafkaConsumer consumer}. Note, that the consumer should have {@code enable.auto.commit=false}
     * and should also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     * 这个方法应该在需要将消费和生产消息批量处理时使用，通常在 consume-transform-produce 模式中。
     * 因此，指定的 {@code consumerGroupId} 应该与使用的 {@link KafkaConsumer consumer} 的配置参数 {@code group.id} 相同。
     * 注意，消费者应该有 {@code enable.auto.commit=false} 并且不应该手动提交偏移量（通过 {@link KafkaConsumer#commitSync(Map) sync} 或 {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} 提交）。
     *
     * @throws IllegalStateException if no transactional.id has been configured, no transaction has been started
     * 如果未配置 transactional.id，则抛出 IllegalStateException
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * 非常严重的错误，表示另一个具有相同 transactional.id 的生产者处于活动状态
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * 非常严重的错误，表示 broker 不支持事务（即如果其版本低于 0.11.0.0）
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * 非常严重的错误，表示 broker 上用于偏移量主题的消息格式不支持事务
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized, or the consumer group id is not authorized.
     * 非常严重的错误，表示配置的 transactional.id 未授权，或消费者组 id 未授权。
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * 非常严重的错误，表示生产者尝试使用旧的 epoch 向分区 leader 发送消息。有关更多详细信息，请参阅异常。
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * 非常严重的错误，表示生产者已遇到先前的致命或可中止错误，或任何其他意外错误
     * @deprecated Since 3.0.0, please use {@link #sendOffsetsToTransaction(Map, ConsumerGroupMetadata)} instead.
     * 从 3.0.0 开始，请使用 {@link #sendOffsetsToTransaction(Map, ConsumerGroupMetadata)} 代替。
     */
    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        // 调用新的方法，使用 ConsumerGroupMetadata 代替 consumerGroupId
        sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * 发送偏移量到消费者组协调器，并将这些偏移量标记为当前事务的一部分。
     * 只有当事务成功提交时，这些偏移量才会被视为已提交。
     * 提交的偏移量应该是应用程序将要消费的下一个消息，即 lastProcessedMessageOffset + 1。
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code groupMetadata} should be extracted from the used {@link KafkaConsumer consumer} via
     * {@link KafkaConsumer#groupMetadata()} to leverage consumer group metadata. This will provide
     * stronger fencing than just supplying the {@code consumerGroupId} and passing in {@code new ConsumerGroupMetadata(consumerGroupId)},
     * however note that the full set of consumer group metadata returned by {@link KafkaConsumer#groupMetadata()}
     * requires the brokers to be on version 2.5 or newer to understand.
     * 这个方法应该在需要将消费和生产消息批量处理时使用，通常在 consume-transform-produce 模式中。
     * 因此，指定的 {@code groupMetadata} 应该通过使用的 {@link KafkaConsumer consumer} 的 {@link KafkaConsumer#groupMetadata()} 提取，以利用消费者组元数据。
     * 这将提供比仅提供 {@code consumerGroupId} 并传递 {@code new ConsumerGroupMetadata(consumerGroupId)} 更强的隔离，
     * 但请注意，{@link KafkaConsumer#groupMetadata()} 返回的完整消费者组元数据集需要 broker 版本为 2.5 或更高版本才能理解。
     *
     * <p>
     * Note, that the consumer should have {@code enable.auto.commit=false} and should
     * also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     * This method will raise {@link TimeoutException} if the producer cannot send offsets before expiration of {@code max.block.ms}.
     * Additionally, it will raise {@link InterruptException} if interrupted.
     * 注意，消费者应该有 {@code enable.auto.commit=false} 并且不应该手动提交偏移量（通过 {@link KafkaConsumer#commitSync(Map) sync} 或 {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} 提交）。
     * 如果生产者在 {@code max.block.ms} 过期前无法发送偏移量，此方法将引发 {@link TimeoutException}。
     * 此外，如果被中断，它将引发 {@link InterruptException}。
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started.
     * 如果未配置 transactional.id 或未启动事务，则抛出 IllegalStateException。
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * 非常严重的错误，表示另一个具有相同 transactional.id 的生产者处于活动状态。
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0) or
     *         the broker doesn't support latest version of transactional API with all consumer group metadata
     *         (i.e. if its version is lower than 2.5.0).
     * 非常严重的错误，表示 broker 不支持事务（即如果其版本低于 0.11.0.0）或 broker 不支持包含所有消费者组元数据的最新事务 API 版本（即如果其版本低于 2.5.0）。
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * 非常严重的错误，表示 broker 上用于偏移量主题的消息格式不支持事务。
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized, or the consumer group id is not authorized.
     * 非常严重的错误，表示配置的 transactional.id 未授权，或消费者组 id 未授权。
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried
     *         (e.g. if the consumer has been kicked out of the group). Users should handle this by aborting the transaction.
     * 如果提交失败且无法重试（例如，如果消费者被踢出组）。用户应通过中止事务来处理此问题。
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this producer instance gets fenced by broker due to a
     *                                                                  mis-configured consumer instance id within group metadata.
     * 如果由于组元数据中的消费者实例 id 配置错误而导致此生产者实例被 broker 隔离。
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * 非常严重的错误，表示生产者尝试使用旧的 epoch 向分区 leader 发送消息。有关更多详细信息，请参阅异常。
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * 非常严重的错误，表示生产者已遇到先前的致命或可中止错误，或任何其他意外错误。
     * @throws TimeoutException if the time taken for sending offsets has surpassed max.block.ms.
     * 如果发送偏移量的时间超过了 max.block.ms。
     * @throws InterruptException if the thread is interrupted while blocked
     * 如果线程在阻塞时被中断。
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        // 检查消费者组元数据是否有效
        throwIfInvalidGroupMetadata(groupMetadata);
        // 检查是否有事务管理器
        throwIfNoTransactionManager();
        // 检查生产者是否已关闭
        throwIfProducerClosed();
        // 发送偏移量到事务中
        TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, groupMetadata);
        // 唤醒发送者线程
        sender.wakeup();
        // 等待事务请求结果
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually committing the transaction.
     * 提交正在进行的事务。此方法将在实际提交事务之前刷新所有未发送的记录。
     *
     * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction will not be committed.
     * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
     * 此外，如果事务中的任何 {@link #send(ProducerRecord)} 调用遇到不可恢复的错误，此方法将立即抛出最后收到的异常，并且事务不会被提交。
     * 因此，事务中的所有 {@link #send(ProducerRecord)} 调用都必须成功，以便此方法成功。
     *
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be committed before expiration
     * of {@code max.block.ms}. Additionally, it will raise {@link InterruptException} if interrupted.
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as abortTransaction)
     * since the commit may already be in the progress of completing. If not retrying, the only option is to close the producer.
     * 请注意，如果在 {@code max.block.ms} 过期之前无法提交事务，此方法将引发 {@link TimeoutException}。此外，如果被中断，它将引发 {@link InterruptException}。
     * 在这两种情况下重试都是安全的，但无法尝试不同的操作（例如 abortTransaction），因为提交可能已经在完成过程中。如果不重试，唯一的选择是关闭生产者。
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * 如果未配置 transactional.id 或未启动事务，则抛出 IllegalStateException
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * 如果另一个具有相同 transactional.id 的生产者处于活动状态，则抛出致命错误 ProducerFencedException
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * 如果代理不支持事务（即其版本低于 0.11.0.0），则抛出致命错误 org.apache.kafka.common.errors.UnsupportedVersionException
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * 如果配置的 transactional.id 未被授权，则抛出致命错误 org.apache.kafka.common.errors.AuthorizationException。有关详细信息，请参阅异常
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * 如果生产者尝试使用旧的 epoch 向分区领导者生产，则抛出 org.apache.kafka.common.errors.InvalidProducerEpochException。有关详细信息，请参阅异常
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * 如果生产者遇到先前的致命或可中止错误，或任何其他意外错误，则抛出 KafkaException
     * @throws TimeoutException if the time taken for committing the transaction has surpassed <code>max.block.ms</code>.
     * 如果提交事务所花费的时间超过 <code>max.block.ms</code>，则抛出 TimeoutException
     * @throws InterruptException if the thread is interrupted while blocked
     * 如果线程在阻塞时被中断，则抛出 InterruptException
     */
    public void commitTransaction() throws ProducerFencedException {
        // 检查是否存在事务管理器，如果没有则抛出异常
        throwIfNoTransactionManager();
        // 检查生产者是否已关闭，如果已关闭则抛出异常
        throwIfProducerClosed();
        // 开始提交事务，并返回事务请求结果
        TransactionalRequestResult result = transactionManager.beginCommit();
        // 唤醒发送线程
        sender.wakeup();
        // 等待事务提交完成，超时时间为 maxBlockTimeMs 毫秒
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call is made.
     * 终止正在进行的事务。任何未刷新的生产消息将在此调用时被终止。
     * This call will throw an exception immediately if any prior {@link #send(ProducerRecord)} calls failed with a
     * {@link ProducerFencedException} or an instance of {@link org.apache.kafka.common.errors.AuthorizationException}.
     * 如果之前的任何 {@link #send(ProducerRecord)} 调用因 {@link ProducerFencedException} 或 {@link org.apache.kafka.common.errors.AuthorizationException} 实例失败，此调用将立即抛出异常。
     *
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be aborted before expiration
     * of {@code max.block.ms}. Additionally, it will raise {@link InterruptException} if interrupted.
     * 请注意，如果在 {@code max.block.ms} 过期之前无法终止事务，此方法将引发 {@link TimeoutException}。此外，如果被中断，它将引发 {@link InterruptException}。
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as commitTransaction)
     * since the abort may already be in the progress of completing. If not retrying, the only option is to close the producer.
     * 在这两种情况下重试都是安全的，但无法尝试不同的操作（例如 commitTransaction），因为终止可能已经在完成过程中。如果不重试，唯一的选择是关闭生产者。
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * 如果没有配置 transactional.id 或没有启动事务，则抛出 IllegalStateException
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * 如果另一个具有相同 transactional.id 的生产者处于活动状态，则抛出致命错误 ProducerFencedException
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * 如果生产者尝试使用旧的 epoch 向分区领导者生产，则抛出 org.apache.kafka.common.errors.InvalidProducerEpochException。有关详细信息，请参阅异常
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * 如果代理不支持事务（即其版本低于 0.11.0.0），则抛出致命错误 org.apache.kafka.common.errors.UnsupportedVersionException
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * 如果配置的 transactional.id 未被授权，则抛出致命错误 org.apache.kafka.common.errors.AuthorizationException。有关详细信息，请参阅异常
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * 如果生产者遇到先前的致命错误或任何其他意外错误，则抛出 KafkaException
     * @throws TimeoutException if the time taken for aborting the transaction has surpassed <code>max.block.ms</code>.
     * 如果终止事务所花费的时间超过 <code>max.block.ms</code>，则抛出 TimeoutException
     * @throws InterruptException if the thread is interrupted while blocked
     * 如果线程在阻塞时被中断，则抛出 InterruptException
     */
    public void abortTransaction() throws ProducerFencedException {
        // 检查是否存在事务管理器，如果没有则抛出异常
        throwIfNoTransactionManager();
        // 检查生产者是否已关闭，如果已关闭则抛出异常
        throwIfProducerClosed();
        // 记录日志，表示正在终止未完成的事务
        log.info("Aborting incomplete transaction");
        // 开始终止事务，并返回事务请求结果
        TransactionalRequestResult result = transactionManager.beginAbort();
        // 唤醒发送线程
        sender.wakeup();
        // 等待事务终止完成，超时时间为 maxBlockTimeMs 毫秒
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     * 异步发送一条记录到一个主题。等同于 <code>send(record, null)</code>。
     * 详情请参见 {@link #send(ProducerRecord, Callback)}。
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        // 调用 send(record, null) 方法发送记录，null 表示没有回调
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * 异步发送一条记录到一个主题，并在发送被确认时调用提供的回调。
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * 发送是异步的，一旦记录被存储在等待发送的记录缓冲区中，此方法将立即返回。这允许并行发送许多记录，而无需在每次发送后阻塞等待响应。
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset
     * it was assigned and the timestamp of the record. If
     * {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} is used by the topic, the timestamp
     * will be the user provided timestamp or the record send time if the user did not specify a timestamp for the
     * record. If {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     * 发送的结果是一个 {@link RecordMetadata}，指定记录发送到的分区、分配的偏移量和记录的时间戳。如果主题使用
     * {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime}，则时间戳将是用户提供的时间戳，
     * 如果用户未为记录指定时间戳，则为记录发送时间。如果主题使用 {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime}，
     * 则时间戳将是消息附加时的 Kafka 代理本地时间。
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will block until the associated request completes and then return the metadata for the record
     * or throw any exception that occurred while sending the record.
     * 由于发送调用是异步的，它返回一个 {@link java.util.concurrent.Future Future}，用于将分配给此记录的 {@link RecordMetadata}。
     * 在此 future 上调用 {@link java.util.concurrent.Future#get() get()} 将阻塞，直到关联的请求完成，然后返回记录的元数据或抛出发送记录时发生的任何异常。
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     * 如果您想模拟一个简单的阻塞调用，可以立即调用 <code>get()</code> 方法：
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }</pre>
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     * 完全非阻塞的使用可以利用 {@link Callback} 参数来提供一个在请求完成时调用的回调。
     *
     * <pre>
     * {@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null) {
     *                          e.printStackTrace();
     *                       } else {
     *                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                       }
     *                   }
     *               });
     * }
     * </pre>
     *
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     * 发送到同一分区的记录的回调保证按顺序执行。也就是说，在以下示例中，<code>callback1</code> 保证在 <code>callback2</code> 之前执行：
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     * When used as part of a transaction, it is not necessary to define a callback or check the result of the future
     * in order to detect errors from <code>send</code>. If any of the send calls failed with an irrecoverable error,
     * the final {@link #commitTransaction()} call will fail and throw the exception from the last failed send. When
     * this happens, your application should call {@link #abortTransaction()} to reset the state and continue to send
     * data.
     * 当用作事务的一部分时，不需要定义回调或检查 future 的结果以检测 <code>send</code> 的错误。如果任何发送调用因不可恢复的错误而失败，
     * 最终的 {@link #commitTransaction()} 调用将失败并抛出最后一次失败发送的异常。当这种情况发生时，您的应用程序应调用 {@link #abortTransaction()} 来重置状态并继续发送数据。
     * </p>
     * <p>
     * Some transactional send errors cannot be resolved with a call to {@link #abortTransaction()}.  In particular,
     * if a transactional send finishes with a {@link ProducerFencedException}, a {@link org.apache.kafka.common.errors.OutOfOrderSequenceException},
     * a {@link org.apache.kafka.common.errors.UnsupportedVersionException}, or an
     * {@link org.apache.kafka.common.errors.AuthorizationException}, then the only option left is to call {@link #close()}.
     * Fatal errors cause the producer to enter a defunct state in which future API calls will continue to raise
     * the same underyling error wrapped in a new {@link KafkaException}.
     * 一些事务性发送错误无法通过调用 {@link #abortTransaction()} 解决。特别是，如果事务性发送以 {@link ProducerFencedException}、
     * {@link org.apache.kafka.common.errors.OutOfOrderSequenceException}、{@link org.apache.kafka.common.errors.UnsupportedVersionException} 或
     * {@link org.apache.kafka.common.errors.AuthorizationException} 结束，那么唯一的选择是调用 {@link #close()}。
     * 致命错误会导致生产者进入失效状态，在这种状态下，未来的 API 调用将继续引发相同的底层错误，并包装在新的 {@link KafkaException} 中。
     * </p>
     * <p>
     * It is a similar picture when idempotence is enabled, but no <code>transactional.id</code> has been configured.
     * In this case, {@link org.apache.kafka.common.errors.UnsupportedVersionException} and
     * {@link org.apache.kafka.common.errors.AuthorizationException} are considered fatal errors. However,
     * {@link ProducerFencedException} does not need to be handled. Additionally, it is possible to continue
     * sending after receiving an {@link org.apache.kafka.common.errors.OutOfOrderSequenceException}, but doing so
     * can result in out of order delivery of pending messages. To ensure proper ordering, you should close the
     * producer and create a new instance.
     * 当启用幂等性但未配置 <code>transactional.id</code> 时，情况类似。在这种情况下，{@link org.apache.kafka.common.errors.UnsupportedVersionException} 和
     * {@link org.apache.kafka.common.errors.AuthorizationException} 被视为致命错误。然而，不需要处理 {@link ProducerFencedException}。
     * 此外，在收到 {@link org.apache.kafka.common.errors.OutOfOrderSequenceException} 后可以继续发送，但这样做可能会导致挂起消息的顺序错误。
     * 为确保正确排序，您应关闭生产者并创建一个新实例。
     * </p>
     * <p>
     * If the message format of the destination topic is not upgraded to 0.11.0.0, idempotent and transactional
     * produce requests will fail with an {@link org.apache.kafka.common.errors.UnsupportedForMessageFormatException}
     * error. If this is encountered during a transaction, it is possible to abort and continue. But note that future
     * sends to the same topic will continue receiving the same exception until the topic is upgraded.
     * 如果目标主题的消息格式未升级到 0.11.0.0，幂等和事务性生产请求将失败并出现 {@link org.apache.kafka.common.errors.UnsupportedForMessageFormatException} 错误。
     * 如果在事务期间遇到此问题，可以中止并继续。但请注意，未来发送到同一主题的消息将继续收到相同的异常，直到主题升级。
     * </p>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     * 请注意，回调通常会在生产者的 I/O 线程中执行，因此应该相对较快，否则会延迟其他线程的消息发送。如果您想执行阻塞或计算量大的回调，建议在回调主体中使用您自己的 {@link java.util.concurrent.Executor} 来并行处理。
     *
     * @param record The record to send
     *               要发送的记录
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *        indicates no callback)
     *                 记录被服务器确认时执行的用户提供的回调（null 表示没有回调）
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     *                                 如果身份验证失败。有关详细信息，请参阅异常
     * @throws AuthorizationException fatal error indicating that the producer is not allowed to write
     *                                表示生产者不允许写入的致命错误
     * @throws IllegalStateException if a transactional.id has been configured and no transaction has been started, or
     *                               when send is invoked after producer has been closed.
     *                               如果已配置 transactional.id 并且未启动事务，或者在生产者关闭后调用 send。
     * @throws InterruptException If the thread is interrupted while blocked
     *                            如果线程在阻塞时被中断
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     *                                如果键或值不是给定配置的序列化器的有效对象
     * @throws TimeoutException If the record could not be appended to the send buffer due to memory unavailable
     *                          or missing metadata within {@code max.block.ms}.
     *                          如果由于内存不可用或在 {@code max.block.ms} 内缺少元数据，记录无法附加到发送缓冲区。
     * @throws KafkaException If a Kafka related error occurs that does not belong to the public API exceptions.
     *                        如果发生不属于公共 API 异常的 Kafka 相关错误。
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // 拦截记录，记录可能会被修改；此方法不会抛出异常
        // interceptors 内部封装了所有了 interceptor，调用 onSend 方法，对记录进行拦截；内部是一个 try-catch 代码块，捕获异常，不会抛出异常
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        // 调用 doSend 方法发送拦截后的记录
        return doSend(interceptedRecord, callback);
    }

    // Verify that this producer instance has not been closed. This method throws IllegalStateException if the producer
    // has already been closed.
    // 验证此生产者实例是否已关闭。如果生产者已关闭，则抛出 IllegalStateException。
    private void throwIfProducerClosed() {
        if (sender == null || !sender.isRunning())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }

    /**
     * Implementation of asynchronously send a record to a topic.
     * 实现异步发送一条记录到一个主题。
     *
     * 核心逻辑：
     * 1. 验证生产者是否已关闭
     * 2. 等待元数据可用
     * 3. 序列化键和值
     * 4. 计算记录大小
     * 5. 确保记录大小有效
     * 6. 创建拦截回调
     * 7. 在事务管理器中失败事务
     * 8. 将记录附加到累加器
     *
     * @param record 要发送的记录
     * @param callback 记录被服务器确认时执行的用户提供的回调
     * @return 记录元数据的 Future 对象
     * 
     * @throws IllegalStateException 如果生产者已关闭
     * @throws SerializationException 如果键或值的序列化失败
     * @throws InterruptException 如果线程在阻塞时被中断
     * @throws KafkaException 如果发生 Kafka 相关错误
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            // 验证生产者是否已关闭
            throwIfProducerClosed();
            // first make sure the metadata for the topic is available
            // 确保主题的元数据可用
            long nowMs = time.milliseconds();
            ClusterAndWaitTime clusterAndWaitTime;
            try {
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), nowMs, maxBlockTimeMs);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
            }
            nowMs += clusterAndWaitTime.waitedOnMetadataMs;
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            Cluster cluster = clusterAndWaitTime.cluster;
            byte[] serializedKey;
            try {
                // 序列化键
                serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }
            byte[] serializedValue;
            try {
                // 序列化值
                serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
            }
            // 计算分区
            int partition = partition(record, serializedKey, serializedValue, cluster);
            tp = new TopicPartition(record.topic(), partition);

            setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();

            // 估算记录大小
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(),
                    compressionType, serializedKey, serializedValue, headers);
            // 确保记录大小有效
            ensureValidRecordSize(serializedSize);
            long timestamp = record.timestamp() == null ? nowMs : record.timestamp();
            if (log.isTraceEnabled()) {
                log.trace("Attempting to append record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);
            }
            // producer callback will make sure to call both 'callback' and interceptor callback
            // 创建拦截回调
            Callback interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);

            if (transactionManager != null && transactionManager.isTransactional()) {
                transactionManager.failIfNotReadyForSend();
            }
            // 将记录附加到累加器（注意：abortOnNewBatch = true）
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,
                    serializedValue, headers, interceptCallback, remainingWaitMs, true, nowMs);

            // 如果 result 提示需要创建新的批次
            if (result.abortForNewBatch) {
                // 记录下之前 partitioner 分区的值
                int prevPartition = partition;
                // 触发分区器感知批次的新建
                partitioner.onNewBatch(record.topic(), cluster, prevPartition);
                // 重新计算分区
                partition = partition(record, serializedKey, serializedValue, cluster);
                tp = new TopicPartition(record.topic(), partition);
                if (log.isTraceEnabled()) {
                    log.trace("Retrying append due to new batch creation for topic {} partition {}. The old partition was {}", record.topic(), partition, prevPartition);
                }
                // producer callback will make sure to call both 'callback' and interceptor callback
                // 创建新的拦截回调
                interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);

                // 再次将记录附加到累加器（注意：abortOnNewBatch=false）
                result = accumulator.append(tp, timestamp, serializedKey,
                    serializedValue, headers, interceptCallback, remainingWaitMs, false, nowMs);
            }

            if (transactionManager != null && transactionManager.isTransactional())
                transactionManager.maybeAddPartitionToTransaction(tp);

            // 如果 result 提示批次已满或新批次已创建，那么就唤醒发送线程
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
                this.sender.wakeup();
            }
            // 返回 RecordAccumulator 给的 append 方法返回的 Future 对象
            return result.future;
            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
            // 处理异常并记录错误；
            // 对于 API 异常，将它们返回到 future 中，
            // 对于其他异常，直接抛出
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            // producer callback will make sure to call both 'callback' and interceptor callback
            // 创建拦截回调
            if (tp == null) {
                // set topicPartition to -1 when null
                // 如果 tp 为空，则设置 topicPartition 为 -1
                tp = ProducerInterceptors.extractTopicPartition(record);
            }

            // 根据回调方法 callback、拦截器、topicPartition 创建一个新的拦截回调；进而触发 onCompletion 方法
            Callback interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);

            // The onCompletion callback does expect a non-null metadata, but one will be created inside
            // the interceptor's onCompletion implementation before the user's callback is invoked.
            // 这里传了一个 null 作为 RecordMetadata，在 onCompletion 方法中会创建一个新的 RecordMetadata
            // 内部会触发拦截器的 onCompletion 方法和回调方法的 onCompletion 方法
            interceptCallback.onCompletion(null, e);
            this.errors.record();
            // 触发 interceptors 的 onSendError 方法
            this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw new InterruptException(e);
        } catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            // 通知拦截器所有异常，因为 onSend 在此方法中的其他任何操作之前调用
            this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        // 如果 headers 是 RecordHeaders 类型，则将其设置为只读
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * 等待集群元数据，包括给定主题的分区可用。
     * 
     * @param topic The topic we want metadata for
     *              我们要获取元数据的主题
     * @param partition A specific partition expected to exist in metadata, or null if there's no preference
     *                 一个特定的分区，期望在元数据中存在，或者为 null 如果没有偏好
     * @param nowMs The current time in ms
     *              当前时间
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     *                 等待元数据的最大时间
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     *         包含主题元数据的集群和等待元数据的时间
     * 
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     *                          如果元数据在 {@code max.block.ms} 内无法刷新
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     *                        对于所有 Kafka 相关异常，包括在生产者关闭后调用此方法的情况
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long nowMs, long maxWaitMs) throws InterruptedException {
        // add topic to metadata topic list if it is not there already and reset expiry
        // 如果主题不在元数据主题列表中，则添加主题并重置过期时间

        // 获取到当前的集群元数据
        Cluster cluster = metadata.fetch();

        // 检查 topic 是否有效，如果无效则抛出 InvalidTopicException
        if (cluster.invalidTopics().contains(topic))
            throw new InvalidTopicException(topic);

        // 将 topic 添加到元数据中，并更新当前时间
        metadata.add(topic, nowMs);

        // 获取 topic 的分区数
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        // 如果我们有缓存的元数据，并且记录的分区未定义或在已知分区范围内，则返回缓存的元数据
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            return new ClusterAndWaitTime(cluster, 0);

        // 走到下面的逻辑的时候，说明：partitionsCount == null 或者指定的 partition 大于等于 partitionsCount
        // 进而说明需要和 broker 通信获取最新的元数据
        long remainingWaitMs = maxWaitMs;
        long elapsed = 0;
        // Issue metadata requests until we have metadata for the topic and the requested partition,
        // or until maxWaitTimeMs is exceeded. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        // 发出元数据请求，直到我们有主题和请求分区的元数据，或者超过 maxWaitTimeMs。
        // 这是必要的，以防元数据过时，并且此期间主题的分区数增加。
        do {
            if (partition != null) {
                log.trace("Requesting metadata update for partition {} of topic {}.", partition, topic);
            } else {
                log.trace("Requesting metadata update for topic {}.", topic);
            }
            // 再更新一次元数据
            metadata.add(topic, nowMs + elapsed);
            int version = metadata.requestUpdateForTopic(topic);
            sender.wakeup();
            // 等待元数据更新，看起来更新是在 sender 中操作的
            try {
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                // 使用原始的 maxWaitMs 重新抛出，以防止记录 remainingWaitMs 的异常
                throw new TimeoutException(
                        String.format("Topic %s not present in metadata after %d ms.",
                                topic, maxWaitMs));
            }
            // 再次获取集群元数据
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - nowMs;
            if (elapsed >= maxWaitMs) {
                throw new TimeoutException(partitionsCount == null ?
                        String.format("Topic %s not present in metadata after %d ms.",
                                topic, maxWaitMs) :
                        String.format("Partition %d of topic %s with partition count %d is not present in metadata after %d ms.",
                                partition, topic, partitionsCount, maxWaitMs));
            }
            metadata.maybeThrowExceptionForTopic(topic);
            remainingWaitMs = maxWaitMs - elapsed;
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null || // 如果分区数为 null，则继续循环
                (partition != null && partition >= partitionsCount)); // 如果分区不为 null 并且分区大于等于分区数（分区数增加），则继续循环

        // 返回包含主题元数据的集群和等待元数据的时间
        return new ClusterAndWaitTime(cluster, elapsed);
    }

    /**
     * Validate that the record size isn't too large
     * 确保记录大小有效
     * 
     * @param size 记录大小
     * @throws RecordTooLargeException 如果记录大小超过最大请求大小
     */
    private void ensureValidRecordSize(int size) {
        // 如果记录大小超过最大请求大小，则抛出 RecordTooLargeException
        if (size > maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than " + maxRequestSize + ", which is the value of the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG + " configuration.");
        // 如果记录大小超过总内存大小，则抛出 RecordTooLargeException
        if (size > totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * 
     * 调用此方法会使所有缓冲的记录立即可发送（即使 <code>linger.ms</code> 大于 0），并阻塞与这些记录相关的请求的完成。
     * <code>flush()</code> 的后置条件是任何先前发送的记录都已完成（例如 <code>Future.isDone() == true</code>）。
     * 根据您指定的 <code>acks</code> 配置，当请求成功确认时，认为请求已完成，否则会导致错误。
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * 
     * 其他线程可以继续发送记录，而一个线程被阻塞等待 flush 调用完成，
     * 但是对于在 flush 调用开始后发送的记录的完成情况不做任何保证。
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * 
     * 当从某些输入系统消费并生产到 Kafka 时，此方法非常有用。<code>flush()</code> 调用提供了一种方便的方法来确保所有先前发送的消息都已完成。
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * 
     * 此示例显示了如何从一个 Kafka 主题消费并生产到另一个 Kafka 主题：
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commitSync();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     * 
     * 请注意，如果生产请求失败，上述示例可能会丢弃记录。如果我们想确保这种情况不会发生，
     * 我们需要在配置中设置 <code>retries=&lt;large_number&gt;</code>。
     * </p>
     * <p>
     * Applications don't need to call this method for transactional producers, since the {@link #commitTransaction()} will
     * flush all buffered records before performing the commit. This ensures that all the {@link #send(ProducerRecord)}
     * calls made since the previous {@link #beginTransaction()} are completed before the commit.
     * 
     * 对于事务性生产者，应用程序不需要调用此方法，因为 {@link #commitTransaction()} 会在执行提交之前刷新所有缓冲的记录。
     * 这确保了自上一次 {@link #beginTransaction()} 以来进行的所有 {@link #send(ProducerRecord)} 调用在提交之前都已完成。
     * </p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     *                            如果线程在阻塞时被中断
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");
        // 开始刷新
        this.accumulator.beginFlush();
        // 唤醒发送器
        this.sender.wakeup();
        try {
            // 等待刷新完成
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * 获取给定主题的分区元数据。这可以用于自定义分区。
     * 
     * @throws AuthenticationException if authentication fails. See the exception for more details
     *                                 如果身份验证失败。有关详细信息，请参阅异常
     * @throws AuthorizationException if not authorized to the specified topic. See the exception for more details
     *                                如果未被授权访问指定的主题。有关详细信息，请参阅异常
     * @throws InterruptException if the thread is interrupted while blocked
     *                            如果线程在阻塞时被中断
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     *                          如果元数据无法在 {@code max.block.ms} 内刷新
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     *                        对于所有与 Kafka 相关的异常，包括在生产者关闭后调用此方法的情况
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        // 检查主题不能为 null
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            // 等待元数据更新，并返回包含主题元数据的集群
            return waitOnMetadata(topic, null, time.milliseconds(), maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     * 获取生产者维护的内部指标的完整集合。
     * 
     * @return 包含所有指标的不可变映射
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        // 返回包含所有指标的不可变映射
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * 关闭此生产者。此方法会阻塞，直到所有先前发送的请求完成。
     * 
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * 此方法等同于 <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>。
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <strong>如果从 {@link Callback} 调用 close()，将记录一条警告消息，并改为调用 close(0, TimeUnit.MILLISECONDS)。
     * 否则，发送线程会尝试 join 自身并永远阻塞。</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked.
     *                            如果线程在阻塞时被中断。
     * @throws KafkaException If a unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer functionable.
     *                        如果在尝试关闭客户端时发生意外错误，此错误应被视为致命错误，并表明客户端不再可用。    
     */
    @Override
    public void close() {
        // 关闭生产者，等待所有未完成的请求完成
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * 此方法等待最多 <code>timeout</code> 以完成所有未完成的请求的发送。
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately. It will also abort the ongoing transaction if it's not
     * already completing.
     * 如果生产者无法在超时之前完成所有请求，此方法将立即失败任何未发送和未确认的记录。
     * 它还将中止任何未完成的交易。
     * 
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(Duration.ofMillis(0))</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     * 
     * 如果从 {@link Callback} 中调用此方法，此方法将不会阻塞，并且等同于 <code>close(Duration.ofMillis(0))</code>。
     * 这是为了避免在生产者阻塞时进一步发送。
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     *              生产者等待完成所有未完成的请求的最大时间。
     *              该值应为非负数。指定超时时间为零表示不等待未完成的请求完成。
     * @throws InterruptException If the thread is interrupted while blocked.
     *                            如果线程在阻塞时被中断。
     * @throws KafkaException If a unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer functionable.
     *                        如果在尝试关闭客户端时发生意外错误，此错误应被视为致命错误，并表明客户端不再可用。
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     *                                    如果 <code>timeout</code> 为负数。
     *
     */
    @Override
    public void close(Duration timeout) {
        close(timeout, false);
    }

    /**
     * 关闭生产者。
     * 
     * @param timeout 等待完成所有未完成的请求的最大时间。
     * @param swallowException 是否吞噬异常。
     * @throws IllegalArgumentException 如果 <code>timeout</code> 为负数。
     * @throws InterruptException 如果线程在阻塞时被中断。
     * @throws KafkaException 如果在尝试关闭客户端时发生意外错误，此错误应被视为致命错误，并表明客户端不再可用。
     */    
    private void close(Duration timeout, boolean swallowException) {
        // 将超时转换为毫秒
        long timeoutMs = timeout.toMillis();
        // 如果超时为负数，则抛出 IllegalArgumentException
        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeoutMs);

        // 记录第一个遇到的异常
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        // 检查是否从回调中调用
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        // 如果超时大于 0，并且没有从回调中调用，则尝试优雅地关闭
        if (timeoutMs > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.",
                        timeoutMs);
            } else {
                // 尝试优雅地关闭
                if (this.sender != null)
                    this.sender.initiateClose();
                if (this.ioThread != null) {
                    try {
                        // 当前线程等待 ioThread 结束
                        this.ioThread.join(timeoutMs);
                    } catch (InterruptedException t) {
                        // 记录第一个遇到的异常
                        firstException.compareAndSet(null, new InterruptException(t));
                        log.error("Interrupted while joining ioThread", t);
                    }
                }
            }
        }

        // 如果 sender 和 ioThread 不为 null，并且 ioThread 仍然活跃，则强制关闭生产者
        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeoutMs);
            // 强制关闭生产者
            this.sender.forceClose();
            // 仅在不从回调中调用时加入发送者线程
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        // 安静地关闭各种资源，这些资源基本上都实现了 Closable 接口或者 AutoCloseable 接口
        Utils.closeQuietly(interceptors, "producer interceptors", firstException);
        Utils.closeQuietly(metrics, "producer metrics", firstException);
        Utils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        Utils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        Utils.closeQuietly(partitioner, "producer partitioner", firstException);
        // 注销 JMX 信息
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        // 获取第一个遇到的异常
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
        // 记录生产者已关闭
        log.debug("Kafka producer has been closed");
    }

    /**
     * 配置集群资源监听器
     *
     * @param keySerializer 用于序列化键的序列化器
     * @param valueSerializer 用于序列化值的序列化器
     * @param candidateLists 候选列表数组
     * @return 配置好的集群资源监听器
     */
    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer,
                                                                       Serializer<V> valueSerializer,
                                                                       List<?>... candidateLists) {
        // 创建一个新的集群资源监听器实例
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        
        // 遍历候选列表数组，并将每个列表中的元素添加到集群资源监听器中
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        // 将键序列化器和值序列化器添加到集群资源监听器中
        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        
        // 返回配置好的集群资源监听器
        return clusterResourceListeners;
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * calls configured partitioner class to compute the partition.
     * 
     * 计算给定记录的分区。
     * 如果记录有分区，则返回分区值，否则调用配置的分区器类来计算分区。
     * 
     * @param record 生产记录
     * @param serializedKey 序列化键
     * @param serializedValue 序列化值
     * @param cluster 集群
     * @return 分区
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        // 从 record 获取指定的分区
        Integer partition = record.partition();
        // 如果分区不为 null，则返回分区值；否则调用分区器类来计算分区
        // 如果手动指定了分区，那么在 doSend 的时候会保证拉取 metadata 来验证分区是否存在
        return partition != null ?
                partition :
                // 调用分区器来计算消息发往的分区
                partitioner.partition(
                        record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }

    /**
     * 抛出无效的消费者组元数据异常
     * 
     * @param groupMetadata 消费者组元数据
     * @throws IllegalArgumentException 如果消费者组元数据为 null 或生成 ID 大于 0 但成员 ID 为未知
     */
    private void throwIfInvalidGroupMetadata(ConsumerGroupMetadata groupMetadata) {
        // 如果消费者组元数据为 null，则抛出 IllegalArgumentException
        if (groupMetadata == null) {
            throw new IllegalArgumentException("Consumer group metadata could not be null");

            // 如果生成 ID 大于 0 且成员 ID 为未知，则抛出 IllegalArgumentException
        } else if (groupMetadata.generationId() > 0
            && JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(groupMetadata.memberId())) {
            throw new IllegalArgumentException("Passed in group metadata " + groupMetadata + " has generationId > 0 but member.id ");
        }
    }

    /**
     * 抛出没有事务管理器异常
     * 
     * @throws IllegalStateException 如果事务管理器为 null
     */
    private void throwIfNoTransactionManager() {
        // 如果事务管理器为 null，则抛出 IllegalStateException
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    // Visible for testing
    /**
     * 获取客户端 ID
     * 
     * @return 客户端 ID
     */
    String getClientId() {
        return clientId;
    }

    
    /**
     * ClusterAndWaitTime 类用于存储集群信息和等待元数据的时间
     */
    private static class ClusterAndWaitTime {
        // 集群信息
        final Cluster cluster;
        // 等待元数据的时间（毫秒）
        final long waitedOnMetadataMs;

        /**
         * 构造函数，初始化 ClusterAndWaitTime 对象
         * 
         * @param cluster 集群信息
         * @param waitedOnMetadataMs 等待元数据的时间（毫秒）
         */
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    /**
     * FutureFailure 类实现了 Future 接口，用于表示一个失败的 Future 对象。
     * 当一个异步操作失败时，可以使用这个类来封装异常信息。
     */
    private static class FutureFailure implements Future<RecordMetadata> {

        // 封装的执行异常
        private final ExecutionException exception;

        /**
         * 构造函数，初始化 FutureFailure 对象
         * 
         * @param exception 异常信息
         */
        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        /**
         * 取消操作，始终返回 false，因为失败的 Future 无法取消
         * 
         * @param interrupt 是否中断
         * @return false
         */
        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        /**
         * 获取操作结果，始终抛出封装的异常
         * 
         * @return 无返回值
         * @throws ExecutionException 封装的异常
         */
        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        /**
         * 获取操作结果，始终抛出封装的异常
         * 
         * @param timeout 超时时间
         * @param unit 时间单位
         * @return 无返回值
         * @throws ExecutionException 封装的异常
         */
        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        /**
         * 检查操作是否被取消，始终返回 false
         * 
         * @return false
         */
        @Override
        public boolean isCancelled() {
            return false;
        }

        /**
         * 检查操作是否完成，始终返回 true，因为失败的 Future 被认为是完成的
         * 
         * @return true
         */
        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * 当生产者请求完成时调用的回调。它会调用用户提供的回调（如果有的话），并通知生产者拦截器请求已完成。
     */
    private static class InterceptorCallback<K, V> implements Callback {
        // 用户提供的回调
        private final Callback userCallback;
        // 生产者拦截器
        private final ProducerInterceptors<K, V> interceptors;
        // 主题分区
        private final TopicPartition tp;

        /**
         * 构造函数，初始化用户回调、生产者拦截器和主题分区
         * 
         * @param userCallback 用户提供的回调
         * @param interceptors 生产者拦截器
         * @param tp 主题分区
         */
        private InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors, TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        /**
         * 当生产者请求完成时调用的方法
         * 
         * @param metadata 记录元数据
         * @param exception 异常信息
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            // 如果元数据为空，则创建一个新的记录元数据
            metadata = metadata != null ? metadata : new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
            // 通知拦截器请求已完成
            this.interceptors.onAcknowledgement(metadata, exception);
            // 如果用户回调不为空，则调用用户回调
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
}
