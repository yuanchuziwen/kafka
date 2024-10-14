/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.IOException
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{Selector => NSelector, _}
import java.util
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic._

import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.metrics.KafkaMetricsGroup
import kafka.network.ConnectionQuotas._
import kafka.network.Processor._
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.network.SocketServer._
import kafka.security.CredentialProvider
import kafka.server.{ApiVersionManager, BrokerReconfigurable, KafkaConfig}
import kafka.utils.Implicits._
import kafka.utils._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Avg, CumulativeSum, Meter, Rate}
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, ClientInformation, KafkaChannel, ListenerName, ListenerReconfigurable, NetworkSend, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time, Utils}
import org.apache.kafka.common.{Endpoint, KafkaException, MetricName, Reconfigurable}
import org.slf4j.event.Level

import scala.collection._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.jdk.CollectionConverters._
import scala.util.control.ControlThrowable

/**
 * Handles new connections, requests and responses to and from broker.
 * <p>
 * 管理新连接、请求和来自代理的响应。
 *
 * Kafka supports two types of request planes :
 *  - data-plane :
 *    - Handles requests from clients and other brokers in the cluster.
 *    - The threading model is
 *      1 Acceptor thread per listener, that handles new connections.
 *      It is possible to configure multiple data-planes by specifying multiple "," separated endpoints for "listeners" in KafkaConfig.
 *      Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *      M Handler threads that handle requests and produce responses back to the processor threads for writing.
 *  - control-plane :
 *    - Handles requests from controller. This is optional and can be configured by specifying "control.plane.listener.name".
 *      If not configured, the controller requests are handled by the data-plane.
 *    - The threading model is
 *      1 Acceptor thread that handles new connections
 *      Acceptor has 1 Processor thread that has its own selector and read requests from the socket.
 *      1 Handler thread that handles requests and produces responses back to the processor thread for writing.
 * <p>
 * Kafka 支持两种类型的请求平面：
 *          - 数据平面：
 *            - 处理来自 clients 和集群中其他 broker 的请求。
 *            - 线程模型是
 * 1 个 Acceptor 线程，用于处理新连接。
 * 可以通过在 KafkaConfig 中的“Acceptor”中指定多个“,”分隔的 endpoints 来配置多个数据平面。
 * Acceptors 有 N 个 Processor 线程，每个 Processor 线程都有自己的 selector，并从 socketChannel 读取请求
 * M 个 Handler 线程，用于处理 request 并将 response 返回给 processor 线程进行写入。
 *        - 控制平面：
 *          - 处理来自 controller 的请求。这是可选的，可以通过指定“control.plane.listener.name”来配置。
 * 如果未配置，则 controller 请求将由「数据平面」处理。
 *          - 线程模型是
 * 1 个 Acceptor 线程，用于处理新连接
 * Acceptor 有 1 个 Processor 线程，该 Processor 线程有自己的 selector，并从 socketChannel 读取请求。
 * 1 个 Handler 线程，用于 request 并将 response 返回给 processor 线程进行写入。
 *
 */
class SocketServer(val config: KafkaConfig,
                   val metrics: Metrics,
                   val time: Time,
                   val credentialProvider: CredentialProvider,
                   val apiVersionManager: ApiVersionManager)
  extends Logging with KafkaMetricsGroup with BrokerReconfigurable {

  // 最大排队请求数
  private val maxQueuedRequests = config.queuedMaxRequests

  // nodeId
  private val nodeId = config.brokerId

  // 日志上下文
  private val logContext = new LogContext(s"[SocketServer listenerType=${apiVersionManager.listenerType}, nodeId=$nodeId] ")

  // 设置日志前缀
  this.logIdent = logContext.logPrefix

  /*
  内存池相关
   */
  // 内存池的指标相关内容
  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup)
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup)
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
  // 内存池
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE

  /*
  数据平面相关
   */
  // 数据平面处理器
  private val dataPlaneProcessors = new ConcurrentHashMap[Int, Processor]()
  // 数据平面接收器
  private[network] val dataPlaneAcceptors = new ConcurrentHashMap[EndPoint, Acceptor]()
  // 数据平面请求通道，请求通道（requestChannel）用于在 IO 线程和业务线程之间传递请求和响应
  val dataPlaneRequestChannel = new RequestChannel(maxQueuedRequests, DataPlaneMetricPrefix, time, apiVersionManager.newRequestMetrics)


  /*
  控制平面相关
   */
  // 控制平面处理器
  private var controlPlaneProcessorOpt : Option[Processor] = None
  // 控制平面接收器
  private[network] var controlPlaneAcceptorOpt : Option[Acceptor] = None
  // 控制平面请求通道
  val controlPlaneRequestChannelOpt: Option[RequestChannel] = config.controlPlaneListenerName.map(_ =>
    new RequestChannel(20, ControlPlaneMetricPrefix, time, apiVersionManager.newRequestMetrics))

  // 下一个 processorId
  private var nextProcessorId = 0
  // 连接配额
  val connectionQuotas = new ConnectionQuotas(config, time, metrics)
  // 标志位-是否开始处理请求
  private var startedProcessingRequests = false
  // 标志位-是否停止处理请求
  private var stoppedProcessingRequests = false

  /**
   * Starts the socket server and creates all the Acceptors and the Processors. The Acceptors
   * start listening at this stage so that the bound port is known when this method completes
   * even when ephemeral ports are used. Acceptors and Processors are started if `startProcessingRequests`
   * is true. If not, acceptors and processors are only started when [[kafka.network.SocketServer#startProcessingRequests()]]
   * is invoked. Delayed starting of acceptors and processors is used to delay processing client
   * connections until server is fully initialized, e.g. to ensure that all credentials have been
   * loaded before authentications are performed. Incoming connections on this server are processed
   * when processors start up and invoke [[org.apache.kafka.common.network.Selector#poll]].
   *
   * @param startProcessingRequests Flag indicating whether `Processor`s must be started.
   * @param controlPlaneListener    The control plane listener, or None if there is none.
   * @param dataPlaneListeners      The data plane listeners.
   */
  def startup(startProcessingRequests: Boolean = true,
              controlPlaneListener: Option[EndPoint] = config.controlPlaneListener,
              dataPlaneListeners: Seq[EndPoint] = config.dataPlaneListeners): Unit = {
    this.synchronized {
      // 创建「控制平面」的 Acceptor 和 Processor
      createControlPlaneAcceptorAndProcessor(controlPlaneListener)
      // 创建「数据平面」的 Acceptor 和 Processor
      createDataPlaneAcceptorsAndProcessors(config.numNetworkThreads, dataPlaneListeners)
      // 如果需要开始处理请求，则调用 startProcessingRequests 方法
      if (startProcessingRequests) {
        this.startProcessingRequests()
      }
    }

    // 下面是注册一些指标？
    newGauge(s"${DataPlaneMetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
      val ioWaitRatioMetricNames = dataPlaneProcessors.values.asScala.iterator.map { p =>
        metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
      }
      ioWaitRatioMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }.sum / dataPlaneProcessors.size
    })
    newGauge(s"${ControlPlaneMetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
      val ioWaitRatioMetricName = controlPlaneProcessorOpt.map { p =>
        metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
      }
      ioWaitRatioMetricName.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }.getOrElse(Double.NaN)
    })
    newGauge("MemoryPoolAvailable", () => memoryPool.availableMemory)
    newGauge("MemoryPoolUsed", () => memoryPool.size() - memoryPool.availableMemory)
    newGauge(s"${DataPlaneMetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
      val expiredConnectionsKilledCountMetricNames = dataPlaneProcessors.values.asScala.iterator.map { p =>
        metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
      }
      expiredConnectionsKilledCountMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
      }.sum
    })
    newGauge(s"${ControlPlaneMetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
      val expiredConnectionsKilledCountMetricNames = controlPlaneProcessorOpt.map { p =>
        metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
      }
      expiredConnectionsKilledCountMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
      }.getOrElse(0.0)
    })
  }

  /**
   * Start processing requests and new connections. This method is used for delayed starting of
   * all the acceptors and processors if [[kafka.network.SocketServer#startup]] was invoked with
   * `startProcessingRequests=false`.
   * <p>
   *   开始处理请求和新连接。如果 [[kafka.network.SocketServer#startup]] 被调用时 `startProcessingRequests=false`，
   *
   * Before starting processors for each endpoint, we ensure that authorizer has all the metadata
   * to authorize requests on that endpoint by waiting on the provided future. We start inter-broker
   * listener before other listeners. This allows authorization metadata for other listeners to be
   * stored in Kafka topics in this cluster.
   * <p>
   *   在为每个 endpoint 启动 processor 之前，我们确保 authorizer 有所有的元数据来授权该 endpoint 上的请求，
   *   通过等待提供的 future。我们在其他 listener 之前启动 inter-broker listener。这允许其他 listener 的授权元数据
   *   存储在此集群中的 Kafka 主题中。
   *
   * @param authorizerFutures Future per [[EndPoint]] used to wait before starting the processor
   *                          corresponding to the [[EndPoint]]
   */
  def startProcessingRequests(authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = Map.empty): Unit = {
    info("Starting socket server acceptors and processors")
    this.synchronized {
      // 第一次调用的时候，这个值是 false；所以会进入到 if 里面
      if (!startedProcessingRequests) {
        // 开启「控制平面」的 Processor 和 Acceptor
        startControlPlaneProcessorAndAcceptor(authorizerFutures)
        // 开启「数据平面」的 Processor 和 Acceptor
        startDataPlaneProcessorsAndAcceptors(authorizerFutures)
        startedProcessingRequests = true
      } else {
        info("Socket server acceptors and processors already started")
      }
    }
    info("Started socket server acceptors and processors")
  }

  /**
   * Starts processors of the provided acceptor and the acceptor itself.
   *
   * Before starting them, we ensure that authorizer has all the metadata to authorize
   * requests on that endpoint by waiting on the provided future.
   */
  private def startAcceptorAndProcessors(threadPrefix: String,
                                         endpoint: EndPoint,
                                         acceptor: Acceptor,
                                         authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = Map.empty): Unit = {
    debug(s"Wait for authorizer to complete start up on listener ${endpoint.listenerName}")
    waitForAuthorizerFuture(acceptor, authorizerFutures)
    debug(s"Start processors on listener ${endpoint.listenerName}")
    acceptor.startProcessors(threadPrefix)
    debug(s"Start acceptor thread on listener ${endpoint.listenerName}")
    if (!acceptor.isStarted()) {
      KafkaThread.nonDaemon(
        s"${threadPrefix}-kafka-socket-acceptor-${endpoint.listenerName}-${endpoint.securityProtocol}-${endpoint.port}",
        acceptor
      ).start()
      acceptor.awaitStartup()
    }
    info(s"Started $threadPrefix acceptor and processor(s) for endpoint : ${endpoint.listenerName}")
  }

  /**
   * Starts processors of all the data-plane acceptors and all the acceptors of this server.
   *
   * We start inter-broker listener before other listeners. This allows authorization metadata for
   * other listeners to be stored in Kafka topics in this cluster.
   */
  private def startDataPlaneProcessorsAndAcceptors(authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {
    val interBrokerListener = dataPlaneAcceptors.asScala.keySet
      .find(_.listenerName == config.interBrokerListenerName)
    val orderedAcceptors = interBrokerListener match {
      case Some(interBrokerListener) => List(dataPlaneAcceptors.get(interBrokerListener)) ++
        dataPlaneAcceptors.asScala.filter { case (k, _) => k != interBrokerListener }.values
      case None => dataPlaneAcceptors.asScala.values
    }
    orderedAcceptors.foreach { acceptor =>
      val endpoint = acceptor.endPoint
      startAcceptorAndProcessors(DataPlaneThreadPrefix, endpoint, acceptor, authorizerFutures)
    }
  }

  /**
   * Start the processor of control-plane acceptor and the acceptor of this server.
   */
  private def startControlPlaneProcessorAndAcceptor(authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {
    controlPlaneAcceptorOpt.foreach { controlPlaneAcceptor =>
      val endpoint = config.controlPlaneListener.get
      startAcceptorAndProcessors(ControlPlaneThreadPrefix, endpoint, controlPlaneAcceptor, authorizerFutures)
    }
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

  private def createDataPlaneAcceptorsAndProcessors(dataProcessorsPerListener: Int,
                                                    endpoints: Seq[EndPoint]): Unit = {
    endpoints.foreach { endpoint =>
      // 将当前 endpoint 添加到配额管理器中
      connectionQuotas.addListener(config, endpoint.listenerName)
      // 创建 acceptor
      val dataPlaneAcceptor = createAcceptor(endpoint, DataPlaneMetricPrefix)
      // 创建 processor 并添加到 requestChannel 中
      addDataPlaneProcessors(dataPlaneAcceptor, endpoint, dataProcessorsPerListener)
      dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
      info(s"Created data-plane acceptor and processors for endpoint : ${endpoint.listenerName}")
    }
  }

  /**
   * 创建「控制平面」的 Acceptor 和 Processor
   *
   * @param endpointOpt 可选的终端点
   */
  private def createControlPlaneAcceptorAndProcessor(endpointOpt: Option[EndPoint]): Unit = {
    // 遍历每个 endpoints
    endpointOpt.foreach { endpoint =>
      // 将当前 endpoint 添加到配额管理器中
      connectionQuotas.addListener(config, endpoint.listenerName)
      // 创建 acceptor
      val controlPlaneAcceptor = createAcceptor(endpoint, ControlPlaneMetricPrefix)
      // 创建 processor
      val controlPlaneProcessor = newProcessor(nextProcessorId, controlPlaneRequestChannelOpt.get,
        connectionQuotas, endpoint.listenerName, endpoint.securityProtocol, memoryPool, isPrivilegedListener = true)

      // 将 acceptor 和 processor 转为 Option，并设置到成员变量中
      controlPlaneAcceptorOpt = Some(controlPlaneAcceptor)
      controlPlaneProcessorOpt = Some(controlPlaneProcessor)

      // 创建 processor list 并添加 processor
      val listenerProcessors = new ArrayBuffer[Processor]()
      listenerProcessors += controlPlaneProcessor
      // 遍历控制相关的 requestChannel，添加 processor
      controlPlaneRequestChannelOpt.foreach(_.addProcessor(controlPlaneProcessor))
      // 增加 processor id
      nextProcessorId += 1
      // 将 processor 添加到 acceptor 中
      controlPlaneAcceptor.addProcessors(listenerProcessors, ControlPlaneThreadPrefix)
      info(s"Created control-plane acceptor and processor for endpoint : ${endpoint.listenerName}")
    }
  }

  private def createAcceptor(endPoint: EndPoint, metricPrefix: String) : Acceptor = {
    val sendBufferSize = config.socketSendBufferBytes
    val recvBufferSize = config.socketReceiveBufferBytes
    new Acceptor(endPoint, sendBufferSize, recvBufferSize, nodeId, connectionQuotas, metricPrefix, time)
  }

  private def addDataPlaneProcessors(acceptor: Acceptor, endpoint: EndPoint, newProcessorsPerListener: Int): Unit = {
    val listenerName = endpoint.listenerName
    val securityProtocol = endpoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()
    val isPrivilegedListener = controlPlaneRequestChannelOpt.isEmpty && config.interBrokerListenerName == listenerName

    for (_ <- 0 until newProcessorsPerListener) {
      val processor = newProcessor(nextProcessorId, dataPlaneRequestChannel, connectionQuotas,
        listenerName, securityProtocol, memoryPool, isPrivilegedListener)
      listenerProcessors += processor
      dataPlaneRequestChannel.addProcessor(processor)
      nextProcessorId += 1
    }
    listenerProcessors.foreach(p => dataPlaneProcessors.put(p.id, p))
    acceptor.addProcessors(listenerProcessors, DataPlaneThreadPrefix)
  }

  /**
   * Stop processing requests and new connections.
   */
  def stopProcessingRequests(): Unit = {
    info("Stopping socket server request processors")
    this.synchronized {
      dataPlaneAcceptors.asScala.values.foreach(_.initiateShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.awaitShutdown())
      controlPlaneAcceptorOpt.foreach(_.initiateShutdown())
      controlPlaneAcceptorOpt.foreach(_.awaitShutdown())
      dataPlaneRequestChannel.clear()
      controlPlaneRequestChannelOpt.foreach(_.clear())
      stoppedProcessingRequests = true
    }
    info("Stopped socket server request processors")
  }

  def resizeThreadPool(oldNumNetworkThreads: Int, newNumNetworkThreads: Int): Unit = synchronized {
    info(s"Resizing network thread pool size for each data-plane listener from $oldNumNetworkThreads to $newNumNetworkThreads")
    if (newNumNetworkThreads > oldNumNetworkThreads) {
      dataPlaneAcceptors.forEach { (endpoint, acceptor) =>
        addDataPlaneProcessors(acceptor, endpoint, newNumNetworkThreads - oldNumNetworkThreads)
      }
    } else if (newNumNetworkThreads < oldNumNetworkThreads)
      dataPlaneAcceptors.asScala.values.foreach(_.removeProcessors(oldNumNetworkThreads - newNumNetworkThreads, dataPlaneRequestChannel))
  }

  /**
   * Shutdown the socket server. If still processing requests, shutdown
   * acceptors and processors first.
   */
  def shutdown(): Unit = {
    info("Shutting down socket server")
    this.synchronized {
      if (!stoppedProcessingRequests)
        stopProcessingRequests()
      dataPlaneRequestChannel.shutdown()
      controlPlaneRequestChannelOpt.foreach(_.shutdown())
      connectionQuotas.close()
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      val acceptor = dataPlaneAcceptors.get(endpoints(listenerName))
      if (acceptor != null) {
        acceptor.serverChannel.socket.getLocalPort
      } else {
        controlPlaneAcceptorOpt.map (_.serverChannel.socket().getLocalPort).getOrElse(throw new KafkaException("Could not find listenerName : " + listenerName + " in data-plane or control-plane"))
      }
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    info(s"Adding data-plane listeners for endpoints $listenersAdded")
    createDataPlaneAcceptorsAndProcessors(config.numNetworkThreads, listenersAdded)
    listenersAdded.foreach { endpoint =>
      val acceptor = dataPlaneAcceptors.get(endpoint)
      startAcceptorAndProcessors(DataPlaneThreadPrefix, endpoint, acceptor)
    }
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing data-plane listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      connectionQuotas.removeListener(config, endpoint.listenerName)
      dataPlaneAcceptors.asScala.remove(endpoint).foreach { acceptor =>
        acceptor.initiateShutdown()
        acceptor.awaitShutdown()
      }
    }
  }

  override def reconfigurableConfigs: Set[String] = SocketServer.ReconfigurableConfigs

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {

  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val maxConnectionsPerIp = newConfig.maxConnectionsPerIp
    if (maxConnectionsPerIp != oldConfig.maxConnectionsPerIp) {
      info(s"Updating maxConnectionsPerIp: $maxConnectionsPerIp")
      connectionQuotas.updateMaxConnectionsPerIp(maxConnectionsPerIp)
    }
    val maxConnectionsPerIpOverrides = newConfig.maxConnectionsPerIpOverrides
    if (maxConnectionsPerIpOverrides != oldConfig.maxConnectionsPerIpOverrides) {
      info(s"Updating maxConnectionsPerIpOverrides: ${maxConnectionsPerIpOverrides.map { case (k, v) => s"$k=$v" }.mkString(",")}")
      connectionQuotas.updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides)
    }
    val maxConnections = newConfig.maxConnections
    if (maxConnections != oldConfig.maxConnections) {
      info(s"Updating broker-wide maxConnections: $maxConnections")
      connectionQuotas.updateBrokerMaxConnections(maxConnections)
    }
    val maxConnectionRate = newConfig.maxConnectionCreationRate
    if (maxConnectionRate != oldConfig.maxConnectionCreationRate) {
      info(s"Updating broker-wide maxConnectionCreationRate: $maxConnectionRate")
      connectionQuotas.updateBrokerMaxConnectionRate(maxConnectionRate)
    }
  }

  private def waitForAuthorizerFuture(acceptor: Acceptor,
                                      authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {
    //we can't rely on authorizerFutures.get() due to ephemeral ports. Get the future using listener name
    authorizerFutures.forKeyValue { (endpoint, future) =>
      if (endpoint.listenerName == Optional.of(acceptor.endPoint.listenerName.value))
        future.join()
    }
  }

  // `protected` for test usage
  protected[network] def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol, memoryPool: MemoryPool, isPrivilegedListener: Boolean): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      config.failedAuthenticationDelayMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider,
      memoryPool,
      logContext,
      Processor.ConnectionQueueSize,
      isPrivilegedListener,
      apiVersionManager
    )
  }

  // For test usage
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  // For test usage
  private[network] def dataPlaneProcessor(index: Int): Processor = dataPlaneProcessors.get(index)

}

object SocketServer {
  val MetricsGroup = "socket-server-metrics"
  val DataPlaneThreadPrefix = "data-plane"
  val ControlPlaneThreadPrefix = "control-plane"
  val DataPlaneMetricPrefix = ""
  val ControlPlaneMetricPrefix = "ControlPlane"

  val ReconfigurableConfigs = Set(
    KafkaConfig.MaxConnectionsPerIpProp,
    KafkaConfig.MaxConnectionsPerIpOverridesProp,
    KafkaConfig.MaxConnectionsProp,
    KafkaConfig.MaxConnectionCreationRateProp)

  val ListenerReconfigurableConfigs = Set(KafkaConfig.MaxConnectionsProp, KafkaConfig.MaxConnectionCreationRateProp)
}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop
   */
  def initiateShutdown(): Unit = {
    if (alive.getAndSet(false))
      wakeup()
  }

  /**
   * Wait for the thread to completely shutdown
   */
  def awaitShutdown(): Unit = shutdownLatch.await

  /**
   * Returns true if the thread is completely started
   */
  def isStarted(): Boolean = startupLatch.getCount == 0

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   * <p>
   *   记录线程启动完成
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    // 用一个关闭的 latch 替换一个打开的 latch
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(listenerName: ListenerName, channel: SocketChannel): Unit = {
    if (channel != null) {
      debug(s"Closing connection from ${channel.socket.getRemoteSocketAddress}")
      connectionQuotas.dec(listenerName, channel.socket.getInetAddress)
      closeSocket(channel)
    }
  }

  protected def closeSocket(channel: SocketChannel): Unit = {
    CoreUtils.swallow(channel.socket().close(), this, Level.ERROR)
    CoreUtils.swallow(channel.close(), this, Level.ERROR)
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 * <p>
 *   接受和配置新连接的线程。
 *   每个 endpoint 都有一个这样的线程。
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              nodeId: Int,
                              connectionQuotas: ConnectionQuotas,
                              metricPrefix: String,
                              time: Time,
                              logPrefix: String = "") extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  this.logIdent = logPrefix
  // nio 的 selector
  // 这个 selector 上只会注册 acceptor 的 serverSocketChannel；所以它只会关注 OP_ACCEPT 事件
  private val nioSelector = NSelector.open()
  // nio 的 serverSocketChannel
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  // 当前 acceptor 对应的 processor 列表
  private val processors = new ArrayBuffer[Processor]()

  // 标志位-当前 acceptor 对应的 processor 是否已经启动
  private val processorsStarted = new AtomicBoolean
  private val blockedPercentMeter = newMeter(s"${metricPrefix}AcceptorBlockedPercent",
    "blocked time", TimeUnit.NANOSECONDS, Map(ListenerMetricTag -> endPoint.listenerName.value))
  private var currentProcessorIndex = 0
  private[network] val throttledSockets = new mutable.PriorityQueue[DelayedCloseSocket]()

  private[network] case class DelayedCloseSocket(socket: SocketChannel, endThrottleTimeMs: Long) extends Ordered[DelayedCloseSocket] {
    override def compare(that: DelayedCloseSocket): Int = endThrottleTimeMs compare that.endThrottleTimeMs
  }

  private[network] def addProcessors(newProcessors: Buffer[Processor], processorThreadPrefix: String): Unit = synchronized {
    processors ++= newProcessors
    if (processorsStarted.get)
      startProcessors(newProcessors, processorThreadPrefix)
  }

  private[network] def startProcessors(processorThreadPrefix: String): Unit = synchronized {
    if (!processorsStarted.getAndSet(true)) {
      startProcessors(processors, processorThreadPrefix)
    }
  }

  private def startProcessors(processors: Seq[Processor], processorThreadPrefix: String): Unit = synchronized {
    // 遍历每个 processor，启动线程
    processors.foreach { processor =>
      KafkaThread.nonDaemon(
        s"${processorThreadPrefix}-kafka-network-thread-$nodeId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor
      ).start()
    }
  }

  private[network] def removeProcessors(removeCount: Int, requestChannel: RequestChannel): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.initiateShutdown())
    toRemove.foreach(_.awaitShutdown())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  override def initiateShutdown(): Unit = {
    super.initiateShutdown()
    synchronized {
      processors.foreach(_.initiateShutdown())
    }
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    synchronized {
      processors.foreach(_.awaitShutdown())
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   * <p>
   *   循环的 accept，处理新的 connection
   */
  def run(): Unit = {
    // 将当前的 serverSocketChannel 注册到 nioSelector 中，并监听 OP_ACCEPT 事件
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()
    try {
      while (isRunning) {
        try {
          // 接受新的连接
          acceptNewConnections()
          // 关闭超时的连接
          closeThrottledConnections()
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket, selector, and any throttled sockets.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      throttledSockets.foreach(throttledSocket => closeSocket(throttledSocket.socket))
      throttledSockets.clear()
      shutdownComplete()
    }
  }

  /**
   * Create a server socket to listen for connections on.
   * <p>
   *   创建一个 server socket 用于监听连接
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    // 创建一个 InetSocketAddress
    val socketAddress =
      if (Utils.isBlank(host)) {
        new InetSocketAddress(port)
      } else {
        new InetSocketAddress(host, port)
      }

    // 创建一个 ServerSocketChannel
    val serverChannel = ServerSocketChannel.open()
    // 配置 ServerSocketChannel
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.socket.bind(socketAddress)
      info(s"Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
    } catch {
      case e: SocketException =>
        throw new KafkaException(s"Socket server failed to bind to ${socketAddress.getHostString}:$port: ${e.getMessage}.", e)
    }
    serverChannel
  }

  /**
   * Listen for new connections and assign accepted connections to processors using round-robin.
   * <p>
   *   监听新的连接，并使用轮询法将接受的连接分配给 processors
   */
  private def acceptNewConnections(): Unit = {
    // 交给 nioSelector 执行 select 操作，最多阻塞 500ms
    val ready = nioSelector.select(500)
    // 如果有新的 OP_ACCEPT 事件
    if (ready > 0) {
      // 获取到 SelectionKey，selectionKey 代表 channel 注册到 selector 上的 key
      val keys = nioSelector.selectedKeys()
      val iter = keys.iterator()
      // 遍历每个新增的 connection
      while (iter.hasNext && isRunning) {
        try {
          val key = iter.next
          iter.remove()

          if (key.isAcceptable) {
            // 接受 connection，并转为 SocketChannel（此时尚未 register）
            accept(key).foreach { socketChannel =>
              // Assign the channel to the next processor (using round-robin) to which the
              // channel can be added without blocking. If newConnections queue is full on
              // all processors, block until the last one is able to accept a connection.

              // 将新的 SocketChannel 分配给下一个 processor（使用轮询法），
              // 如果所有 processor 的 newConnections 队列都已满，则阻塞，直到最后一个 processor 能够接受连接

              // 计算可重试的次数，即 processors 的数量
              var retriesLeft = synchronized(processors.length)
              var processor: Processor = null
              do {
                retriesLeft -= 1
                // 分配一个 processor
                processor = synchronized {
                  // adjust the index (if necessary) and retrieve the processor atomically for
                  // correct behaviour in case the number of processors is reduced dynamically
                  // 如果 processors 的数量动态减少，则需要调整索引（如果有必要），并原子地检索 processor，以确保正确的行为
                  currentProcessorIndex = currentProcessorIndex % processors.length
                  processors(currentProcessorIndex)
                }
                currentProcessorIndex += 1

                // 尝试将这个新的 SocketChannel 分配给 processor
              } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0))
            }
          } else
            throw new IllegalStateException("Unrecognized key state for acceptor thread.")
        } catch {
          case e: Throwable => error("Error while accepting connection", e)
        }
      }
    }
  }

  
  /**
   * 接受新的连接
   *
   * @param key SelectionKey，代表 channel 注册到 selector 上的 key
   * @return 返回一个 Option[SocketChannel]，如果成功接受连接则返回 Some(SocketChannel)，否则返回 None
   */
  private def accept(key: SelectionKey): Option[SocketChannel] = {
    // 获取 ServerSocketChannel
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    // 接受连接，返回 SocketChannel
    val socketChannel = serverSocketChannel.accept()
    try {
      // 更新配额信息
      connectionQuotas.inc(endPoint.listenerName, socketChannel.socket.getInetAddress, blockedPercentMeter)
      /*
      配置新增的 SocketChannel
       */
      // 配置非阻塞模式
      socketChannel.configureBlocking(false)
      // 设置 TCP NoDelay
      socketChannel.socket().setTcpNoDelay(true)
      // 设置 KeepAlive
      socketChannel.socket().setKeepAlive(true)
      // 设置发送缓冲区大小
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)
      // 返回 Option[SocketChannel]
      Some(socketChannel)
    } catch {
      // 处理 TooManyConnectionsException 异常，一般是由配额限制导致的
      case e: TooManyConnectionsException =>
        info(s"Rejected connection from ${e.ip}, address already has the configured maximum of ${e.count} connections.")
        close(endPoint.listenerName, socketChannel)
        None
      // 处理 ConnectionThrottledException 异常
      case e: ConnectionThrottledException =>
        val ip = socketChannel.socket.getInetAddress
        debug(s"Delaying closing of connection from $ip for ${e.throttleTimeMs} ms")
        val endThrottleTimeMs = e.startThrottleTimeMs + e.throttleTimeMs
        throttledSockets += DelayedCloseSocket(socketChannel, endThrottleTimeMs)
        None
    }
  }

  /**
   * Close sockets for any connections that have been throttled.
   * <p>
   *   关闭所有已经被限流的连接
   */
  private def closeThrottledConnections(): Unit = {
    val timeMs = time.milliseconds
    while (throttledSockets.headOption.exists(_.endThrottleTimeMs < timeMs)) {
      val closingSocket = throttledSockets.dequeue()
      debug(s"Closing socket from ip ${closingSocket.socket.getRemoteAddress}")
      closeSocket(closingSocket.socket)
    }
  }

  private def assignNewConnection(socketChannel: SocketChannel, processor: Processor, mayBlock: Boolean): Boolean = {

    // 调用 processor 的 accept 方法，将 SocketChannel 记录到 processor 的 newConnections 队列中
    if (processor.accept(socketChannel, mayBlock, blockedPercentMeter)) {
      debug(s"Accepted connection from ${socketChannel.socket.getRemoteSocketAddress} on" +
        s" ${socketChannel.socket.getLocalSocketAddress} and assigned it to processor ${processor.id}," +
        s" sendBufferSize [actual|requested]: [${socketChannel.socket.getSendBufferSize}|$sendBufferSize]" +
        s" recvBufferSize [actual|requested]: [${socketChannel.socket.getReceiveBufferSize}|$recvBufferSize]")
      true
    } else
      false
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup(): Unit = nioSelector.wakeup()

}

private[kafka] object Processor {
  val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"
  val ConnectionQueueSize = 20
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 * <p>
 * 处理一个 connection 的所有 request 的线程。
 *   有 N 个这样的线程并行运行，每个线程都有自己的 selector
 *
 * @param isPrivilegedListener The privileged listener flag is used as one factor to determine whether
 *                             a certain request is forwarded or not. When the control plane is defined,
 *                             the control plane processor would be fellow broker's choice for sending
 *                             forwarding requests; if the control plane is not defined, the processor
 *                             relying on the inter broker listener would be acting as the privileged listener.
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               failedAuthenticationDelayMs: Int,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               config: KafkaConfig,
                               metrics: Metrics,
                               credentialProvider: CredentialProvider,
                               memoryPool: MemoryPool,
                               logContext: LogContext,
                               connectionQueueSize: Int,
                               isPrivilegedListener: Boolean,
                               apiVersionManager: ApiVersionManager) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {
  // 寻找 endpoint
  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote, index) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort, Integer.parseInt(index))
        }
      }
      case _ => None
    }
  }

  private[network] case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
  }

  // 用于记录需要当前 processor 处理的连接，通过这个 blockingQueue 来解耦 acceptor 和 processor 的具体实现
  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)
  // 已发送的响应（未收到回应）
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  // 一些指标
  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  newGauge(IdlePercentMetricName, () => {
    Option(metrics.metric(metrics.metricName("io-wait-ratio", MetricsGroup, metricTags))).fold(0.0)(m =>
      Math.min(m.metricValue.asInstanceOf[Double], 1.0))
  },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString)
  )

  // 因超时而关闭的连接数
  val expiredConnectionsKilledCount = new CumulativeSum()
  private val expiredConnectionsKilledCountMetricName = metrics.metricName("expired-connections-killed-count", MetricsGroup, metricTags)
  metrics.addMetric(expiredConnectionsKilledCountMetricName, expiredConnectionsKilledCount)

  // 当前 processor 对应的 selector，用于处理真正的读写事件
  private val selector = createSelector(
    ChannelBuilders.serverChannelBuilder(
      listenerName,
      listenerName == config.interBrokerListenerName,
      securityProtocol,
      config,
      credentialProvider.credentialCache,
      credentialProvider.tokenCache,
      time,
      logContext,
      () => apiVersionManager.apiVersionResponse(throttleTimeMs = 0)
    )
  )

  // Visible to override for testing
  protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }
    new KSelector(
      maxRequestSize,
      connectionsMaxIdleMs,
      failedAuthenticationDelayMs,
      metrics,
      time,
      "socket-server",
      metricTags,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
  // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
  // closed, connection ids are not reused while requests from the closed connection are being processed.

  // Connection ids 的格式为 `localAddr:localPort-remoteAddr:remotePort-index`。
  // index 是一个非负递增值，它确保即使在连接关闭后 remotePort 被重用，当处理来自关闭连接的请求时，连接 id 不会被重用。
  private var nextConnectionIndex = 0

  override def run(): Unit = {
    // 更新自身维护的代表 shutdown 状态和 startup 状态的两个 countdownLatch
    startupComplete()
    try {
      while (isRunning) {
        try {
          // setup any new connections that have been queued up
          // 处理新的连接，即将新的 SocketChannel 注册到当前 processor 的 selector 上
          configureNewConnections()
          // register any new responses for writing
          // 处理新的响应
          processNewResponses()
          // 触发一次 selector 的 poll 操作
          poll()
          // 处理已经完成的请求
          processCompletedReceives()
          // 处理已经完成的发送
          processCompletedSends()
          // 处理已经断开的连接
          processDisconnected()
          // 处理已经关闭的连接
          closeExcessConnections()
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug(s"Closing selector - processor $id")
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  private[network] def processException(errorMessage: String, throwable: Throwable): Unit = {
    throwable match {
      case e: ControlThrowable => throw e
      case e => error(errorMessage, e)
    }
  }

  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable): Unit = {
    if (openOrClosingChannel(channelId).isDefined) {
      error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }

  private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    // 遍历每个新的响应，这指的是服务端将要发给客户端的响应，即客户端请求的结果
    // response 是由 requestChannel 完成业务逻辑后投递给 processor 的
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      // 确定 response 对应的 socketChannel
      val channelId = currentResponse.request.context.connectionId
      try {
        currentResponse match {
          // 如果是 NoOpResponse
          case response: NoOpResponse =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer

            // 不需要向客户端发送响应，我们需要读取服务器 socket 缓冲区中等待处理的更多流水线请求
            updateRequestMetrics(response)
            trace(s"Socket server received empty response to send, registering for read: $response")
            // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
            // it will be unmuted immediately. If the channel has been throttled, it will be unmuted only if the
            // throttling delay has already passed by now.

            // 尝试取消静音 channel。
            // 如果没有配额违规，且 channel 未被限流，则会立即取消静音。
            // 如果 channel 被限流，则只有在限流延迟已经过去时才会取消静音。
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)

          // 如果是 SendResponse
          case response: SendResponse =>
            // 发送响应
            sendResponse(response, response.responseSend)

          // 如果是 CloseConnectionResponse
          case response: CloseConnectionResponse =>
            updateRequestMetrics(response)
            trace("Closing socket connection actively according to the response code.")
            // 关闭连接
            close(channelId)

          // 如果是 StartThrottlingResponse 或 EndThrottlingResponse
          case _: StartThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)
          case _: EndThrottlingResponse =>
            // Try unmuting the channel. The channel will be unmuted only if the response has already been sent out to
            // the client.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
            tryUnmuteChannel(channelId)

          // 否则，如果是其他 response 类型，抛异常
          case _ =>
            throw new IllegalArgumentException(s"Unknown response type: ${currentResponse.getClass}")
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
  }

  // `protected` for test usage
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send): Unit = {
    // 获取到 connectionId
    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long

    // 如果连接被远程关闭，或者 selector 因为空闲时间过长而关闭连接，`channel` 可能为 None
    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L, response)
    }
    // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
    // removed from the Selector after discarding any pending staged receives.
    // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long

    // 调用 send 方法关闭 channel，以便正确关闭 channel 并在丢弃任何挂起的 staged receives 后从 Selector 中删除 channel
    // `openOrClosingChannel` 可能为 None，如果 selector 因为空闲时间过长而关闭连接
    if (openOrClosingChannel(connectionId).isDefined) {
      // 将 send 信息放到 channel 的缓冲区中
      selector.send(new NetworkSend(connectionId, responseSend))
      // 将 response 记录到 inflightResponses 中
      inflightResponses += (connectionId -> response)
    }
  }

  private def poll(): Unit = {
    // 根据是否存在新的连接，决定是否阻塞 poll
    val pollTimeout = if (newConnections.isEmpty) 300 else 0
    // 触发 selector 的 poll 操作
    try selector.poll(pollTimeout)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed", e)
    }
  }

  protected def parseRequestHeader(buffer: ByteBuffer): RequestHeader = {
    val header = RequestHeader.parse(buffer)
    if (apiVersionManager.isApiEnabled(header.apiKey)) {
      header
    } else {
      throw new InvalidRequestException(s"Received request api key ${header.apiKey} which is not enabled")
    }
  }

  private def processCompletedReceives(): Unit = {
    selector.completedReceives.forEach { receive =>
      try {
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            // 解析请求的 header 部分
            val header = parseRequestHeader(receive.payload)
            // 如果是 SASL_HANDSHAKE 请求，并且 channel 可能开始重新认证，则打印日志
            if (header.apiKey == ApiKeys.SASL_HANDSHAKE &&
              channel.maybeBeginServerReauthentication(receive, () => time.nanoseconds()))
              trace(s"Begin re-authentication: $channel")
            else {
              val nowNanos = time.nanoseconds()
              // 如果 SASL 校验过期，则关闭 channel
              if (channel.serverAuthenticationSessionExpired(nowNanos)) {
                // be sure to decrease connection count and drop any in-flight responses
                debug(s"Disconnecting expired channel: $channel : $header")
                close(channel.id)
                expiredConnectionsKilledCount.record(null, 1, 0)
              } else {
                val connectionId = receive.source
                val context = new RequestContext(header, connectionId, channel.socketAddress,
                  channel.principal, listenerName, securityProtocol,
                  channel.channelMetadataRegistry.clientInformation, isPrivilegedListener, channel.principalSerde)

                // 将 NetworkReceive 转为 RequestChannel.Request 对象，并投递给 requestChannel 处理
                val req = new RequestChannel.Request(processor = id, context = context,
                  startTimeNanos = nowNanos, memoryPool, receive.payload, requestChannel.metrics, None)

                // KIP-511: ApiVersionsRequest is intercepted here to catch the client software name
                // and version. It is done here to avoid wiring things up to the api layer.
                if (header.apiKey == ApiKeys.API_VERSIONS) {
                  val apiVersionsRequest = req.body[ApiVersionsRequest]
                  if (apiVersionsRequest.isValid) {
                    channel.channelMetadataRegistry.registerClientInformation(new ClientInformation(
                      apiVersionsRequest.data.clientSoftwareName,
                      apiVersionsRequest.data.clientSoftwareVersion))
                  }
                }
                // 将请求交给 requestChannel 处理
                requestChannel.sendRequest(req)
                selector.mute(connectionId)
                handleChannelMuteEvent(connectionId, ChannelMuteEvent.REQUEST_RECEIVED)
              }
            }
          case None =>
            // This should never happen since completed receives are processed immediately after `poll()`
            throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
        }
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
    // 移除已经处理的 receive
    selector.clearCompletedReceives()
  }

  private def processCompletedSends(): Unit = {
    selector.completedSends.forEach { send =>
      try {
        // 将已经成功发出的响应从 inflightResponses 中移除
        val response = inflightResponses.remove(send.destinationId).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destinationId} completed, but not in `inflightResponses`")
        }
        updateRequestMetrics(response)

        // Invoke send completion callback
        // 调用 send 完成的回调
        response.onComplete.foreach(onComplete => onComplete(send))

        // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
        // it will be unmuted immediately. If the channel has been throttled, it will unmuted only if the throttling
        // delay has already passed by now.

        // 尝试取消静音 channel。
        // 如果没有配额违规，且 channel 未被限流，则会立即取消静音。
        // 如果 channel 被限流，则只有在限流延迟已经过去时才会取消静音。
        handleChannelMuteEvent(send.destinationId, ChannelMuteEvent.RESPONSE_SENT)
        tryUnmuteChannel(send.destinationId)
      } catch {
        case e: Throwable => processChannelException(send.destinationId,
          s"Exception while processing completed send to ${send.destinationId}", e)
      }
    }
    selector.clearCompletedSends()
  }

  private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  private def processDisconnected(): Unit = {
    selector.disconnected.keySet.forEach { connectionId =>
      try {
        val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        // 在 inflightResponses 中移除 connectionId 对应的响应，并更新 requestMetrics
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }

  private def closeExcessConnections(): Unit = {
    if (connectionQuotas.maxConnectionsExceeded(listenerName)) {
      val channel = selector.lowestPriorityChannel()
      if (channel != null)
        close(channel.id)
    }
  }

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * The channel will be immediately removed from the selector's `channels` or `closingChannels`
   * and no further disconnect notifications will be sent for this channel by the selector.
   * If responses are pending for the channel, they are dropped and metrics is updated.
   * If the channel has already been removed from selector, no action is taken.
   */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      // 更新配额
      if (address != null) {
        connectionQuotas.dec(listenerName, address)
      }
      // 关闭 channel
      selector.close(connectionId)

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * Queue up a new connection for reading
   * <p>
   *   将新的连接加入到 newConnections 队列中；
   *   具体会在 processor 的 run 方法中将 newConnections 队列中的连接注册到 selector 上
   */
  def accept(socketChannel: SocketChannel,
             mayBlock: Boolean,
             acceptorIdlePercentMeter: com.yammer.metrics.core.Meter): Boolean = {
    val accepted = {
      // 将新的 SocketChannel 加入到 newConnections 队列中
      if (newConnections.offer(socketChannel)) {
        true

        // 如果可以阻塞，那么就尝试阻塞的方式将新的 SocketChannel 加入到 newConnections 队列中
      } else if (mayBlock) {
        val startNs = time.nanoseconds
        newConnections.put(socketChannel)
        acceptorIdlePercentMeter.mark(time.nanoseconds() - startNs)
        true
      } else
        false
    }

    // 如果添加成功了，则唤醒 Processor 中的 selector
    if (accepted)
      wakeup()
    accepted
  }

  /**
   * Register any new connections that have been queued up. The number of connections processed
   * in each iteration is limited to ensure that traffic and connection close notifications of
   * existing channels are handled promptly.
   * <p>
   *   注册已经排队的新连接。
   *   每次迭代处理的连接数量是有限的，以确保及时处理现有通道的流量和连接关闭通知。
   */
  private def configureNewConnections(): Unit = {
    var connectionsProcessed = 0
    // 如果 newConnections 队列不为空，且 connectionsProcessed 小于 connectionQueueSize
    while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty) {
      // 从队列中取出一个新的 SocketChannel
      val channel = newConnections.poll()
      try {
        // 将这个新的 SocketChannel 注册到 selector 上
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        selector.register(connectionId(channel.socket), channel)
        connectionsProcessed += 1
      } catch {
        // We explicitly catch all exceptions and close the socket to avoid a socket leak.
        // 我们明确捕获所有异常，并关闭 socket，以避免 socket 泄漏。
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // need to close the channel here to avoid a socket leak.
          // 需要在这里关闭 channel，以避免 socket 泄漏。
          close(listenerName, channel)
          processException(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll(): Unit = {
    while (!newConnections.isEmpty) {
      newConnections.poll().close()
    }
    selector.channels.forEach { channel =>
      close(channel.id)
    }
    selector.close()
    removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString))
  }

  // 'protected` to allow override for testing
  protected[network] def connectionId(socket: Socket): String = {
    val localHost = socket.getLocalAddress.getHostAddress
    val localPort = socket.getLocalPort
    val remoteHost = socket.getInetAddress.getHostAddress
    val remotePort = socket.getPort
    val connId = ConnectionId(localHost, localPort, remoteHost, remotePort, nextConnectionIndex).toString
    nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
    connId
  }

  private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
    // 由 requestChannel 调用，将 response 加入到 responseQueue 中
    responseQueue.put(response)
    wakeup()
  }

  private def dequeueResponse(): RequestChannel.Response = {
    val response = responseQueue.poll()
    if (response != null)
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  private[network] def responseQueueSize = responseQueue.size

  // Only for testing
  private[network] def inflightResponseCount: Int = inflightResponses.size

  // Visible for testing
  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  // Indicate the specified channel that the specified channel mute-related event has happened so that it can change its
  // mute state.
  // 指示指定的 channel 发生了指定的 channel 静音相关事件，以便它可以更改其静音状态。
  private def handleChannelMuteEvent(connectionId: String, event: ChannelMuteEvent): Unit = {
    openOrClosingChannel(connectionId).foreach(c => c.handleChannelMuteEvent(event))
  }

  private def tryUnmuteChannel(connectionId: String) = {
    openOrClosingChannel(connectionId).foreach(c => selector.unmute(c.id))
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  override def wakeup() = selector.wakeup()

  override def initiateShutdown(): Unit = {
    super.initiateShutdown()
    removeMetric("IdlePercent", Map("networkProcessor" -> id.toString))
    metrics.removeMetric(expiredConnectionsKilledCountMetricName)
  }
}

/**
 * Interface for connection quota configuration. Connection quotas can be configured at the
 * broker, listener or IP level.
 */
sealed trait ConnectionQuotaEntity {
  def sensorName: String
  def metricName: String
  def sensorExpiration: Long
  def metricTags: Map[String, String]
}

object ConnectionQuotas {
  private val InactiveSensorExpirationTimeSeconds = TimeUnit.HOURS.toSeconds(1)
  private val ConnectionRateSensorName = "Connection-Accept-Rate"
  private val ConnectionRateMetricName = "connection-accept-rate"
  private val IpMetricTag = "ip"
  private val ListenerThrottlePrefix = ""
  private val IpThrottlePrefix = "ip-"

  private case class ListenerQuotaEntity(listenerName: String) extends ConnectionQuotaEntity {
    override def sensorName: String = s"$ConnectionRateSensorName-$listenerName"
    override def sensorExpiration: Long = Long.MaxValue
    override def metricName: String = ConnectionRateMetricName
    override def metricTags: Map[String, String] = Map(ListenerMetricTag -> listenerName)
  }

  private case object BrokerQuotaEntity extends ConnectionQuotaEntity {
    override def sensorName: String = ConnectionRateSensorName
    override def sensorExpiration: Long = Long.MaxValue
    override def metricName: String = s"broker-$ConnectionRateMetricName"
    override def metricTags: Map[String, String] = Map.empty
  }

  private case class IpQuotaEntity(ip: InetAddress) extends ConnectionQuotaEntity {
    override def sensorName: String = s"$ConnectionRateSensorName-${ip.getHostAddress}"
    override def sensorExpiration: Long = InactiveSensorExpirationTimeSeconds
    override def metricName: String = ConnectionRateMetricName
    override def metricTags: Map[String, String] = Map(IpMetricTag -> ip.getHostAddress)
  }
}

class ConnectionQuotas(config: KafkaConfig, time: Time, metrics: Metrics) extends Logging with AutoCloseable {

  @volatile private var defaultMaxConnectionsPerIp: Int = config.maxConnectionsPerIp
  @volatile private var maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides.map { case (host, count) => (InetAddress.getByName(host), count) }
  @volatile private var brokerMaxConnections = config.maxConnections
  private val interBrokerListenerName = config.interBrokerListenerName
  private val counts = mutable.Map[InetAddress, Int]()

  // Listener counts and configs are synchronized on `counts`
  private val listenerCounts = mutable.Map[ListenerName, Int]()
  private[network] val maxConnectionsPerListener = mutable.Map[ListenerName, ListenerConnectionQuota]()
  @volatile private var totalCount = 0
  // updates to defaultConnectionRatePerIp or connectionRatePerIp must be synchronized on `counts`
  @volatile private var defaultConnectionRatePerIp = QuotaConfigs.IP_CONNECTION_RATE_DEFAULT.intValue()
  private val connectionRatePerIp = new ConcurrentHashMap[InetAddress, Int]()
  // sensor that tracks broker-wide connection creation rate and limit (quota)
  private val brokerConnectionRateSensor = getOrCreateConnectionRateQuotaSensor(config.maxConnectionCreationRate, BrokerQuotaEntity)
  private val maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(config.quotaWindowSizeSeconds.toLong)

  def inc(listenerName: ListenerName, address: InetAddress, acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Unit = {
    counts.synchronized {
      waitForConnectionSlot(listenerName, acceptorBlockedPercentMeter)

      recordIpConnectionMaybeThrottle(listenerName, address)
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      totalCount += 1
      if (listenerCounts.contains(listenerName)) {
        listenerCounts.put(listenerName, listenerCounts(listenerName) + 1)
      }
      val max = maxConnectionsPerIpOverrides.getOrElse(address, defaultMaxConnectionsPerIp)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  private[network] def updateMaxConnectionsPerIp(maxConnectionsPerIp: Int): Unit = {
    defaultMaxConnectionsPerIp = maxConnectionsPerIp
  }

  private[network] def updateMaxConnectionsPerIpOverride(overrideQuotas: Map[String, Int]): Unit = {
    maxConnectionsPerIpOverrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  }

  private[network] def updateBrokerMaxConnections(maxConnections: Int): Unit = {
    counts.synchronized {
      brokerMaxConnections = maxConnections
      counts.notifyAll()
    }
  }

  private[network] def updateBrokerMaxConnectionRate(maxConnectionRate: Int): Unit = {
    // if there is a connection waiting on the rate throttle delay, we will let it wait the original delay even if
    // the rate limit increases, because it is just one connection per listener and the code is simpler that way
    updateConnectionRateQuota(maxConnectionRate, BrokerQuotaEntity)
  }

  /**
   * Update the connection rate quota for a given IP and updates quota configs for updated IPs.
   * If an IP is given, metric config will be updated only for the given IP, otherwise
   * all metric configs will be checked and updated if required.
   *
   * @param ip ip to update or default if None
   * @param maxConnectionRate new connection rate, or resets entity to default if None
   */
  def updateIpConnectionRateQuota(ip: Option[InetAddress], maxConnectionRate: Option[Int]): Unit = synchronized {
    def isIpConnectionRateMetric(metricName: MetricName) = {
      metricName.name == ConnectionRateMetricName &&
      metricName.group == MetricsGroup &&
      metricName.tags.containsKey(IpMetricTag)
    }

    def shouldUpdateQuota(metric: KafkaMetric, quotaLimit: Int) = {
      quotaLimit != metric.config.quota.bound
    }

    ip match {
      case Some(address) =>
        // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
        counts.synchronized {
          maxConnectionRate match {
            case Some(rate) =>
              info(s"Updating max connection rate override for $address to $rate")
              connectionRatePerIp.put(address, rate)
            case None =>
              info(s"Removing max connection rate override for $address")
              connectionRatePerIp.remove(address)
          }
        }
        updateConnectionRateQuota(connectionRateForIp(address), IpQuotaEntity(address))
      case None =>
        // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
        counts.synchronized {
          defaultConnectionRatePerIp = maxConnectionRate.getOrElse(QuotaConfigs.IP_CONNECTION_RATE_DEFAULT.intValue())
        }
        info(s"Updated default max IP connection rate to $defaultConnectionRatePerIp")
        metrics.metrics.forEach { (metricName, metric) =>
          if (isIpConnectionRateMetric(metricName)) {
            val quota = connectionRateForIp(InetAddress.getByName(metricName.tags.get(IpMetricTag)))
            if (shouldUpdateQuota(metric, quota)) {
              debug(s"Updating existing connection rate quota config for ${metricName.tags} to $quota")
              metric.config(rateQuotaMetricConfig(quota))
            }
          }
        }
    }
  }

  // Visible for testing
  def connectionRateForIp(ip: InetAddress): Int = {
    connectionRatePerIp.getOrDefault(ip, defaultConnectionRatePerIp)
  }

  private[network] def addListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
    counts.synchronized {
      if (!maxConnectionsPerListener.contains(listenerName)) {
        val newListenerQuota = new ListenerConnectionQuota(counts, listenerName)
        maxConnectionsPerListener.put(listenerName, newListenerQuota)
        listenerCounts.put(listenerName, 0)
        config.addReconfigurable(newListenerQuota)
        newListenerQuota.configure(config.valuesWithPrefixOverride(listenerName.configPrefix))
      }
      counts.notifyAll()
    }
  }

  private[network] def removeListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
    counts.synchronized {
      maxConnectionsPerListener.remove(listenerName).foreach { listenerQuota =>
        listenerCounts.remove(listenerName)
        // once listener is removed from maxConnectionsPerListener, no metrics will be recorded into listener's sensor
        // so it is safe to remove sensor here
        listenerQuota.close()
        counts.notifyAll() // wake up any waiting acceptors to close cleanly
        config.removeReconfigurable(listenerQuota)
      }
    }
  }

  def dec(listenerName: ListenerName, address: InetAddress): Unit = {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)

      if (totalCount <= 0)
        error(s"Attempted to decrease total connection count for broker with no connections")
      totalCount -= 1

      if (maxConnectionsPerListener.contains(listenerName)) {
        val listenerCount = listenerCounts(listenerName)
        if (listenerCount == 0)
          error(s"Attempted to decrease connection count for listener $listenerName with no connections")
        else
          listenerCounts.put(listenerName, listenerCount - 1)
      }
      counts.notifyAll() // wake up any acceptors waiting to process a new connection since listener connection limit was reached
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

  private def waitForConnectionSlot(listenerName: ListenerName,
                                    acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Unit = {
    counts.synchronized {
      val startThrottleTimeMs = time.milliseconds
      val throttleTimeMs = math.max(recordConnectionAndGetThrottleTimeMs(listenerName, startThrottleTimeMs), 0)

      if (throttleTimeMs > 0 || !connectionSlotAvailable(listenerName)) {
        val startNs = time.nanoseconds
        val endThrottleTimeMs = startThrottleTimeMs + throttleTimeMs
        var remainingThrottleTimeMs = throttleTimeMs
        do {
          counts.wait(remainingThrottleTimeMs)
          remainingThrottleTimeMs = math.max(endThrottleTimeMs - time.milliseconds, 0)
        } while (remainingThrottleTimeMs > 0 || !connectionSlotAvailable(listenerName))
        acceptorBlockedPercentMeter.mark(time.nanoseconds - startNs)
      }
    }
  }

  // This is invoked in every poll iteration and we close one LRU connection in an iteration
  // if necessary
  def maxConnectionsExceeded(listenerName: ListenerName): Boolean = {
    totalCount > brokerMaxConnections && !protectedListener(listenerName)
  }

  private def connectionSlotAvailable(listenerName: ListenerName): Boolean = {
    if (listenerCounts(listenerName) >= maxListenerConnections(listenerName))
      false
    else if (protectedListener(listenerName))
      true
    else
      totalCount < brokerMaxConnections
  }

  private def protectedListener(listenerName: ListenerName): Boolean =
    interBrokerListenerName == listenerName && listenerCounts.size > 1

  private def maxListenerConnections(listenerName: ListenerName): Int =
    maxConnectionsPerListener.get(listenerName).map(_.maxConnections).getOrElse(Int.MaxValue)

  /**
   * Calculates the delay needed to bring the observed connection creation rate to listener-level limit or to broker-wide
   * limit, whichever the longest. The delay is capped to the quota window size defined by QuotaWindowSizeSecondsProp
   *
   * @param listenerName listener for which calculate the delay
   * @param timeMs current time in milliseconds
   * @return delay in milliseconds
   */
  private def recordConnectionAndGetThrottleTimeMs(listenerName: ListenerName, timeMs: Long): Long = {
    def recordAndGetListenerThrottleTime(minThrottleTimeMs: Int): Int = {
      maxConnectionsPerListener
        .get(listenerName)
        .map { listenerQuota =>
          val listenerThrottleTimeMs = recordAndGetThrottleTimeMs(listenerQuota.connectionRateSensor, timeMs)
          val throttleTimeMs = math.max(minThrottleTimeMs, listenerThrottleTimeMs)
          // record throttle time due to hitting connection rate quota
          if (throttleTimeMs > 0) {
            listenerQuota.listenerConnectionRateThrottleSensor.record(throttleTimeMs.toDouble, timeMs)
          }
          throttleTimeMs
        }
        .getOrElse(0)
    }

    if (protectedListener(listenerName)) {
      recordAndGetListenerThrottleTime(0)
    } else {
      val brokerThrottleTimeMs = recordAndGetThrottleTimeMs(brokerConnectionRateSensor, timeMs)
      recordAndGetListenerThrottleTime(brokerThrottleTimeMs)
    }
  }

  /**
   * Record IP throttle time on the corresponding listener. To avoid over-recording listener/broker connection rate, we
   * also un-record the listener and broker connection if the IP gets throttled.
   *
   * @param listenerName listener to un-record connection
   * @param throttleMs IP throttle time to record for listener
   * @param timeMs current time in milliseconds
   */
  private def updateListenerMetrics(listenerName: ListenerName, throttleMs: Long, timeMs: Long): Unit = {
    if (!protectedListener(listenerName)) {
      brokerConnectionRateSensor.record(-1.0, timeMs, false)
    }
    maxConnectionsPerListener
      .get(listenerName)
      .foreach { listenerQuota =>
        listenerQuota.ipConnectionRateThrottleSensor.record(throttleMs.toDouble, timeMs)
        listenerQuota.connectionRateSensor.record(-1.0, timeMs, false)
      }
  }

  /**
   * Calculates the delay needed to bring the observed connection creation rate to the IP limit.
   * If the connection would cause an IP quota violation, un-record the connection for both IP,
   * listener, and broker connection rate and throw a ConnectionThrottledException. Calls to
   * this function must be performed with the counts lock to ensure that reading the IP
   * connection rate quota and creating the sensor's metric config is atomic.
   *
   * @param listenerName listener to unrecord connection if throttled
   * @param address ip address to record connection
   */
  private def recordIpConnectionMaybeThrottle(listenerName: ListenerName, address: InetAddress): Unit = {
    val connectionRateQuota = connectionRateForIp(address)
    val quotaEnabled = connectionRateQuota != QuotaConfigs.IP_CONNECTION_RATE_DEFAULT
    if (quotaEnabled) {
      val sensor = getOrCreateConnectionRateQuotaSensor(connectionRateQuota, IpQuotaEntity(address))
      val timeMs = time.milliseconds
      val throttleMs = recordAndGetThrottleTimeMs(sensor, timeMs)
      if (throttleMs > 0) {
        trace(s"Throttling $address for $throttleMs ms")
        // unrecord the connection since we won't accept the connection
        sensor.record(-1.0, timeMs, false)
        updateListenerMetrics(listenerName, throttleMs, timeMs)
        throw new ConnectionThrottledException(address, timeMs, throttleMs)
      }
    }
  }

  /**
   * Records a new connection into a given connection acceptance rate sensor 'sensor' and returns throttle time
   * in milliseconds if quota got violated
   * @param sensor sensor to record connection
   * @param timeMs current time in milliseconds
   * @return throttle time in milliseconds if quota got violated, otherwise 0
   */
  private def recordAndGetThrottleTimeMs(sensor: Sensor, timeMs: Long): Int = {
    try {
      sensor.record(1.0, timeMs)
      0
    } catch {
      case e: QuotaViolationException =>
        val throttleTimeMs = QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs).toInt
        debug(s"Quota violated for sensor (${sensor.name}). Delay time: $throttleTimeMs ms")
        throttleTimeMs
    }
  }

  /**
   * Creates sensor for tracking the connection creation rate and corresponding connection rate quota for a given
   * listener or broker-wide, if listener is not provided.
   * @param quotaLimit connection creation rate quota
   * @param connectionQuotaEntity entity to create the sensor for
   */
  private def getOrCreateConnectionRateQuotaSensor(quotaLimit: Int, connectionQuotaEntity: ConnectionQuotaEntity): Sensor = {
    Option(metrics.getSensor(connectionQuotaEntity.sensorName)).getOrElse {
      val sensor = metrics.sensor(
        connectionQuotaEntity.sensorName,
        rateQuotaMetricConfig(quotaLimit),
        connectionQuotaEntity.sensorExpiration
      )
      sensor.add(connectionRateMetricName(connectionQuotaEntity), new Rate, null)
      sensor
    }
  }

  /**
   * Updates quota configuration for a given connection quota entity
   */
  private def updateConnectionRateQuota(quotaLimit: Int, connectionQuotaEntity: ConnectionQuotaEntity): Unit = {
    Option(metrics.metric(connectionRateMetricName(connectionQuotaEntity))).foreach { metric =>
      metric.config(rateQuotaMetricConfig(quotaLimit))
      info(s"Updated ${connectionQuotaEntity.metricName} max connection creation rate to $quotaLimit")
    }
  }

  private def connectionRateMetricName(connectionQuotaEntity: ConnectionQuotaEntity): MetricName = {
    metrics.metricName(
      connectionQuotaEntity.metricName,
      MetricsGroup,
      s"Tracking rate of accepting new connections (per second)",
      connectionQuotaEntity.metricTags.asJava)
  }

  private def rateQuotaMetricConfig(quotaLimit: Int): MetricConfig = {
    new MetricConfig()
      .timeWindow(config.quotaWindowSizeSeconds.toLong, TimeUnit.SECONDS)
      .samples(config.numQuotaSamples)
      .quota(new Quota(quotaLimit, true))
  }

  def close(): Unit = {
    metrics.removeSensor(brokerConnectionRateSensor.name)
    maxConnectionsPerListener.values.foreach(_.close())
  }

  class ListenerConnectionQuota(lock: Object, listener: ListenerName) extends ListenerReconfigurable with AutoCloseable {
    @volatile private var _maxConnections = Int.MaxValue
    private[network] val connectionRateSensor = getOrCreateConnectionRateQuotaSensor(Int.MaxValue, ListenerQuotaEntity(listener.value))
    private[network] val listenerConnectionRateThrottleSensor = createConnectionRateThrottleSensor(ListenerThrottlePrefix)
    private[network] val ipConnectionRateThrottleSensor = createConnectionRateThrottleSensor(IpThrottlePrefix)

    def maxConnections: Int = _maxConnections

    override def listenerName(): ListenerName = listener

    override def configure(configs: util.Map[String, _]): Unit = {
      _maxConnections = maxConnections(configs)
      updateConnectionRateQuota(maxConnectionCreationRate(configs), ListenerQuotaEntity(listener.value))
    }

    override def reconfigurableConfigs(): util.Set[String] = {
      SocketServer.ListenerReconfigurableConfigs.asJava
    }

    override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
      val value = maxConnections(configs)
      if (value <= 0)
        throw new ConfigException(s"Invalid ${KafkaConfig.MaxConnectionsProp} $value")

      val rate = maxConnectionCreationRate(configs)
      if (rate <= 0)
        throw new ConfigException(s"Invalid ${KafkaConfig.MaxConnectionCreationRateProp} $rate")
    }

    override def reconfigure(configs: util.Map[String, _]): Unit = {
      lock.synchronized {
        _maxConnections = maxConnections(configs)
        updateConnectionRateQuota(maxConnectionCreationRate(configs), ListenerQuotaEntity(listener.value))
        lock.notifyAll()
      }
    }

    def close(): Unit = {
      metrics.removeSensor(connectionRateSensor.name)
      metrics.removeSensor(listenerConnectionRateThrottleSensor.name)
      metrics.removeSensor(ipConnectionRateThrottleSensor.name)
    }

    private def maxConnections(configs: util.Map[String, _]): Int = {
      Option(configs.get(KafkaConfig.MaxConnectionsProp)).map(_.toString.toInt).getOrElse(Int.MaxValue)
    }

    private def maxConnectionCreationRate(configs: util.Map[String, _]): Int = {
      Option(configs.get(KafkaConfig.MaxConnectionCreationRateProp)).map(_.toString.toInt).getOrElse(Int.MaxValue)
    }

    /**
     * Creates sensor for tracking the average throttle time on this listener due to hitting broker/listener connection
     * rate or IP connection rate quota. The average is out of all throttle times > 0, which is consistent with the
     * bandwidth and request quota throttle time metrics.
     */
    private def createConnectionRateThrottleSensor(throttlePrefix: String): Sensor = {
      val sensor = metrics.sensor(s"${throttlePrefix}ConnectionRateThrottleTime-${listener.value}")
      val metricName = metrics.metricName(s"${throttlePrefix}connection-accept-throttle-time",
        MetricsGroup,
        "Tracking average throttle-time, out of non-zero throttle times, per listener",
        Map(ListenerMetricTag -> listener.value).asJava)
      sensor.add(metricName, new Avg)
      sensor
    }
  }
}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException(s"Too many connections from $ip (maximum = $count)")

class ConnectionThrottledException(val ip: InetAddress, val startThrottleTimeMs: Long, val throttleTimeMs: Long)
  extends KafkaException(s"$ip throttled for $throttleTimeMs")
