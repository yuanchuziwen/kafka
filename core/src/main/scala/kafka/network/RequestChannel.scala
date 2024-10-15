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

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent._
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.network
import kafka.server.KafkaConfig
import kafka.utils.{Logging, NotNothing, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.EnvelopeResponseData
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors, ObjectSerializationCache}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  val RequestQueueSizeMetric = "RequestQueueSize"
  val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"

  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled

  sealed trait BaseRequest
  case object ShutdownRequest extends BaseRequest

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser: String = Sanitizer.sanitize(principal.getName)
  }

  class Metrics(enabledApis: Iterable[ApiKeys]) {
    def this(scope: ListenerType) = {
      this(ApiKeys.apisForListener(scope).asScala)
    }

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    (enabledApis.map(_.name) ++
      Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName)).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name))
    }

    def apply(metricName: String): RequestMetrics = metricsMap(metricName)

    def close(): Unit = {
       metricsMap.values.foreach(_.removeMetrics())
    }
  }

  // ---- 定义一些 request ----
  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                val memoryPool: MemoryPool,
                @volatile var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics,
                val envelope: Option[RequestChannel.Request] = None) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads

    // 这些需要是 volatile 的，因为 readers 在网络线程中，writers 在 request handler 线程或 purgatory 线程中
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var apiThrottleTimeMs = 0L
    @volatile var temporaryMemoryBytes = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    val session = Session(context.principal, context.clientAddress)

    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    // This is constructed on creation of a Request so that the JSON representation is computed before the request is
    // processed by the api layer. Otherwise, a ProduceRequest can occur without its data (ie. it goes into purgatory).

    // 在创建 Request 时构建，以便在请求被 api 层处理之前计算 JSON 表示。否则，ProduceRequest 可能会在没有数据的情况下发生（即进入 purgatory）。
    val requestLog: Option[JsonNode] =
      if (RequestChannel.isRequestLoggingEnabled) Some(RequestConvertToJson.request(loggableRequest))
      else None

    def header: RequestHeader = context.header

    def sizeOfBodyInBytes: Int = bodyAndSize.size

    def sizeInBytes: Int = header.size(new ObjectSerializationCache) + sizeOfBodyInBytes

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.

    // 大多数请求类型在这一点上完全解析为对象。对于这些请求，我们可以释放底层缓冲区。
    // 一些请求（例如 produce，或者模式包含 BYTES 或 NULLABLE_BYTES 类型字段的任何请求）保留对缓冲区的引用。
    // 对于这些请求，我们不能提前释放缓冲区，而只能在请求处理完成时才能释放。
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def isForwarded: Boolean = envelope.isDefined

    def buildResponseSend(abstractResponse: AbstractResponse): Send = {
      // 将业务处理的 response 封装成 Send 对象
      envelope match {
        case Some(request) =>
          val envelopeResponse = if (abstractResponse.errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
            // Since it's a NOT_CONTROLLER error response, we need to make envelope response with NOT_CONTROLLER error
            // to notify the requester (i.e. BrokerToControllerRequestThread) to update active controller

            // 因为它是一个 NOT_CONTROLLER 错误响应，我们需要使用 NOT_CONTROLLER 错误创建包装响应，
            // 以通知请求者（即 BrokerToControllerRequestThread）更新 active controller
            new EnvelopeResponse(new EnvelopeResponseData()
              .setErrorCode(Errors.NOT_CONTROLLER.code()))
          } else {
            val responseBytes = context.buildResponseEnvelopePayload(abstractResponse)
            new EnvelopeResponse(responseBytes, Errors.NONE)
          }
          request.context.buildResponseSend(envelopeResponse)
        case None =>
          context.buildResponseSend(abstractResponse)
      }
    }

    def responseNode(response: AbstractResponse): Option[JsonNode] = {
      if (RequestChannel.isRequestLoggingEnabled) {
        // 将 response 基于 ApiVersion 转换为 JsonNode
        Some(RequestConvertToJson.response(response, context.apiVersion))
      } else
        None
    }

    def headerForLoggingOrThrottling(): RequestHeader = {
      envelope match {
        case Some(request) =>
          request.context.header
        case None =>
          context.header
      }
    }

    def requestDesc(details: Boolean): String = {
      val forwardDescription = envelope.map { request =>
        s"Forwarded request: ${request.context} "
      }.getOrElse("")
      s"$forwardDescription$header -- ${loggableRequest.toString(details)}"
    }

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def loggableRequest: AbstractRequest = {

      bodyAndSize.request match {
        case alterConfigs: AlterConfigsRequest =>
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new AlterConfigsRequest(newData, alterConfigs.version())

        case alterConfigs: IncrementalAlterConfigsRequest =>
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new IncrementalAlterConfigsRequest.Builder(newData).build(alterConfigs.version())

        case _ =>
          bodyAndSize.request
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos: Long = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response): Unit = {
      val endTimeNanos = Time.SYSTEM.nanoseconds

      /**
       * Converts nanos to millis with micros precision as additional decimal places in the request log have low
       * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
       * to the nearest long.
       */
      def nanosToMs(nanos: Long): Double = {
        val positiveNanos = math.max(nanos, 0)
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
      val apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val fetchMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val isFromFollower = body[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ header.apiKey.name
      metricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(apiThrottleTimeMs)
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.requestBytesHist.update(sizeOfBodyInBytes)
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (isRequestLoggingEnabled) {
        val desc = RequestConvertToJson.requestDescMetrics(header, requestLog, response.responseLog,
          context, session, isForwarded,
          totalTimeMs, requestQueueTimeMs, apiLocalTimeMs,
          apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs,
          responseSendTimeMs, temporaryMemoryBytes,
          messageConversionsTimeMs)
        requestLogger.debug("Completed request:" + desc.toString)
      }
    }

    def releaseBuffer(): Unit = {
      envelope match {
        case Some(request) =>
          request.releaseBuffer()
        case None =>
          if (buffer != null) {
            memoryPool.release(buffer)
            buffer = null
          }
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer, " +
      s"envelope=$envelope)"

  }

  // ---- 开始定义一些 response ----

  sealed abstract class Response(val request: Request) {

    // 每个 response 都有一个 processor，用于将 response 发送给客户端
    def processor: Int = request.processor

    def responseLog: Option[JsonNode] = None

    def onComplete: Option[Send => Unit] = None
  }

  /**
   * responseLogValue should only be defined if request logging is enabled
   * <p>
   *   responseLogValue 只有在启用请求日志记录时才应该定义
   */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseLogValue: Option[JsonNode],
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseLog: Option[JsonNode] = responseLogValue

    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseLogValue)"
  }

  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }

  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}

// ---- 结束定义一些 response ----

class RequestChannel(val queueSize: Int,
                     val metricNamePrefix: String,
                     time: Time,
                     val metrics: RequestChannel.Metrics) extends KafkaMetricsGroup {
  import RequestChannel._
  // processor 收到请求后，将请求放入 RequestChannel 的 requestQueue 中
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  /*
  processor 线程将读取到的请求存入 requestQueue 中；
  Handler 线程从 requestQueue 队列取出请求进行处理；
  Handler 线程处理请求产生的响应会存放到 Processor 对应的 responseQueue 中；
  Processor 线程从 responseQueue 中取出响应并发送给客户端。
   */
  // channel 对应的 processor 集合，实际每个 processor 对应一个 selector 线程
  // 也可以把这个 map 看作是 responseQueue 的集合，0.10.x 版本的时候是一个 responseQueue 对应一个 processor
  private val processors = new ConcurrentHashMap[Int, Processor]()
  val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)

  newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })

  // 添加 processor
  def addProcessor(processor: Processor): Unit = {
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      Map(ProcessorMetricTag -> processor.id.toString))
  }

  // 移除 processor
  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)
    removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString))
  }

  /**
   * Send a request to be handled, potentially blocking until there is room in the queue for the request
   * <p>
   *   将请求发送到处理，可能会阻塞，直到队列中有请求的空间
   */
  def sendRequest(request: RequestChannel.Request): Unit = {
    // 由 processor 调用，将请求放入队列，后续进行业务操作
    requestQueue.put(request)
  }

  // 关闭连接
  def closeConnection(
    request: RequestChannel.Request,
    errorCounts: java.util.Map[Errors, Integer]
  ): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    // 这个 case 用于当 request handler 遇到错误时，
    // 但 client 不希望收到响应时使用（例如，当 produce 请求的 acks 设置为 0 时）
    updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  def sendResponse(
    request: RequestChannel.Request,
    response: AbstractResponse,
    onComplete: Option[Send => Unit]
  ): Unit = {
    updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala)
    // 封装 SendResponse，发送消息
    sendResponse(new RequestChannel.SendResponse(
      request,
      // 将 response 封装为 Send
      request.buildResponseSend(response),
      // 基于 response 及 ApiVersion，封装 JsonNode
      request.responseNode(response),
      onComplete
    ))
  }

  def sendNoOpResponse(request: RequestChannel.Request): Unit = {
    sendResponse(new network.RequestChannel.NoOpResponse(request))
  }

  def startThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new RequestChannel.StartThrottlingResponse(request))
  }

  def endThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new EndThrottlingResponse(request))
  }

  /**
   * Send a response back to the socket server to be sent over the network
   * <p>
   *   发送响应到 socket server，以便通过网络发送
   */
  private[network] def sendResponse(response: RequestChannel.Response): Unit = {
    if (isTraceEnabled) {
      // 取到 request 对应的 header，用于日志打印
      val requestHeader = response.request.headerForLoggingOrThrottling()
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }

    response match {
      // We should only send one of the following per request
      // 我们应该只发送以下内容之一
      case _: SendResponse | _: NoOpResponse | _: CloseConnectionResponse =>
        val request = response.request
        val timeNanos = time.nanoseconds()
        request.responseCompleteTimeNanos = timeNanos
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos = timeNanos
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    // 根据 response 获取到它对应的 processor
    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    // 如果 processor 为 null，说明 processor 已经关闭，此时连接已经关闭，所以直接丢弃 response
    if (processor != null) {
      processor.enqueueResponse(response)
    }
  }

  /**
   * Get the next request or block until specified time has elapsed
   * <p>
   *   获取下一个请求，或者阻塞直到指定的时间已经过去
   */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest = {
    // 从 requestQueue 中拉取请求，如果没有请求，则阻塞 timeout 毫秒
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)
  }

  /**
   * Get the next request or block until there is one
   * <p>
   *   获取下一个请求，或者阻塞直到有请求
   */
  def receiveRequest(): RequestChannel.BaseRequest =
    requestQueue.take()

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.forKeyValue { (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

}

object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"

  val RequestsPerSec = "RequestsPerSec"
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  val LocalTimeMs = "LocalTimeMs"
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {

  import RequestMetrics._

  val tags = Map("request" -> name)
  val requestRateInternal = new Pool[Short, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram(RequestQueueTimeMs, biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(LocalTimeMs, biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(RemoteTimeMs, biased = true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
  val throttleTimeHist = newHistogram(ThrottleTimeMs, biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram(ResponseQueueTimeMs, biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram(ResponseSendTimeMs, biased = true, tags)
  val totalTimeHist = newHistogram(TotalTimeMs, biased = true, tags)
  // request size in bytes
  val requestBytesHist = newHistogram(RequestBytes, biased = true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(MessageConversionsTimeMs, biased = true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(TemporaryMemoryBytes, biased = true, tags))
    else
      None

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter = {
    requestRateInternal.getAndMaybePut(version, newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags + ("version" -> version.toString)))
  }

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name)

    @volatile private var meter: Meter = null

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) removeMetric(RequestsPerSec, tags + ("version" -> version.toString))
    removeMetric(RequestQueueTimeMs, tags)
    removeMetric(LocalTimeMs, tags)
    removeMetric(RemoteTimeMs, tags)
    removeMetric(RequestsPerSec, tags)
    removeMetric(ThrottleTimeMs, tags)
    removeMetric(ResponseQueueTimeMs, tags)
    removeMetric(TotalTimeMs, tags)
    removeMetric(ResponseSendTimeMs, tags)
    removeMetric(RequestBytes, tags)
    removeMetric(ResponseSendTimeMs, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      removeMetric(MessageConversionsTimeMs, tags)
      removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
