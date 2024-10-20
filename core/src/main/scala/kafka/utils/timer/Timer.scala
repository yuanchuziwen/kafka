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
package kafka.utils.timer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{DelayQueue, Executors, TimeUnit}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
   * <p>
   *   将新任务添加到此执行程序。
   *   它将在任务的延迟之后执行（从提交时间开始）
   *
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
   * <p>
   *   推进内部时钟，执行任何到期的任务，这些任务在传递的超时持续时间内到达
   *   （即在超时时间内到期的任务将被执行）
   *
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
   * <p>
   *   获取待执行的任务数
   *
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
   * <p>
   *   关闭计时器服务，使待处理任务未执行
    */
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // timeout timer
  // 处理超时任务的线程池
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))

  // 一个延迟队列，用于存放 TimerTaskList
  // 每个 timerTaskList 就是时间轮中的一个桶
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  private[this] val taskCounter = new AtomicInteger(0)
  // 一个时间轮，用于存放 TimerTaskEntry
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  // TODO 注意，这个方法被调用的两个场景！
  // 手动往 SystemTimer 添加定时任务会触发；
  // 当 timer 自动取到超时的 timerTaskList 后，也会调用这个方法来过一遍这个 list 里面的所有任务，然后尝试再次 add，即实现任务的降级
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    // 尝试将 TimerTaskEntry 添加到时间轮中
    // 如果失败了，说明任务已经过期或者被取消了
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   * <p>
   *   如果有一个过期的桶，则推进时钟。
   *   如果在调用时没有任何过期的桶，则在放弃之前等待 timeoutMs
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    // 从延迟队列中取出一个过期的桶，这是一个阻塞的 poll
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    // 如果 bucket 不为空，说明有任务过期了
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          // 推进时间轮
          timingWheel.advanceClock(bucket.getExpiration)
          // 将 bucket 中的任务取出来，提交给线程池执行
          bucket.flush(addTimerTaskEntry)
          // 继续从延迟队列中取出一个过期的桶
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown(): Unit = {
    taskExecutor.shutdown()
  }

}
