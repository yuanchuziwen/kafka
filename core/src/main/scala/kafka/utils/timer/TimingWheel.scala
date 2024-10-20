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

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/*
 * Hierarchical Timing Wheels
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 * <p>
 * 一个简单的时间轮是一个计时器任务的桶的循环列表。
 * 假设 u 为时间单位。大小为 n 的时间轮有 n 个桶，可以在 n * u 时间间隔内保存计时器任务。
 * 每个桶保存落入相应时间范围的计时器任务。
 * 一开始，第一个桶保存 [0, u) 的任务，第二个桶保存 [u, 2u) 的任务，以此类推，第 n 个桶保存 [u * (n -1), u * n) 的任务。
 * 每个时间单位 u，计时器滴答一次并移动到下一个桶，然后过期其中的所有计时器任务。
 * 因此，计时器从不会将任务插入当前时间的桶中，因为它已经过期。
 * 计时器立即运行过期的任务。然后，清空的桶可用于下一轮，因此，如果当前桶是时间 t 的桶，则在滴答后它变为 [t + u * n, t + (n + 1) * u) 的桶。
 * 时间轮的插入/删除（启动计时器/停止计时器）成本为 O(1)，而基于优先级队列的计时器，例如 java.util.concurrent.DelayQueue 和 java.util.Timer，插入/删除成本为 O(log n)。
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 * <p>
 * 简单时间轮的一个主要缺点是它假设计时器请求在当前时间的 n * u 时间间隔内。如果计时器请求超出此间隔，则为溢出。
 * 分层时间轮处理此类溢出。
 * 它是一个分层组织的时间轮。最低级别具有最好的时间分辨率。随着向上移动层次，时间分辨率变得更粗糙。
 * 如果一个级别的时间轮的分辨率为 u，大小为 n，则下一个级别的分辨率应为 n * u。
 * 在每个级别上，溢出被委托到更高级别的时间轮。当更高级别的时间轮滴答时，它会将计时器任务重新插入到较低级别。
 * 可以根据需要创建溢出时间轮。当溢出桶中的桶过期时，其中的所有任务都会递归地重新插入计时器。
 * 然后，任务将移动到更精细的时间轮或执行。插入（启动计时器）成本为 O(m)，其中 m 是时间轮的数量，通常与系统中的请求数量相比非常小，删除（停止计时器）成本仍为 O(1)。
 *
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 *
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 *
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 *
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 *
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 *
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
 *
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 *
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 */
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {

  // 整个时间轮能够表示的时间范围
  private[this] val interval = tickMs * wheelSize
  // 当前层级时间轮持有的桶
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }

  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs

  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM

  // 由于 JVM 中的双重检查锁定模式的问题，overflowWheel 可能会通过 add() 被两个并发线程更新和读取，因此它需要是 volatile 的
  @volatile private[this] var overflowWheel: TimingWheel = null

  // 为当前层级的时间轮创建一个新的 TimerTaskEntry
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          tickMs = interval, // 上一个层级的时间轮中每个桶代表的时间间隔
          wheelSize = wheelSize,
          startMs = currentTime,
          taskCounter = taskCounter,
          queue
        )
      }
    }
  }

  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    val expiration = timerTaskEntry.expirationMs

    // 如果 task 已经取消，则返回 false
    if (timerTaskEntry.cancelled) {
      // Cancelled
      false

      // 如果 task 已经过期，则返回 false
    } else if (expiration < currentTime + tickMs) {
      // Already expired
      false

      // 如果 task 的过期时间在当前层级时间轮的时间范围内，则将 task 添加到当前层级时间轮的桶中
    } else if (expiration < currentTime + interval) {
      // Put in its own bucket
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      // 添加到某一个桶内
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.

        // 需要将桶入队，因为它是一个过期的桶
        // 我们只需要在其过期时间更改时将桶入队，即时间轮已经前进并且之前的桶被重用；
        // 在同一个时间轮周期内进一步调用设置过期时间将传递相同的值，因此具有相同过期时间的桶不会多次入队。
        queue.offer(bucket)
      }
      true
    } else {
      // Out of the interval. Put it into the parent timer
      // 交给上层的时间轮处理
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(timerTaskEntry)
    }
  }

  // Try to advance the clock
  // 尝试推进时钟
  def advanceClock(timeMs: Long): Unit = {
    if (timeMs >= currentTime + tickMs) {
      currentTime = timeMs - (timeMs % tickMs)

      // Try to advance the clock of the overflow wheel if present
      // 如果存在上层的时间轮，则尝试推进上层时间轮的时钟
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
