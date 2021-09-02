package org.apache.openwhisk.core.invoker

import akka.actor.Actor
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.openwhisk.core.containerpool.Interval

case class RunFinishedMessage(name: String, time: Interval)
case class CacheEntry(avgTimeMs: Long, numInvocation: Long)

/**
 * This actor implements a cache which stores a running average of a function's execution time. Lookups are concurrent,
 * writes are serialized through the actor's message handling
 */
case class PrewarmDeadlineCache() extends Actor {
  // key is arbitrary String, CacheEntry is two Longs which are typically 8 bytes each
  // assume an average String size of ~32 bytes (chars), to get 48 bytes/entry
  // maximum cache size for 100M of data would be ~2.2M entries
  val cache: Cache[String, CacheEntry] = CacheBuilder.newBuilder().maximumSize(2200000L).build();
  override def receive: Receive = {
    case r: RunFinishedMessage =>
      val cur = lookup(r.name)
      val newCount = cur.numInvocation + 1
      val newAvg = ((cur.avgTimeMs * cur.numInvocation) + r.time.duration.toMillis) / newCount
      cache.put(r.name, CacheEntry(newAvg, newCount))
  }

  /**
   * Lookups a cache entry. Reports a time of 0
   * @param name name of the action to lookup
   * @return the average execution time of the action since this process has started
   */
  private def lookup(name: String): CacheEntry = cache.get(name, () => CacheEntry(0, 0))

  def getTimeMs(name: String): Long = lookup(name).avgTimeMs
}
