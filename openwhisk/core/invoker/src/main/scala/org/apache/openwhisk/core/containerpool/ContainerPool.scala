/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import org.apache.openwhisk.common.{Logging, LoggingMarkers, MetricEmitter, TransactionId}
import org.apache.openwhisk.core.connector.{MessageFeed, PartialPrewarmConfig}
import org.apache.openwhisk.core.entity.ExecManifest.ReactivePrewarmingConfig
import org.apache.openwhisk.core.entity._

import scala.annotation.tailrec
import scala.collection._
import scala.concurrent.duration._
import scala.util.{Random, Try}
import cats.implicits._
import org.apache.openwhisk.core.ConfigKeys
import pureconfig.loadConfigOrThrow
import scala.concurrent.ExecutionContextExecutor

import java.util.concurrent.atomic.LongAdder

sealed trait WorkerState
case object Busy extends WorkerState
case object Free extends WorkerState

case class ColdStartKey(kind: String, resources: RuntimeResources)

case class WorkerData(data: ContainerData, state: WorkerState)

case object EmitMetrics

case object AdjustPrewarmedContainer

case class PrewarmContainer(action: ExecutableWhiskAction, msg: PartialPrewarmConfig)

/**
 * A pool managing containers to run actions on.
 *
 * This pool fulfills the other half of the ContainerProxy contract. Only
 * one job (either Start or Run) is sent to a child-actor at any given
 * time. The pool then waits for a response of that container, indicating
 * the container is done with the job. Only then will the pool send another
 * request to that container.
 *
 * Upon actor creation, the pool will start to prewarm containers according
 * to the provided prewarmConfig, iff set. Those containers will **not** be
 * part of the poolsize calculation, which is capped by the poolSize parameter.
 * Prewarm containers are only used, if they have matching arguments
 * (kind, memory) and there is space in the pool.
 *
 * @param childFactory method to create new container proxy actor
 * @param feed actor to request more work from
 * @param prewarmConfig optional settings for container prewarming
 * @param poolConfig config for the ContainerPool
 */
class ContainerPool(childFactory: ActorRefFactory => ActorRef,
                    feed: ActorRef,
                    prewarmConfig: List[PrewarmingConfig] = List.empty,
                    poolConfig: ContainerPoolConfig,
                    proxyAddressBook: Option[ActorProxyAddressBook]
                   )(implicit val logging: Logging)
    extends Actor {
  import ContainerPool.resourceConsumptionOf

  private val useRdma: Boolean = loadConfigOrThrow[Boolean](ConfigKeys.useRdma)

  implicit val ec: ExecutionContextExecutor = context.dispatcher

  var freePool = immutable.Map.empty[ActorRef, ContainerData]
  var busyPool = immutable.Map.empty[ActorRef, ContainerData]
  var prewarmedPool = immutable.Map.empty[ActorRef, PreWarmedData]
  var prewarmStartingPool = immutable.Map.empty[ActorRef, (String, RuntimeResources)]
  // If all memory slots are occupied and if there is currently no container to be removed, than the actions will be
  // buffered here to keep order of computation.
  // Otherwise actions with small memory-limits could block actions with large memory limits.
  var runBuffer: immutable.Queue[Run] = immutable.Queue.empty[Run]
  // Track the resent buffer head - so that we don't resend buffer head multiple times
  var resent: Option[Run] = None
  val logMessageInterval: FiniteDuration = 10.seconds
  //periodically emit metrics (don't need to do this for each message!)
  context.system.scheduler.schedule(30.seconds, 10.seconds, self, EmitMetrics)

  // Key is ColdStartKey, value is the number of cold Start in minute
  var coldStartCount = immutable.Map.empty[ColdStartKey, Int]

  adjustPrewarmedContainer(init = true, scheduled = false)

  // check periodically, adjust prewarmed container(delete if unused for some time and create some increment containers)
  // add some random amount to this schedule to avoid a herd of container removal + creation
  val interval: FiniteDuration = poolConfig.prewarmExpirationCheckInterval + poolConfig.prewarmExpirationCheckIntervalVariance
    .map(v =>
      Random
        .nextInt(v.toSeconds.toInt))
    .getOrElse(0)
    .seconds
  context.system.scheduler.schedule(2.seconds, interval, self, AdjustPrewarmedContainer)

  // We need to have a actionId, which maps an actionId to activationId
  // This can happens in both side of the work
  var activationMap : Map[ActivationId, ActorRef] = Map.empty
  val addressBook = new CorunningAddressBook(this)


  def logContainerStart(r: Run, containerState: String, activeActivations: Int, container: Option[Container]): Unit = {
    val namespaceName = r.msg.user.namespace.name.asString
    val actionName = r.action.name.name
    val actionNamespace = r.action.namespace.namespace
    val maxConcurrent = r.action.limits.concurrency.maxConcurrent
    val activationId = r.msg.activationId.toString

    r.msg.transid.mark(
      this,
      LoggingMarkers.INVOKER_CONTAINER_START(containerState, namespaceName, actionNamespace, actionName),
      s"containerStart containerState: $containerState container: $container activations: $activeActivations of max $maxConcurrent action: $actionName namespace: $namespaceName activationId: $activationId",
      akka.event.Logging.InfoLevel)
  }

  def receive: Receive = {
    // A job to run on a container
    //
    // Run messages are received either via the feed or from child containers which cannot process
    // their requests and send them back to the pool for rescheduling (this may happen if "docker" operations
    // fail for example, or a container has aged and was destroying itself when a new request was assigned)
    case r: Run =>
      // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
      val isResentFromBuffer = runBuffer.nonEmpty && runBuffer.dequeueOption.exists(_._1.msg == r.msg)
      // Only process request, if there are no other requests waiting for free slots, or if the current request is the
      // next request to process
      // It is guaranteed, that only the first message on the buffer is resent.
      if (runBuffer.isEmpty || isResentFromBuffer) {
        if (isResentFromBuffer) {
          //remove from resent tracking - it may get resent again, or get processed
          resent = None
        }
        val createdContainer =
          // Is there enough space on the invoker for this action to be executed.
          if (hasPoolSpaceFor(busyPool, r.action)) {
            // Schedule a job to a warm container
            ContainerPool
              .schedule(r.action, r.msg.user.namespace.name, freePool)
              .map(container => (container, container._2.initingState)) //warmed, warming, and warmingCold always know their state
              .orElse(
                // There was no warm/warming/warmingCold container. Try to take a prewarm container or a cold container.

                // Is there enough space to create a new container or do other containers have to be removed?
                if (hasPoolSpaceFor(busyPool ++ freePool, r.action)) {
                  takePrewarmContainer(r.action)
                    .map(container => (container, "prewarmed"))
                    .orElse {
                      val container = Some(createContainer(r.action.limits.resources.limits), "cold")
                      incrementColdStartCount(r.action.exec.kind, r.action.limits.resources.limits)
                      container
                    }
                } else None)
              .orElse(
                // Remove a container and create a new one for the given job
                ContainerPool
                // Only free up the amount, of resources that is really needed
                  .remove(freePool, r.action.limits.resources.limits)
                  .map(removeContainer)
                  // If the list had at least one entry, enough containers were removed to start the new container. After
                  // removing the containers, we are not interested anymore in the containers that have been removed.
                  .headOption
                  .map(_ =>
                    takePrewarmContainer(r.action)
                      .map(container => (container, "recreatedPrewarm"))
                      .getOrElse {
                        val container = (createContainer(r.action.limits.resources.limits), "recreated")
                        incrementColdStartCount(r.action.exec.kind, r.action.limits.resources.limits)
                        container
                    }))

          } else None

        createdContainer match {
          case Some(((actor, data), containerState)) =>
            //increment active count before storing in pool map
            val newData = data.nextRun(r)
            val container = newData.getContainer

            if (newData.activeActivationCount < 1) {
              logging.error(this, s"invalid activation count < 1 ${newData}")
            }

            //only move to busyPool if max reached
            if (!newData.hasCapacity()) {
              if (r.action.limits.concurrency.maxConcurrent > 1) {
                logging.info(
                  this,
                  s"container ${container} is now busy with ${newData.activeActivationCount} activations")
              }
              busyPool = busyPool + (actor -> newData)
              freePool = freePool - actor
            } else {
              //update freePool to track counts
              freePool = freePool + (actor -> newData)
            }
            // Remove the action that was just executed from the buffer and execute the next one in the queue.
            if (isResentFromBuffer) {
              // It is guaranteed that the currently executed messages is the head of the queue, if the message comes
              // from the buffer
              val (_, newBuffer) = runBuffer.dequeue
              runBuffer = newBuffer
              // Try to process the next item in buffer (or get another message from feed, if buffer is now empty)
              processBufferOrFeed()
            }

            // Pre run actions, Add map to the buffer
            activationMap = activationMap + (r.msg.activationId -> actor)

            // We also log this into the run message

            // for proxy, we can still post wait here, but we need to carry the port number
            // since if we got parallelism, we need to reply to the src (remember which remote proxy addr for local)
            // it needs extra logic
            val defaultAddresses = LibdAPIs.Transport.getDefaultTransport(r.action, useRdma)
            val fetchedAddresses = for {
              runtime <- r.action.porusParams.runtimeType
              if LibdAPIs.Transport.needWait(runtime)
              seq <- r.msg.siblings
            } yield seq.toSet.groupBy(LibdAPIs.Transport.getName).flatMap {
              // check parallelism, if we have multiple elements with same name
              // We mangle the name
              case (name, ras) =>
                val isPar = ras.size > 1
                ras.zipWithIndex.map { case (ra, par) =>
                  val aid = r.msg.activationId
                  val impl = LibdAPIs.Transport.getImpl(ra, runtime)
                  val request = if (isPar) TransportRequest.configPar(name, par, impl, aid)
                                else TransportRequest.config(name, impl, aid)

                  if (poolConfig.useProxy) {
                    val srcTransportName = if (isPar) s"$name@$par" else name
                    val dstTransportName = "memory"
                    proxyAddressBook.foreach { book =>
                      book.prepareReply(
                        src = ProxyAddress(aid,              srcTransportName),
                        dst = ProxyAddress(ra.objActivation, dstTransportName))
                    }
                    // Return empty string for config.
                    // TODO: merge to libd
                    s"${srcTransportName};rdma_uverbs_proxy;"
                  } else addressBook
                    .postWait(ra.objActivation, request)
                    .toFullString

                }
            }
            val transports =
              (defaultAddresses.getOrElse(Seq.empty) ++ fetchedAddresses.getOrElse(Seq.empty)) match {
                case seq if seq.isEmpty => None
                case s                  => Some(s)
              }

            if (poolConfig.useProxy && fetchedAddresses.filter(_.nonEmpty).nonEmpty) {
              // Send a GetMessage() request after run
              val request = TransportRequest.getMessage(r.msg.activationId)
              logging.debug(this, "[MPT] Sending Message Request...")
              actor ! LibdTransportConfig(r.msg.activationId, request, TransportAddress.empty)
            }


            logging.debug(this, s"Get activation for ${r.msg.action} with id ${r.msg.activationId}: addresses ${transports}: run msg: $r")
            logging.debug(this, s"siblings ${r.msg.siblings} -> ${r.msg.siblings.map(_.map(_.objName))}")
            actor ! r.copy(corunningConfig = transports)

            // Only run this when we use containers as memory pool
            if (!poolConfig.useProxy) {
              for {
                runtime <- r.action.porusParams.runtimeType
                if LibdAPIs.Transport.needSignal(runtime)
                seq <- r.msg.siblings
              } yield seq.map { ra =>
                // Here we post self's name
                val name = r.action.name.name
                val port = LibdAPIs.Transport.getPort(ra)

                // check if the container already exists
                container.map(_.addr.host) match {
                  case None =>
                    addressBook.prepareSignal(actor,
                        r.msg.activationId, name, TransportAddress(port.toString))
                    logging.debug(this, s"signal prepared, ${actor}")
                  case Some(containerIp) =>
                    addressBook.signalReady(r.msg.activationId, name,
                    TransportAddress.TCPTransport(containerIp, port.toString))
                }
              }
            }

            logContainerStart(r, containerState, newData.activeActivationCount, container)

          case None =>
            // this can also happen if createContainer fails to start a new container, or
            // if a job is rescheduled but the container it was allocated to has not yet destroyed itself
            // (and a new container would over commit the pool)
            val isErrorLogged = r.retryLogDeadline.forall(_.isOverdue)
            val retryLogDeadline = if (isErrorLogged) {
              logging.warn(
                this,
                s"Rescheduling Run message, too many message in the pool, " +
                  s"freePoolSize: ${freePool.size} containers and resources: ${resourceConsumptionOf(freePool)}, " +
                  s"busyPoolSize: ${busyPool.size} containers and resources: ${resourceConsumptionOf(busyPool)}, " +
                  s"maxComputeContainersResources ${poolConfig.invokerPoolResources.computePool}, " +
                  s"maxMemContainersResources ${poolConfig.invokerPoolResources.memPool}, " +
                  s"maxBalancedContainersResources ${poolConfig.invokerPoolResources.balancedPool}, " +
                  s"userNamespace: ${r.msg.user.namespace.name}, action: ${r.action}, " +
                  s"needed resources: ${r.action.limits.resources.limits}, " +
                  s"waiting messages: ${runBuffer.size}")(r.msg.transid)
              MetricEmitter.emitCounterMetric(LoggingMarkers.CONTAINER_POOL_RESCHEDULED_ACTIVATION)
              Some(logMessageInterval.fromNow)
            } else {
              r.retryLogDeadline
            }
            if (!isResentFromBuffer) {
              // Add this request to the buffer, as it is not there yet.
              runBuffer = runBuffer.enqueue(Run(r.action, r.msg, retryLogDeadline))
            }
          //buffered items will be processed via processBufferOrFeed()
        }
      } else {
        // There are currently actions waiting to be executed before this action gets executed.
        // These waiting actions were not able to free up enough memory.
        runBuffer = runBuffer.enqueue(r)
      }

    // Container is free to take more work
    case NeedWork(warmData: WarmedData) =>
      val oldData = freePool.getOrElse(sender(), busyPool(sender()))
      val newData =
        warmData.copy(lastUsed = oldData.lastUsed, activeActivationCount = oldData.activeActivationCount - 1)
      if (newData.activeActivationCount < 0) {
        logging.error(this, s"invalid activation count after warming < 1 ${newData}")
      }
      if (newData.hasCapacity()) {
        //remove from busy pool (may already not be there), put back into free pool (to update activation counts)
        freePool = freePool + (sender() -> newData)
        if (busyPool.contains(sender())) {
          busyPool = busyPool - sender()
          if (newData.maxConcurrent > 1) {
            logging.info(
              this,
              s"concurrent container ${newData.container} is no longer busy with ${newData.activeActivationCount} activations")
          }
        }
      } else {
        busyPool = busyPool + (sender() -> newData)
        freePool = freePool - sender()
      }
      processBufferOrFeed()

    // Container is prewarmed and ready to take work
    case NeedWork(data: PreWarmedData) =>
      prewarmStartingPool = prewarmStartingPool - sender()
      prewarmedPool = prewarmedPool + (sender() -> data)
      // try to signal for cold container

    // Container got removed
    case ContainerRemoved(replacePrewarm) =>
      // if container was in free pool, it may have been processing (but under capacity),
      // so there is capacity to accept another job request
      freePool.get(sender()).foreach { f =>
        freePool = freePool - sender()
      }
      // container was busy (busy indicates at full capacity), so there is capacity to accept another job request
      busyPool.get(sender()).foreach { _ =>
        busyPool = busyPool - sender()
      }
      processBufferOrFeed()

      //in case this was a prewarm
      prewarmedPool.get(sender()).foreach { data =>
        prewarmedPool = prewarmedPool - sender()
      }
      //in case this was a starting prewarm
      prewarmStartingPool.get(sender()).foreach { _ =>
        logging.info(this, "failed starting prewarm removed")
        prewarmStartingPool = prewarmStartingPool - sender()
      }

      //backfill prewarms on every ContainerRemoved(replacePrewarm = true), just in case
      if (replacePrewarm) {
        adjustPrewarmedContainer(false, false) //in case a prewarm is removed due to health failure or crash
      }

      // Handles cold start
    case ColdStarted(data) =>
      addressBook.signalReady(sender(), data.container.addr.host)

    // Handles end of objects
    case ObjectEnd(activationId) =>
      activationMap = activationMap - activationId
      if (poolConfig.useProxy)
        // TODO: release the connection
        proxyAddressBook.foreach { book => book.releaseAll(activationId) }
      else
        addressBook.remove(activationId)

    // This message is received for one of these reasons:
    // 1. Container errored while resuming a warm container, could not process the job, and sent the job back
    // 2. The container aged, is destroying itself, and was assigned a job which it had to send back
    // 3. The container aged and is destroying itself
    // Update the free/busy lists but no message is sent to the feed since there is no change in capacity yet
    case RescheduleJob =>
      freePool = freePool - sender()
      busyPool = busyPool - sender()
    case EmitMetrics =>
      emitMetrics()

    case AdjustPrewarmedContainer =>
      adjustPrewarmedContainer(false, true)
      logging.debug(this, s"freePool (${freePool.size}) ${freePool.values}, busyPool(${busyPool.size}): ${busyPool.values}, prewarmed (${prewarmedPool.size}): ${prewarmedPool.values}")
    case warm: PrewarmContainer =>
      logging.debug(this, s"prewarming container ${warm.action.name} with TTL: ${warm.msg.ttlMs}ms")
      prewarmContainer(warm.action.exec, warm.msg.resources, Some(FiniteDuration(warm.msg.ttlMs, MILLISECONDS)))

    // Forward Libd Messages
    case request: LibdTransportConfig =>
      activationMap.get(request.activationId).foreach { _ ! request }
  }

  /** Resend next item in the buffer, or trigger next item in the feed, if no items in the buffer. */
  def processBufferOrFeed() = {
    // If buffer has more items, and head has not already been resent, send next one, otherwise get next from feed.
    runBuffer.dequeueOption match {
      case Some((run, _)) => //run the first from buffer
        implicit val tid = run.msg.transid
        //avoid sending dupes
        if (resent.isEmpty) {
          logging.info(this, s"re-processing from buffer (${runBuffer.length} items in buffer)")
          resent = Some(run)
          self ! run
        } else {
          //do not resend the buffer head multiple times (may reach this point from multiple messages, before the buffer head is re-processed)
        }
      case None => //feed me!
        feed ! MessageFeed.Processed
    }
  }

  /** adjust prewarm containers up to the configured requirements for each kind/memory combination. */
  def adjustPrewarmedContainer(init: Boolean, scheduled: Boolean): Unit = {
    if (scheduled) {
      //on scheduled time, remove expired prewarms
      ContainerPool.removeExpired(poolConfig, prewarmConfig, prewarmedPool).foreach { p =>
        prewarmedPool = prewarmedPool - p
        p ! Remove
      }
      //on scheduled time, emit cold start counter metric with memory + kind
      coldStartCount foreach { coldStart =>
        val coldStartKey = coldStart._1
        MetricEmitter.emitCounterMetric(
          LoggingMarkers.CONTAINER_POOL_PREWARM_COLDSTART(coldStartKey.resources.toString, coldStartKey.kind))
      }
    }
    //fill in missing prewarms (replaces any deletes)
    ContainerPool
      .increasePrewarms(init, scheduled, coldStartCount, prewarmConfig, prewarmedPool, prewarmStartingPool)
      .foreach { c =>
        val config = c._1
        val currentCount = c._2._1
        val desiredCount = c._2._2
        if (currentCount < desiredCount) {
          (currentCount until desiredCount).foreach { _ =>
            prewarmContainer(config.exec, config.resources, config.reactive.map(_.ttl))
          }
        }
      }
    if (scheduled) {
      //   lastly, clear coldStartCounts each time scheduled event is processed to reset counts
      coldStartCount = immutable.Map.empty[ColdStartKey, Int]
    }
  }

  /** Creates a new container and updates state accordingly. */
  def createContainer(resources: RuntimeResources): (ActorRef, ContainerData) = {
    val ref = childFactory(context)
    val data = ResourceData(resources)
    freePool = freePool + (ref -> data)
    ref -> data
  }

  /** Creates a new prewarmed container */
  def prewarmContainer(exec: CodeExec[_], resources: RuntimeResources, ttl: Option[FiniteDuration]): Unit = {
    val newContainer = childFactory(context)
    prewarmStartingPool = prewarmStartingPool + (newContainer -> (exec.kind, resources))
    newContainer ! Start(exec, resources, ttl)
  }

  /** this is only for cold start statistics of prewarm configs, e.g. not blackbox or other configs. */
  def incrementColdStartCount(kind: String, resources: RuntimeResources): Unit = {
    prewarmConfig
      .filter { config =>
        kind == config.exec.kind && resources == config.resources
      }
      .foreach { _ =>
        val coldStartKey = ColdStartKey(kind, resources)
        coldStartCount.get(coldStartKey) match {
          case Some(value) => coldStartCount = coldStartCount + (coldStartKey -> (value + 1))
          case None        => coldStartCount = coldStartCount + (coldStartKey -> 1)
        }
      }
  }

  var totalPrewarms: LongAdder = new LongAdder()

  /**
   * Takes a prewarm container out of the prewarmed pool
   * iff a container with a matching kind and memory is found.
   *
   * @param action the action that holds the kind and the required memory.
   * @return the container iff found
   */
  def takePrewarmContainer(action: ExecutableWhiskAction): Option[(ActorRef, ContainerData)] = {
    totalPrewarms.increment()
    logging.debug(this, s"total taken prewarms: ${totalPrewarms.sum()}")
    val kind = action.exec.kind
    val resources = action.limits.resources.limits
    val now = Deadline.now
    val x = prewarmedPool.toSeq
      .sortBy(_._2.expires.getOrElse(now))
      .find {
        case (_, PreWarmedData(_, `kind`, `resources`, _, _)) => true
        case _                                             => false
      }
      .map {
        case (ref, data) =>
          // Move the container to the usual pool
          freePool = freePool + (ref -> data)
          prewarmedPool = prewarmedPool - ref
          // Create a new prewarm container
          // NOTE: prewarming ignores the action code in exec, but this is dangerous as the field is accessible to the
          // factory

          //get the appropriate ttl from prewarm configs
          val ttl =
            prewarmConfig.find(pc => pc.resources == resources && pc.exec.kind == kind).flatMap(_.reactive.map(_.ttl))
          prewarmContainer(action.exec, resources, ttl)
          (ref, data)
      }
    x
  }

  /** Removes a container and updates state accordingly. */
  def removeContainer(toDelete: ActorRef) = {
    toDelete ! Remove
    freePool = freePool - toDelete
    busyPool = busyPool - toDelete
  }

  /**
   * Calculate if there is enough free resources within a given pool.
   *
   * @param pool The pool, that has to be checked, if there is enough free memory.
   * @return true, if there is enough space for the given amount of memory.
   */
  def hasPoolSpaceFor[A](pool: Map[A, ContainerData], action: ExecutableWhiskAction): Boolean = {
    val usedResources: RuntimeResources = resourceConsumptionOf(pool) + action.limits.resources.limits
    !usedResources.exceedsAnyOf(InvokerPoolResourceType.poolFor(action).ofResources(poolConfig.invokerPoolResources))
  }

  /**
   * Log metrics about pool state (buffer size, buffer memory requirements, active number, active memory, prewarm number, prewarm memory)
   */
  private def emitMetrics() = {
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_RUNBUFFER_COUNT, runBuffer.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_RUNBUFFER_SIZE,
      runBuffer.map(_.action.limits.resources.limits.mem.toMB).sum)
    val containersInUse = freePool.filter(_._2.activeActivationCount > 0) ++ busyPool
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_ACTIVE_COUNT, containersInUse.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_ACTIVE_SIZE,
      containersInUse.map(_._2.resources.mem.toMB).sum)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_PREWARM_COUNT,
      prewarmedPool.size + prewarmStartingPool.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_PREWARM_SIZE,
      prewarmedPool.map(_._2.resources.mem.toMB).sum + prewarmStartingPool.map(_._2._2.mem.toMB).sum)
    val unused = freePool.filter(_._2.activeActivationCount == 0)
    val unusedMB = unused.map(_._2.resources.mem.toMB).sum
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_IDLES_COUNT, unused.size)
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_IDLES_SIZE, unusedMB)
  }
}

object ContainerPool {

  /**
   * Calculate the memory of a given pool.
   *
   * @param pool The pool with the containers.
   * @return The memory consumption of all containers in the pool in Megabytes.
   */
  protected[containerpool] def resourceConsumptionOf[A](pool: Map[A, ContainerData]): RuntimeResources = {
    pool.map(_._2.resources).foldLeft(RuntimeResources.none())(_ + _)
  }

  /**
   * Finds the best container for a given job to run on.
   *
   * Selects an arbitrary warm container from the passed pool of idle containers
   * that matches the action and the invocation namespace. The implementation uses
   * matching such that structural equality of action and the invocation namespace
   * is required.
   * Returns None iff no matching container is in the idle pool.
   * Does not consider pre-warmed containers.
   *
   * @param action the action to run
   * @param invocationNamespace the namespace, that wants to run the action
   * @param idles a map of idle containers, awaiting work
   * @return a container if one found
   */
  protected[containerpool] def
  schedule[A](action: ExecutableWhiskAction,
                                           invocationNamespace: EntityName,
                                           idles: Map[A, ContainerData]): Option[(A, ContainerData)] = {
    idles.find {
      case (_, c@WarmedData(_, `invocationNamespace`, inits, _, _, _, _)) if c.hasCapacity() && inits.contains(action) => true
      case _ => false
    } orElse {
      idles.find {
        case (_, c@WarmingData(_, `invocationNamespace`, inits, _, _)) if c.hasCapacity() && inits.contains(action) => true
        case _ => false
      }
    } orElse {
      idles.find {
        case (_, c@WarmingColdData(`invocationNamespace`, inits, _, _)) if c.hasCapacity() && inits.contains(action) => true
        case _ => false
      }
    } orElse {
      idles.find {
//        case (_, c@WarmedData(_, `invocationNamespace`, _, _, _, _)) if c.hasCapacity() => true
        case _ => false
      }
    } orElse {
      idles.find {
//        case (_, c@WarmingData(_, `invocationNamespace`, _, _, _)) if c.hasCapacity() => true
        case _ => false
      }
    }
  }

  /**
   * Finds the oldest previously used container to remove to make space for the job passed to run.
   * Depending on the space that has to be allocated, several containers might be removed.
   *
   * NOTE: This method is never called to remove an action that is in the pool already,
   * since this would be picked up earlier in the scheduler and the container reused.
   *
   * @param pool a map of all free containers in the pool
   * @param resources the amount of resources that has to be freed up
   * @return a list of containers to be removed iff found
   */
  @tailrec
  protected[containerpool] def remove[A](pool: Map[A, ContainerData],
                                         resources: RuntimeResources,
                                         toRemove: List[A] = List.empty): List[A] = {
    // Try to find a Free container that does NOT have any active activations AND is initialized with any OTHER action
    val freeContainers = pool.collect {
      // Only warm containers will be removed. Prewarmed containers will stay always.
      case (ref, w: WarmedData) if w.activeActivationCount == 0 =>
        ref -> w
    }

    if (resources.exceedsAnyOf(RuntimeResources.none()) && freeContainers.nonEmpty && resourceConsumptionOf(freeContainers).exceedsAnyOf(resources)) {
      // Remove the oldest container if:
      // - there is more memory required
      // - there are still containers that can be removed
      // - there are enough free containers that can be removed
      val (ref, data) = freeContainers.minBy(_._2.lastUsed)
      // Catch exception if remaining memory will be negative
      val remainingResources = Try(resources - data.resources).getOrElse(RuntimeResources.none())
      remove(freeContainers - ref, remainingResources, toRemove ++ List(ref))
    } else {
      // If this is the first call: All containers are in use currently, or there is more memory needed than
      // containers can be removed.
      // Or, if this is one of the recursions: Enough containers are found to get the memory, that is
      // necessary. -> Abort recursion
      toRemove
    }
  }

  /**
   * Find the expired actor in prewarmedPool
   *
   * @param poolConfig
   * @param prewarmConfig
   * @param prewarmedPool
   * @param logging
   * @return a list of expired actor
   */
  def removeExpired[A](poolConfig: ContainerPoolConfig,
                       prewarmConfig: List[PrewarmingConfig],
                       prewarmedPool: Map[A, PreWarmedData])(implicit logging: Logging): List[A] = {
    val now = Deadline.now
    val expireds = prewarmConfig
      .flatMap { config =>
        val kind = config.exec.kind
        config.reactive
          .map { c =>
            val exps = prewarmedPool.toSeq.map(f => f._2.isExpired())
            val expiredPrewarmedContainer = prewarmedPool.toSeq
              .filter { warmInfo =>
                warmInfo match {
                  case (_, p @ PreWarmedData(_, `kind`, _, _, _)) if p.isExpired() => true
                  case _                                                                  => false
                }
              }
              .sortBy(_._2.expires.getOrElse(now))

            // emit expired container counter metric with memory + kind
            MetricEmitter.emitCounterMetric(LoggingMarkers.CONTAINER_POOL_PREWARM_EXPIRED(null, kind))
            if (expiredPrewarmedContainer.nonEmpty) {
              logging.info(
                this,
                s"[kind: ${kind}] ${expiredPrewarmedContainer.size} expired prewarmed containers")
            }
            expiredPrewarmedContainer.map(e => (e._1, e._2.expires.getOrElse(now)))
          }
          .getOrElse(List.empty)
      }
      .sortBy(_._2) //need to sort these so that if the results are limited, we take the oldest
      .map(_._1)
    if (expireds.nonEmpty) {
      logging.info(this, s"removing up to ${poolConfig.prewarmExpirationLimit} of ${expireds.size} expired containers")
      expireds.take(poolConfig.prewarmExpirationLimit).foreach { e =>
        prewarmedPool.get(e).map { d =>
          logging.info(this, s"removing expired prewarm of kind ${d.kind} with container ${d.container} ")
        }
      }
    }
    expireds.take(poolConfig.prewarmExpirationLimit)
  }

  /**
   * Find the increased number for the prewarmed kind
   *
   * @param init
   * @param scheduled
   * @param coldStartCount
   * @param prewarmConfig
   * @param prewarmedPool
   * @param prewarmStartingPool
   * @param logging
   * @return the current number and increased number for the kind in the Map
   */
  def increasePrewarms(init: Boolean,
                       scheduled: Boolean,
                       coldStartCount: Map[ColdStartKey, Int],
                       prewarmConfig: List[PrewarmingConfig],
                       prewarmedPool: Map[ActorRef, PreWarmedData],
                       prewarmStartingPool: Map[ActorRef, (String, RuntimeResources)])(
    implicit logging: Logging): Map[PrewarmingConfig, (Int, Int)] = {
    prewarmConfig.map { config =>
      val kind = config.exec.kind
      val resources = config.resources

      val runningCount = prewarmedPool.count {
        // done starting (include expired, since they may not have been removed yet)
        case (_, p @ PreWarmedData(_, `kind`, resources, _, _)) => true
        // started but not finished starting (or expired)
        case _ => false
      }
      val startingCount = prewarmStartingPool.count(p => p._2._1 == kind && p._2._2 == resources)
      val currentCount = runningCount + startingCount

      // determine how many are needed
      val desiredCount: Int =
        if (init) config.initialCount
        else {
          if (scheduled) {
            // scheduled/reactive config backfill
            config.reactive
              .map(c => getReactiveCold(coldStartCount, c, kind, resources).getOrElse(c.minCount)) //reactive -> desired is either cold start driven, or minCount
              .getOrElse(config.initialCount) //not reactive -> desired is always initial count
          } else {
            // normal backfill after removal - make sure at least minCount or initialCount is started
            config.reactive.map(_.minCount).getOrElse(config.initialCount)
          }
        }

      if (currentCount < desiredCount) {
        logging.info(
          this,
          s"found ${currentCount} started and ${startingCount} starting; ${if (init) "initing" else "backfilling"} ${desiredCount - currentCount} pre-warms to desired count: ${desiredCount} for kind:${config.exec.kind} resources:${config.resources.toString}")(
          TransactionId.invokerWarmup)
      }
      (config, (currentCount, desiredCount))
    }.toMap
  }

  /**
   * Get the required prewarmed container number according to the cold start happened in previous minute
   *
   * @param coldStartCount
   * @param config
   * @param kind
   * @param resources
   * @return the required prewarmed container number
   */
  def getReactiveCold(coldStartCount: Map[ColdStartKey, Int],
                      config: ReactivePrewarmingConfig,
                      kind: String,
                      resources: RuntimeResources): Option[Int] = {
    coldStartCount.get(ColdStartKey(kind, resources)).map { value =>
      // Let's assume that threshold is `2`, increment is `1` in runtimes.json
      // if cold start number in previous minute is `2`, requireCount is `2/2 * 1 = 1`
      // if cold start number in previous minute is `4`, requireCount is `4/2 * 1 = 2`
      math.min(math.max(config.minCount, (value / config.threshold) * config.increment), config.maxCount)
    }
  }

  def props(factory: ActorRefFactory => ActorRef,
            poolConfig: ContainerPoolConfig,
            feed: ActorRef,
            prewarmConfig: List[PrewarmingConfig] = List.empty,
            proxyAddressBook: Option[ActorProxyAddressBook] = None,
           )(implicit logging: Logging) =
    Props(new ContainerPool(factory, feed, prewarmConfig, poolConfig, proxyAddressBook))
}

/** Contains settings needed to perform container prewarming. */
case class PrewarmingConfig(initialCount: Int,
                            exec: CodeExec[_],
                            resources: RuntimeResources,
                            reactive: Option[ReactivePrewarmingConfig] = None)
