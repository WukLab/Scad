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

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, FSM, Props, Stash}

import java.time.Instant
import akka.actor.Status.{Failure => FailureMessage}
import akka.event.Logging.InfoLevel
import akka.io.IO
import akka.io.Tcp
import akka.io.Tcp.Close
import akka.io.Tcp.CommandFailed
import akka.io.Tcp.Connect
import akka.io.Tcp.Connected
import akka.pattern.pipe
import pureconfig.loadConfigOrThrow
import pureconfig.generic.auto._
import akka.stream.ActorMaterializer

import java.net.InetSocketAddress
import java.net.SocketException
import org.apache.openwhisk.common.MetricEmitter
import org.apache.openwhisk.common.TransactionId.systemPrefix

import scala.collection.immutable
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.openwhisk.common.{AkkaLogging, Counter, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.ack.ActiveAck
import org.apache.openwhisk.core.connector.{ActivationMessage, CombinedCompletionAndResultMessage, CompletionMessage, MessageProducer, ResultMessage}
import org.apache.openwhisk.core.containerpool.logging.LogCollectingException
import org.apache.openwhisk.core.database.UserContext
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.invoker.Invoker.LogsCollector
import org.apache.openwhisk.core.invoker.{PrewarmDeadlineCache, PrewarmDeadlineCacheActor, ResultActivation, RunFinishedMessage}
import org.apache.openwhisk.core.scheduler.FinishActivation
import org.apache.openwhisk.http.Messages

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

// States
sealed trait ContainerState
case object Uninitialized extends ContainerState
case object Starting extends ContainerState
case object Started extends ContainerState
case object Running extends ContainerState
case object Ready extends ContainerState
case object Pausing extends ContainerState
case object Paused extends ContainerState
case object Removing extends ContainerState

sealed trait RuntimeSpec
case object UserFunctionSpec extends RuntimeSpec
case object MemorySpec extends RuntimeSpec
case object RemoteMemorySpec extends RuntimeSpec
case object LocalStorageSpec extends RuntimeSpec
case object RemoteStorageSpec extends RuntimeSpec

// Data
/** Base data type */
sealed abstract class ContainerData(val lastUsed: Instant, val runtimeSpec: RuntimeSpec, val resources: RuntimeResources, val activeActivationCount: Int) {

  /** When ContainerProxy in this state is scheduled, it may result in a new state (ContainerData)*/
  def nextRun(r: Run): ContainerData

  /**
   *  Return Some(container) (for ContainerStarted instances) or None(for ContainerNotStarted instances)
   *  Useful for cases where all ContainerData instances are handled, vs cases where only ContainerStarted
   *  instances are handled */
  def getContainer: Option[Container]

  /** String to indicate the state of this container after scheduling */
  val initingState: String

  /** Inidicates whether this container can service additional activations */
  def hasCapacity(): Boolean
}

/** abstract type to indicate an unstarted container */
sealed abstract class ContainerNotStarted(override val lastUsed: Instant,
                                          override val runtimeSpec: RuntimeSpec,
                                          override val resources: RuntimeResources,
                                          override val activeActivationCount: Int)
    extends ContainerData(lastUsed, runtimeSpec, resources, activeActivationCount) {
  override def getContainer = None
  override val initingState = "cold"
}

/** abstract type to indicate a started container */
sealed abstract class ContainerStarted(val container: Container,
                                       override val lastUsed: Instant,
                                       override val runtimeSpec: RuntimeSpec,
                                       override val resources: RuntimeResources,
                                       override val activeActivationCount: Int)
    extends ContainerData(lastUsed, runtimeSpec, resources, activeActivationCount) {
  override def getContainer = Some(container)
}

object ActionUtils {
  def resourceLimits(actions: Set[ExecutableWhiskAction]): RuntimeResources = {
    actions.map(_.limits.resources.limits).reduceLeft(_ + _)
  }
}

/** trait representing a container that is in use and (potentially) usable by subsequent or concurrent activations */
sealed abstract trait ContainerInUse {
  val activeActivationCount: Int
  val actions: Set[ExecutableWhiskAction]
  def maxConcurrent = actions.map(a => a.limits.concurrency.maxConcurrent).sum
  def hasCapacity() =
    activeActivationCount < maxConcurrent
}

/** trait representing a container that is NOT in use and is usable by subsequent activation(s) */
sealed abstract trait ContainerNotInUse {
  def hasCapacity() = true
}

/** type representing a cold (not running) container */
case class NoData(override val activeActivationCount: Int = 0)
    extends ContainerNotStarted(Instant.EPOCH, UserFunctionSpec, new RuntimeResources(0, 0.B,0.B), activeActivationCount)
    with ContainerNotInUse {
  override def nextRun(r: Run) = WarmingColdData(r.msg.user.namespace.name, Set(r.action), Instant.now, 1)
}

/** type representing a cold (not running) container with specific resource allocation */
case class ResourceData(override val resources: RuntimeResources, override val activeActivationCount: Int = 0)
    extends ContainerNotStarted(Instant.EPOCH, UserFunctionSpec, resources, activeActivationCount)
    with ContainerNotInUse {
  override def nextRun(r: Run) = WarmingColdData(r.msg.user.namespace.name, Set(r.action), Instant.now, 1)
}

/** type representing a prewarmed (running, but unused) container (with a specific resource allocation) */
case class PreWarmedData(override val container: Container,
                         kind: String,
                         override val resources: RuntimeResources,
                         override val activeActivationCount: Int = 0,
                         expires: Option[Deadline] = None)
    extends ContainerStarted(container, Instant.EPOCH, UserFunctionSpec, resources, activeActivationCount)
    with ContainerNotInUse {
  override val initingState = "prewarmed"
  override def nextRun(r: Run) =
    WarmingData(container, r.msg.user.namespace.name, Set(r.action), Instant.now, 1)
  def isExpired(): Boolean = expires.exists(_.isOverdue())
}

/** type representing a prewarm (running, but not used) container that is being initialized (for a specific action + invocation namespace) */
case class WarmingData(override val container: Container,
                       invocationNamespace: EntityName,
                       actions: Set[ExecutableWhiskAction],
                       override val lastUsed: Instant,
                       override val activeActivationCount: Int = 0)
    extends ContainerStarted(container, lastUsed, UserFunctionSpec, ActionUtils.resourceLimits(actions), activeActivationCount)
    with ContainerInUse {
  override val initingState = "warming"
  override def nextRun(r: Run) = copy(actions = actions + r.action, lastUsed = Instant.now, activeActivationCount = activeActivationCount + 1)
}

/** type representing a cold (not yet running) container that is being initialized (for a specific action + invocation namespace) */
case class WarmingColdData(invocationNamespace: EntityName,
                           actions: Set[ExecutableWhiskAction],
                           override val lastUsed: Instant,
                           override val activeActivationCount: Int = 0)
    extends ContainerNotStarted(lastUsed, UserFunctionSpec, ActionUtils.resourceLimits(actions), activeActivationCount)
    with ContainerInUse {
  override val initingState = "warmingCold"
  override def nextRun(r: Run) = copy(actions = actions + r.action, lastUsed = Instant.now, activeActivationCount = activeActivationCount + 1)
}

/** type representing a warm container that has already been in use (for a specific action + invocation namespace) */
case class WarmedData(override val container: Container,
                      invocationNamespace: EntityName,
                      actions: Set[ExecutableWhiskAction],
                      override val lastUsed: Instant,
                      override val activeActivationCount: Int = 0,
                      resumeRun: Option[Run] = None)
    extends ContainerStarted(container, lastUsed, UserFunctionSpec, ActionUtils.resourceLimits(actions), activeActivationCount)
    with ContainerInUse {
  override val initingState = "warmed"
  override def nextRun(r: Run) = copy(actions = actions + r.action, lastUsed = Instant.now, activeActivationCount = activeActivationCount + 1)
  //track the resuming run for easily referring to the action being resumed (it may fail and be resent)
  def withoutResumeRun() = this.copy(resumeRun = None)
  def withResumeRun(job: Run) = this.copy(resumeRun = Some(job))
}

// Events received by the actor
case class Start(exec: CodeExec[_], resources: RuntimeResources, ttl: Option[FiniteDuration] = None)
case class Run(action: ExecutableWhiskAction, msg: ActivationMessage, retryLogDeadline: Option[Deadline] = None,
               corunningConfig: Option[Seq[String]] = None)
case object Remove
case class HealthPingEnabled(enabled: Boolean)
//   Added for communication with libd
case class LibdActionConfig(activationId: ActivationId, actionName: String, transports: Seq[String])
case class LibdTransportConfig(activationId: ActivationId, request: TransportRequest, result: TransportAddress)
//   Added for component broadcast
case class TransportReady(activationId: ActivationId, transportAddress: TransportAddress)
case class ColdStarted(preWarmedData: PreWarmedData)
case class ObjectEnd(activationId: ActivationId)

// Events sent by the actor
case class NeedWork(data: ContainerData)
case object ContainerPaused
case class ContainerRemoved(replacePrewarm: Boolean) // when container is destroyed
case object RescheduleJob // job is sent back to parent and could not be processed because container is being destroyed
case class PreWarmCompleted(data: PreWarmedData)
case class InitCompleted(data: WarmedData)
case object RunCompleted

/**
 * A proxy that wraps a Container. It is used to keep track of the lifecycle
 * of a container and to guarantee a contract between the client of the container
 * and the container itself.
 *
 * The contract is as follows:
 * 1. If action.limits.concurrency.maxConcurrent == 1:
 *    Only one job is to be sent to the ContainerProxy at one time. ContainerProxy
 *    will delay all further jobs until a previous job has finished.
 *
 *    1a. The next job can be sent to the ContainerProxy after it indicates available
 *       capacity by sending NeedWork to its parent.
 *
 * 2. If action.limits.concurrency.maxConcurrent > 1:
 *    Parent must coordinate with ContainerProxy to attempt to send only data.action.limits.concurrency.maxConcurrent
 *    jobs for concurrent processing.
 *
 *    Since the current job count is only periodically sent to parent, the number of jobs
 *    sent to ContainerProxy may exceed data.action.limits.concurrency.maxConcurrent,
 *    in which case jobs are buffered, so that only a max of action.limits.concurrency.maxConcurrent
 *    are ever sent into the container concurrently. Parent will NOT be signalled to send more jobs until
 *    buffered jobs are completed, but their order is not guaranteed.
 *
 *    2a. The next job can be sent to the ContainerProxy after ContainerProxy has "concurrent capacity",
 *        indicated by sending NeedWork to its parent.
 *
 * 3. A Remove message can be sent at any point in time. Like multiple jobs though,
 *    it will be delayed until the currently running job finishes.
 *
 * @constructor
 * @param factory a function generating a Container
 * @param sendActiveAck a function sending the activation via active ack
 * @param storeActivation a function storing the activation in a persistent store
 * @param unusedTimeout time after which the container is automatically thrown away
 * @param pauseGrace time to wait for new work before pausing the container
 */
class ContainerProxy(factory: (TransactionId,
                               String,
                               ImageName,
                               Boolean,
                               ByteSize,
                               Int,
                               Option[ExecutableWhiskAction]) => Future[Container],
                     sendActiveAck: ActiveAck,
                     storeActivation: (TransactionId, WhiskActivation, Boolean, UserContext) => Future[Any],
                     collectLogs: LogsCollector,
                     instance: InvokerInstanceId,
                     poolConfig: ContainerPoolConfig,
                     healtCheckConfig: ContainerProxyHealthCheckConfig,
                     activationErrorLoggingConfig: ContainerProxyActivationErrorLogConfig,
                     unusedTimeout: FiniteDuration,
                     pauseGrace: FiniteDuration,
                     msgProducer: MessageProducer,
                     prewarmDeadlineCache: ActorRef,
                     testTcp: Option[ActorRef],
                     resultWaiter: Option[ActorRef],
                     addressBook: Option[ActorProxyAddressBook]
                    )
    extends FSM[ContainerState, ContainerData]
    with Stash {
  implicit val ec: ExecutionContextExecutor = context.system.dispatcher
  implicit val logging: AkkaLogging = new AkkaLogging(context.system.log)
  implicit val ac: ActorSystem = context.system
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  var rescheduleJob = false // true iff actor receives a job but cannot process it because actor will destroy itself
  var runBuffer: Queue[Run] = immutable.Queue.empty[Run] //does not retain order, but does manage jobs that would have pushed past action concurrency limit
  //track buffer processing state to avoid extra transitions near end of buffer - this provides a pseudo-state between Running and Ready
  var bufferProcessing = false

  var configBuffer: Queue[LibdTransportConfig] = immutable.Queue.empty[LibdTransportConfig] //does not retain order, but does manage jobs that would have pushed past action concurrency limit

  //keep a separate count to avoid confusion with ContainerState.activeActivationCount that is tracked/modified only in ContainerPool
  var activeCount = 0;
  var healthPingActor: Option[ActorRef] = None //setup after prewarm starts
  val tcp: ActorRef = testTcp.getOrElse(IO(Tcp)) //allows to testing interaction with Tcp extension

  startWith(Uninitialized, NoData())

  when(Uninitialized) {
    // pre warm a container (creates a stem cell container)
    case Event(job: Start, _) =>
      factory(
        TransactionId.invokerWarmup,
        ContainerProxy.containerName(instance, "prewarm", job.exec.kind),
        job.exec.image,
        job.exec.pull,
        job.resources.mem,
        poolConfig.cpuShare(job.resources),
        None)
        .map(container =>
          PreWarmCompleted(PreWarmedData(container, job.exec.kind, job.resources, expires = job.ttl.map(_.fromNow).orElse(Some(poolConfig.prewarmExpirationCheckInterval.fromNow) ))))
        .pipeTo(self)

      goto(Starting)

    // cold start (no container to reuse or available stem cell container)
    case Event(job: Run, _) =>
      implicit val transid: TransactionId = job.msg.transid
      activeCount += 1
      // create a new container
      val name = job.msg.swapFrom match {
        case Some(value) =>
          // This is a swap container, change name to activation ID
          job.msg.activationId.toString
        case None =>
          ContainerProxy.containerName(instance, job.msg.user.namespace.name.asString, job.action.name.asString)

      }
      val container = factory(
        job.msg.transid,
        name,
        job.action.exec.image,
        job.action.exec.pull,
        job.action.limits.resources.limits.mem,
        poolConfig.cpuShare(job.action.limits.resources.limits),
        Some(job.action))

      // container factory will either yield a new container ready to execute the action, or
      // starting up the container failed; for the latter, it's either an internal error starting
      // a container or a docker action that is not conforming to the required action API
      container
        .andThen {
          case Success(container) =>
            // the container is ready to accept an activation; register it as PreWarmed; this
            // normalizes the life cycle for containers and their cleanup when activations fail
            self ! PreWarmCompleted(
              PreWarmedData(container, job.action.exec.kind, job.action.limits.resources.limits, 1, expires = Some(poolConfig.prewarmExpirationCheckInterval.fromNow)))

          case Failure(t) =>
            // the container did not come up cleanly, so disambiguate the failure mode and then cleanup
            // the failure is either the system fault, or for docker actions, the application/developer fault
            val response = t match {
              case WhiskContainerStartupError(msg) => ActivationResponse.whiskError(msg)
              case BlackboxStartupError(msg)       => ActivationResponse.developerError(msg)
              case _                               => ActivationResponse.whiskError(Messages.resourceProvisionError)
            }
            val context = UserContext(job.msg.user)
            // construct an appropriate activation and record it in the datastore,
            // also update the feed and active ack; the container cleanup is queued
            // implicitly via a FailureMessage which will be processed later when the state
            // transitions to Running
            val activation = ContainerProxy.constructWhiskActivation(job, None, Interval.zero, false, response)
            sendActiveAck(
              transid,
              activation,
              job.msg.blocking,
              job.msg.rootControllerIndex,
              job.msg.user.namespace.uuid,
              CombinedCompletionAndResultMessage(transid, activation, instance))
            storeActivation(transid, activation, job.msg.blocking, context)
        }
        .flatMap { container =>
          // now attempt to inject the user code and run the action
          initializeAndRun(container, job)
            .map(_ => RunCompleted)
        }
        .pipeTo(self)

      goto(Running)

    // Get config before
    case Event(config: LibdTransportConfig, _) =>
      logging.debug(this, s"Get config uninit")
      configBuffer = configBuffer.enqueue(config)
      stay()
  }

  when(Starting) {
    // container was successfully obtained
    case Event(completed: PreWarmCompleted, _) =>
      context.parent ! NeedWork(completed.data)
      goto(Started) using completed.data

    // container creation failed
    case Event(_: FailureMessage, _) =>
      context.parent ! ContainerRemoved(true)
      stop()

    // Get config before
    case Event(config: LibdTransportConfig, _) =>
      logging.debug(this, s"Get config starting")
      configBuffer = configBuffer.enqueue(config)
      stay()

    case _ => delay
  }

  when(Started) {
    case Event(job: Run, data: PreWarmedData) =>
      implicit val transid = job.msg.transid
      activeCount += 1
      initializeAndRun(data.container, job)
        .map(_ => RunCompleted)
        .pipeTo(self)
      goto(Running) using PreWarmedData(data.container, data.kind, data.resources, 1, data.expires)

    case Event(Remove, data: PreWarmedData) => destroyContainer(data, replacePrewarm = false)

    // prewarm container failed
    case Event(_: FailureMessage, data: PreWarmedData) =>
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_PREWARM)
      destroyContainer(data, replacePrewarm = true)

    case Event(config: LibdTransportConfig, _) =>
      logging.debug(this, s"Get config started")
      configBuffer = configBuffer.enqueue(config)
      stay()
  }

  when(Running) {
    // Intermediate state, we were able to start a container
    // and we keep it in case we need to destroy it.
    case Event(completed: PreWarmCompleted, _) =>
      logging.debug(this, s"CALL 1")
      context.parent ! ColdStarted(completed.data)
      stay using completed.data

    // Run during prewarm init (for concurrent > 1)
    case Event(job: Run, data: PreWarmedData) =>
      implicit val transid: TransactionId = job.msg.transid
      logging.debug(this, s"CALL 2")
      logging.info(this, s"buffering for warming container ${data.container}; ${activeCount} activations in flight")
      runBuffer = runBuffer.enqueue(job)
      stay()

    // Run during cold init (for concurrent > 1)
    case Event(job: Run, _: NoData) =>
      implicit val transid: TransactionId = job.msg.transid
      logging.debug(this, s"CALL 3")
      logging.info(this, s"buffering for cold warming container ${activeCount} activations in flight")
      runBuffer = runBuffer.enqueue(job)
      stay()

    // Get Config after init
    case Event(config: LibdTransportConfig, data : WarmedData) =>
      logging.debug(this, s"Get config after init")
      val res = sendConfig(config, data.container)
      stay()

    // Get config before init
    case Event(config: LibdTransportConfig, _) =>
      logging.debug(this, s"Get config before init")
      configBuffer = configBuffer.enqueue(config)
      stay()

    // Init was successful
    // Try to "Pop Add Transport" here
    case Event(completed: InitCompleted, data : PreWarmedData) =>
      logging.debug(this, s"CALL 4")
      // Its safe to dump all user config to container
      configBuffer.foreach (sendConfig(_, data.container))
      configBuffer = Queue.empty

      processBuffer(completed.data, completed.data)
      stay using completed.data

    // Init was successful
    case Event(data: WarmedData, _: PreWarmedData) =>
      logging.debug(this, s"CALL 5")
      //in case concurrency supported, multiple runs can begin as soon as init is complete
      context.parent ! NeedWork(data)
      stay using data

    // Run was successful
    case Event(RunCompleted, data: WarmedData) =>
      logging.debug(this, s"CALL 6")
      activeCount -= 1
      val newData = data.withoutResumeRun()
      //if there are items in runbuffer, process them if there is capacity, and stay; otherwise if we have any pending activations, also stay
      if (requestWork(data) || activeCount > 0) {
        stay using newData
      } else {
        goto(Ready) using newData
      }
    case Event(job: Run, data: WarmedData)
        if activeCount >= data.maxConcurrent && !rescheduleJob => //if we are over concurrency limit, and not a failure on resume
      logging.debug(this, s"CALL 7")
      implicit val transid = job.msg.transid
      logging.warn(this, s"buffering for maxed warm container ${data.container}; ${activeCount} activations in flight")
      runBuffer = runBuffer.enqueue(job)
      stay()
    case Event(job: Run, data: WarmedData)
        if activeCount < data.maxConcurrent && !rescheduleJob => //if there was a delay, and not a failure on resume, skip the run
      logging.debug(this, s"CALL 8")
      activeCount += 1
      implicit val transid = job.msg.transid
      bufferProcessing = false //reset buffer processing state
      initializeAndRun(data.container, job)
        .map(_ => RunCompleted)
        .pipeTo(self)
      stay() using data

    //ContainerHealthError should cause rescheduling of the job
    case Event(FailureMessage(e: ContainerHealthError), data: WarmedData) =>
      logging.debug(this, s"CALL 9")
      implicit val tid = e.tid
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_WARM)
      //resend to self will send to parent once we get to Removing state
      val newData = data.resumeRun
        .map { run =>
          logging.warn(this, "Ready warm container unhealthy, will retry activation.")
          self ! run
          data.withoutResumeRun()
        }
        .getOrElse(data)
      rescheduleJob = true
      rejectBuffered()
      destroyContainer(newData, true)

    // Failed after /init (the first run failed) on prewarmed or cold start
    // - container will be destroyed
    // - buffered will be aborted (if init fails, we assume it will always fail)
    case Event(f: FailureMessage, data: PreWarmedData) =>
      logging.debug(this, s"CALL 10")
      logging.error(
        this,
        s"Failed during init of cold container ${data.getContainer}, queued activations will be aborted.")

      activeCount -= 1
      //reuse an existing init failure for any buffered activations that will be aborted
      val r = f.cause match {
        case ActivationUnsuccessfulError(r) => Some(r.response)
        case _                              => None
      }
      destroyContainer(data, true, true, r)

    // Failed for a subsequent /run
    // - container will be destroyed
    // - buffered will be resent (at least 1 has completed, so others are given a chance to complete)
    case Event(fm: FailureMessage, data: WarmedData) =>
      logging.error(
        this,
        s"Failed during use of warm container ${data.getContainer}, queued activations will be resent. msg: ${fm}")
      activeCount -= 1
      if (activeCount == 0) {
        destroyContainer(data, true)
      } else {
        //signal that this container is going away (but don't remove it yet...)
        rescheduleJob = true
        goto(Removing)
      }

    // Failed at getting a container for a cold-start run
    // - container will be destroyed
    // - buffered will be aborted (if cold start container fails to start, we assume it will continue to fail)
    case Event(_: FailureMessage, _) =>
      logging.error(this, "Failed to start cold container, queued activations will be aborted.")
      activeCount -= 1
      context.parent ! ContainerRemoved(true)
      abortBuffered()
      rescheduleJob = true
      goto(Removing)

    case _ => delay
  }

  when(Ready, stateTimeout = pauseGrace) {
    case Event(job: Run, data: WarmedData) =>
      implicit val transid: TransactionId = job.msg.transid
      activeCount += 1
      val newData = data.withResumeRun(job)
      initializeAndRun(data.container, job, true)
        .map(_ => RunCompleted)
        .pipeTo(self)

      goto(Running) using newData

    // pause grace timed out
    case Event(StateTimeout, data: WarmedData) =>
      data.container.suspend()(TransactionId.invokerNanny).map(_ => ContainerPaused).pipeTo(self)
      goto(Pausing)

    case Event(Remove, data: WarmedData) => destroyContainer(data, true)

    // warm container failed
    case Event(_: FailureMessage, data: WarmedData) =>
      destroyContainer(data, true)

      // Send message to runtime when ready
    case Event(config: LibdTransportConfig, data : WarmedData) =>
      logging.debug(this, s"Get config in ready")
      val res = sendConfig(config, data.container)
      stay()
  }

  when(Pausing) {
    case Event(ContainerPaused, data: WarmedData)   => goto(Paused)
    case Event(_: FailureMessage, data: WarmedData) => destroyContainer(data, true)
    case _                                          => delay
  }

  when(Paused, stateTimeout = unusedTimeout) {
    case Event(job: Run, data: WarmedData) =>
      implicit val transid = job.msg.transid
      activeCount += 1
      val newData = data.withResumeRun(job)
      data.container
        .resume()
        .andThen {
          // Sending the message to self on a failure will cause the message
          // to ultimately be sent back to the parent (which will retry it)
          // when container removal is done.
          case Failure(_) =>
            rescheduleJob = true
            self ! job
        }
        .flatMap(_ => initializeAndRun(data.container, job, true))
        .map(_ => RunCompleted)
        .pipeTo(self)
      goto(Running) using newData

    // container is reclaimed by the pool or it has become too old
    case Event(StateTimeout | Remove, data: WarmedData) =>
      rescheduleJob = true // to supress sending message to the pool and not double count
      destroyContainer(data, true)

    case Event(config: LibdTransportConfig, data : WarmedData) =>
      logging.debug(this, s"Get config in Paused")
      val res = sendConfig(config, data.container)
      stay()
  }

  when(Removing) {
    case Event(job: Run, _) =>
      // Send the job back to the pool to be rescheduled
      context.parent ! job
      stay
    // Run was successful, after another failed concurrent Run
    case Event(RunCompleted, data: WarmedData) =>
      activeCount -= 1
      val newData = data.withoutResumeRun()
      //if there are items in runbuffer, process them if there is capacity, and stay; otherwise if we have any pending activations, also stay
      if (activeCount == 0) {
        destroyContainer(newData, replacePrewarm = true)
      } else {
        stay using newData
      }
    case Event(ContainerRemoved(_), _) =>
      stop()
    // Run failed, after another failed concurrent Run
    case Event(_: FailureMessage, data: WarmedData) =>
      activeCount -= 1
      val newData = data.withoutResumeRun()
      if (activeCount == 0) {
        destroyContainer(newData, replacePrewarm = true)
      } else {
        stay using newData
      }
  }

  whenUnhandled {
    case Event(action : LibdActionConfig, data) =>
      val tidOpt = runBuffer.find(_.msg.activationId == action.activationId).map(_.msg.transid)
      implicit val tid = tidOpt.get

      val res = data.getContainer
          .foreach(_.addAction("", action.actionName, action.activationId, Some(action.transports)))

      stay()

  }

  // Unstash all messages stashed while in intermediate state
  onTransition {
    case _ -> Started =>
      if (healtCheckConfig.enabled) {
        logging.debug(this, "enabling health ping on Started")
        nextStateData.getContainer.foreach { c =>
          enableHealthPing(c)
        }
      }
      unstashAll()
    case _ -> Running =>
      if (healtCheckConfig.enabled && healthPingActor.isDefined) {
        logging.debug(this, "disabling health ping on Running")
        disableHealthPing()
      }
    case _ -> Ready =>
      unstashAll()
    case _ -> Paused =>
      unstashAll()
    case _ -> Removing =>
      unstashAll()
  }

  initialize()

  /** Either process runbuffer or signal parent to send work; return true if runbuffer is being processed */
  def requestWork(newData: WarmedData): Boolean = {
    //if there is concurrency capacity, process runbuffer, signal NeedWork, or both
    if (activeCount < newData.maxConcurrent) {
      if (runBuffer.nonEmpty) {
        //only request work once, if available larger than runbuffer
        val available = newData.maxConcurrent - activeCount
        val needWork: Boolean = available > runBuffer.size
        processBuffer(newData, newData)
        if (needWork) {
          //after buffer processing, then send NeedWork
          context.parent ! NeedWork(newData)
        }
        true
      } else {
        context.parent ! NeedWork(newData)
        bufferProcessing //true in case buffer is still in process
      }
    } else {
      false
    }
  }

  def sendConfig(config: LibdTransportConfig, container: Container) = config match {
    case LibdTransportConfig(activationId, request, result) =>
      val address = request.address + result
      logging.debug(this, s"transport config: sending ${address} to ${container}")

      config.request.operation match {
        case TransportRequest.Create =>
          container.addTransport(activationId, address.toFullString)
        case TransportRequest.Config =>
          container.configTransport(activationId, request.name, address.toConfigString)
        case TransportRequest.GetMessage =>
          // TODO: change the create parameter
          container.getMessages(activationId).map {
            _.foreach { resp =>
              val reply = resp
                .entity
                .parseJson
                .convertTo[LibdMessagesReply]

              reply
                .messages
                .foreach { case (trans, msg) =>
                  val srcAddr = ProxyAddress(activationId, trans)
                  addressBook
                    .get
                    .finishReply(self, srcAddr, TransportAddress.ProxyTransport(msg))
                }
            }
          }
          /* end case */
      }
  }

  /** Process buffered items up to the capacity of action concurrency config */
  def processBuffer(completed: WarmedData, newData: ContainerData): Unit = {
    //send as many buffered as possible
    val available = completed.maxConcurrent - activeCount
    logging.info(this, s"resending up to ${available} from ${runBuffer.length} buffered jobs")
    1 to available foreach { _ =>
      runBuffer.dequeueOption match {
        case Some((run, q)) =>
          self ! run
          bufferProcessing = true
          runBuffer = q
        case _ =>
      }
    }
  }

  /** Delays all incoming messages until unstashAll() is called */
  def delay: State = {
    stash()
    stay
  }

  /**
   * Destroys the container after unpausing it if needed. Can be used
   * as a state progression as it goes to Removing.
   *
   * @param newData the ContainerStarted which container will be destroyed
   */
  def destroyContainer(newData: ContainerStarted,
                       replacePrewarm: Boolean,
                       abort: Boolean = false,
                       abortResponse: Option[ActivationResponse] = None) = {
    val container = newData.container
    if (!rescheduleJob) {
      context.parent ! ContainerRemoved(replacePrewarm)
    } else {
      context.parent ! RescheduleJob
    }
    val abortProcess = if (abort && runBuffer.nonEmpty) {
      abortBuffered(abortResponse)
    } else {
      rejectBuffered()
      Future.successful(())
    }

    val unpause = stateName match {
      case Paused => container.resume()(TransactionId.invokerNanny)
      case _      => Future.successful(())
    }

    unpause
      .flatMap(_ => container.destroy()(TransactionId.invokerNanny))
      .flatMap(_ => abortProcess)
      .map(_ => ContainerRemoved(replacePrewarm))
      .pipeTo(self)
    if (stateName != Removing) {
      goto(Removing) using newData
    } else {
      stay using newData
    }
  }

  def abortBuffered(abortResponse: Option[ActivationResponse] = None): Future[Any] = {
    logging.info(this, s"aborting ${runBuffer.length} queued activations after failed init or failed cold start")
    val f = runBuffer.flatMap { job =>
      implicit val tid = job.msg.transid
      logging.info(
        this,
        s"aborting activation ${job.msg.activationId} after failed init or cold start with ${abortResponse}")
      val result = ContainerProxy.constructWhiskActivation(
        job,
        None,
        Interval.zero,
        false,
        abortResponse.getOrElse(ActivationResponse.whiskError(Messages.abnormalRun)))
      val context = UserContext(job.msg.user)
      val msg = if (job.msg.blocking) {
        CombinedCompletionAndResultMessage(tid, result, instance)
      } else {
        CompletionMessage(tid, result, instance)
      }
      val ack =
        sendActiveAck(tid, result, job.msg.blocking, job.msg.rootControllerIndex, job.msg.user.namespace.uuid, msg)
          .andThen {
            case Failure(e) => logging.error(this, s"failed to send abort ack $e")
          }
      val store = storeActivation(tid, result, job.msg.blocking, context)
        .andThen {
          case Failure(e) => logging.error(this, s"failed to store aborted activation $e")
        }
      //return both futures
      Seq(ack, store)
    }
    Future.sequence(f)
  }

  /**
   * Return any buffered jobs to parent, in case buffer is not empty at removal/error time.
   */
  def rejectBuffered() = {
    //resend any buffered items on container removal
    if (runBuffer.nonEmpty) {
      logging.info(this, s"resending ${runBuffer.size} buffered jobs to parent on container removal")
      runBuffer.foreach(context.parent ! _)
      runBuffer = immutable.Queue.empty[Run]
    }
  }

  private def enableHealthPing(c: Container) = {
    val hpa = healthPingActor.getOrElse {
      logging.info(this, s"creating health ping actor for ${c.addr.asString()}")
      val hp = context.actorOf(
        TCPPingClient
          .props(tcp, c.toString(), healtCheckConfig, new InetSocketAddress(c.addr.host, c.addr.port)))
      healthPingActor = Some(hp)
      hp
    }
    hpa ! HealthPingEnabled(true)
  }

  private def disableHealthPing() = {
    healthPingActor.foreach(_ ! HealthPingEnabled(false))
  }

  /**
   * Runs the job, initialize first if necessary.
   * Completes the job by:
   * 1. sending an activate ack,
   * 2. fetching the logs for the run,
   * 3. indicating the resource is free to the parent pool,
   * 4. recording the result to the data store
   *
   * @param container the container to run the job on
   * @param job       the job to run
   * @return a future completing after logs have been collected and
   *         added to the WhiskActivation
   */
  def initializeAndRun(container: Container, job: Run, reschedule: Boolean = false)(
    implicit tid: TransactionId): Future[WhiskActivation] = {
    val actionTimeout = job.action.limits.timeout.duration
    val unlockedArgs =
      ContainerProxy.unlockArguments(job.msg.content, job.msg.lockedArgs, ParameterEncryption.singleton)

    val (env, parameters) = ContainerProxy.partitionArguments(unlockedArgs, job.msg.initArgs)

    val environment = Map(
      "namespace" -> job.msg.user.namespace.name.toJson,
      "action_name" -> job.msg.action.qualifiedNameWithLeadingSlash.toJson,
      "action_version" -> job.msg.action.version.toJson,
      "activation_id" -> job.msg.activationId.toString.toJson,
      "function_activation_id" -> job.msg.functionActivationId.toJson,
      "app_activation_id" -> job.msg.appActivationId.toJson,
      "transaction_id" -> job.msg.transid.id.toJson,
    )

    // if the action requests the api key to be injected into the action context, add it here;
    // treat a missing annotation as requesting the api key for backward compatibility
    val authEnvironment = {
      if (job.action.annotations.isTruthy(Annotations.ProvideApiKeyAnnotationName, valueForNonExistent = true)) {
        job.msg.user.authkey.toEnvironment.fields
      } else Map.empty
    }

    // Only initialize iff we haven't yet warmed the container
    val initialize = stateData match {
      case data: WarmedData =>
        Future.successful(None)
      case _ =>
        val owEnv = (authEnvironment ++ environment ++ Map(
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)) map {
          case (key, value) => "__OW_" + key.toUpperCase -> value
        }

        container
          .initialize(
            job.action.containerInitializer(env ++ owEnv),
            actionTimeout,
            job.action.limits.concurrency.maxConcurrent,
            Some(job.action.toWhiskAction))
          .map(Some(_))
    }

    val activation: Future[WhiskActivation] = initialize
      .flatMap { initInterval =>
        //immediately setup warmedData for use (before first execution) so that concurrent actions can use it asap
        if (initInterval.isDefined) {
          self ! InitCompleted(WarmedData(container, job.msg.user.namespace.name, Set(job.action), Instant.now, 1))
        }

        val env = authEnvironment ++ environment ++ Map(
          // compute deadline on invoker side avoids discrepancies inside container
          // but potentially under-estimates actual deadline
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

        val serverUrl = "8081"
        // TODO: make this address configurable
        val profileAddress = "http://172.17.0.1:8080/"
        val envMix = LibdAPIs.Action.mix(env)(serverUrl, job.msg.activationId.toString, job.corunningConfig,
          job.msg.profile.map(_ => profileAddress))
        container
          .run(
            parameters,
            envMix.toJson.asJsObject,
            actionTimeout,
            job.action.limits.concurrency.maxConcurrent,
            reschedule)(job.msg.transid)
          .map {
            case (runInterval, response) =>
              prewarmDeadlineCache ! RunFinishedMessage(job.action.fullyQualifiedName(false).asString, runInterval)
              val initRunInterval = initInterval
                .map(i => Interval(runInterval.start.minusMillis(i.duration.toMillis), runInterval.end))
                .getOrElse(runInterval)
              job.msg.appActivationId map { appId =>
                job.action.porusParams.relationships map { rel =>
                  if (rel.dependents.isEmpty) {
                    msgProducer.send("topsched", FinishActivation(appId, response, ActivationLogs(), SemVer(), Parameters()), 8)
                  }
                }
              }
              val activation  = ContainerProxy.constructWhiskActivation(
                job,
                initInterval,
                initRunInterval,
                runInterval.duration >= actionTimeout,
                response)
              resultWaiter.foreach(waiter => {
                if (!activation.name.asString.contains("invokerHealthTestAction")) {
                  waiter ! ResultActivation(activation, job.action)
                }
              })
            activation
          }
      }
      .recoverWith {
        case h: ContainerHealthError =>
          Future.failed(h)
        case InitializationError(interval, response) =>
          Future.successful(
            ContainerProxy
              .constructWhiskActivation(job, Some(interval), interval, interval.duration >= actionTimeout, response))
        case t =>
          // Actually, this should never happen - but we want to make sure to not miss a problem
          logging.error(this, s"caught unexpected error while running activation: ${t}")
          Future.successful(
            ContainerProxy.constructWhiskActivation(
              job,
              None,
              Interval.zero,
              false,
              ActivationResponse.whiskError(Messages.abnormalRun))
          )
      }

    val splitAckMessagesPendingLogCollection = collectLogs.logsToBeCollected(job.action)
    // Sending an active ack is an asynchronous operation. The result is forwarded as soon as
    // possible for blocking activations so that dependent activations can be scheduled. The
    // completion message which frees a load balancer slot is sent after the active ack future
    // completes to ensure proper ordering.
    val sendResult = if (job.msg.blocking) {
      activation.map { result =>
        val msg =
          if (splitAckMessagesPendingLogCollection) ResultMessage(tid, result)
          else CombinedCompletionAndResultMessage(tid, result, instance)
        sendActiveAck(tid, result, job.msg.blocking, job.msg.rootControllerIndex, job.msg.user.namespace.uuid, msg)
      }
    } else {
      // For non-blocking request, do not forward the result.
      if (splitAckMessagesPendingLogCollection) Future.successful(())
      else
        activation.map { result =>
          val msg = CompletionMessage(tid, result, instance)
          sendActiveAck(tid, result, job.msg.blocking, job.msg.rootControllerIndex, job.msg.user.namespace.uuid, msg)
        }
    }

    val context = UserContext(job.msg.user)

    // Adds logs to the raw activation.
    val activationWithLogs: Future[Either[ActivationLogReadingError, WhiskActivation]] = activation
      .flatMap { activation =>
        // Skips log collection entirely, if the limit is set to 0
        if (!splitAckMessagesPendingLogCollection) {
          Future.successful(Right(activation))
        } else {
          val start = tid.started(this, LoggingMarkers.INVOKER_COLLECT_LOGS, logLevel = InfoLevel)
          collectLogs(tid, job.msg.user, activation, container, job.action)
            .andThen {
              case Success(_) => tid.finished(this, start)
              case Failure(t) => tid.failed(this, start, s"reading logs failed: $t")
            }
            .map(logs => Right(activation.withLogs(logs)))
            .recover {
              case LogCollectingException(logs) =>
                Left(ActivationLogReadingError(activation.withLogs(logs)))
              case _ =>
                Left(ActivationLogReadingError(activation.withLogs(ActivationLogs(Vector(Messages.logFailure)))))
            }
        }
      }

    activationWithLogs
      .map(_.fold(_.activation, identity))
      .foreach { activation =>
        // Sending the completion message to the controller after the active ack ensures proper ordering
        // (result is received before the completion message for blocking invokes).
        if (splitAckMessagesPendingLogCollection) {
          sendResult.onComplete(
            _ =>
              sendActiveAck(
                tid,
                activation,
                job.msg.blocking,
                job.msg.rootControllerIndex,
                job.msg.user.namespace.uuid,
                CompletionMessage(tid, activation, instance)))
        }
        storeActivation(tid, activation, job.msg.blocking, context)
      }

    // Disambiguate activation errors and transform the Either into a failed/successful Future respectively.
    activationWithLogs.flatMap {
      case Right(act) if act.response.isSuccess || act.response.isApplicationError =>
        if (act.response.isApplicationError && activationErrorLoggingConfig.applicationErrors) {
          logTruncatedError(act)
        }
        Future.successful(act)
      case Right(act) =>
        if ((act.response.isContainerError && activationErrorLoggingConfig.developerErrors) ||
            (act.response.isWhiskError && activationErrorLoggingConfig.whiskErrors)) {
          logTruncatedError(act)
        }
        Future.failed(ActivationUnsuccessfulError(act))
      case Left(error) => Future.failed(error)
    }
  }
  //to ensure we don't blow up logs with potentially large activation response error
  private def logTruncatedError(act: WhiskActivation) = {
    val truncate = 1024
    val resultString = act.response.result.map(_.compactPrint).getOrElse("[no result]")
    val truncatedResult = if (resultString.length > truncate) {
      s"${resultString.take(truncate)}..."
    } else {
      resultString
    }
    val errorTypeMessage = ActivationResponse.messageForCode(act.response.statusCode)
    logging.warn(
      this,
      s"Activation ${act.activationId} at container ${stateData.getContainer} (with $activeCount still active) returned a $errorTypeMessage: $truncatedResult")
  }
}

final case class ContainerProxyTimeoutConfig(idleContainer: FiniteDuration, pauseGrace: FiniteDuration)
final case class ContainerProxyHealthCheckConfig(enabled: Boolean, checkPeriod: FiniteDuration, maxFails: Int)
final case class ContainerProxyActivationErrorLogConfig(applicationErrors: Boolean,
                                                        developerErrors: Boolean,
                                                        whiskErrors: Boolean)

object ContainerProxy {
  def props(factory: (TransactionId,
                      String,
                      ImageName,
                      Boolean,
                      ByteSize,
                      Int,
                      Option[ExecutableWhiskAction]) => Future[Container],
            ack: ActiveAck,
            store: (TransactionId, WhiskActivation, Boolean, UserContext) => Future[Any],
            collectLogs: LogsCollector,
            instance: InvokerInstanceId,
            poolConfig: ContainerPoolConfig,
            healthCheckConfig: ContainerProxyHealthCheckConfig =
              loadConfigOrThrow[ContainerProxyHealthCheckConfig](ConfigKeys.containerProxyHealth),
            activationErrorLogConfig: ContainerProxyActivationErrorLogConfig = activationErrorLogging,
            unusedTimeout: FiniteDuration = timeouts.idleContainer,
            pauseGrace: FiniteDuration = timeouts.pauseGrace,
            msgProducer: MessageProducer,
            prewarmDeadlineCache: ActorRef = PrewarmDeadlineCacheActor(PrewarmDeadlineCache()).self,
            tcp: Option[ActorRef] = None,
            resultWaiter: Option[ActorRef] = None,
            addressBook: Option[ActorProxyAddressBook] = None,
           ) =
    Props(
      new ContainerProxy(
        factory,
        ack,
        store,
        collectLogs,
        instance,
        poolConfig,
        healthCheckConfig,
        activationErrorLogConfig,
        unusedTimeout,
        pauseGrace,
        msgProducer,
        prewarmDeadlineCache,
        tcp,
        resultWaiter,
        addressBook,
      ))

  // Needs to be thread-safe as it's used by multiple proxies concurrently.
  private val containerCount = new Counter

  val timeouts = loadConfigOrThrow[ContainerProxyTimeoutConfig](ConfigKeys.containerProxyTimeouts)
  val activationErrorLogging =
    loadConfigOrThrow[ContainerProxyActivationErrorLogConfig](ConfigKeys.containerProxyActivationErrorLogs)

  /**
   * Generates a unique container name.
   *
   * @param prefix the container name's prefix
   * @param suffix the container name's suffix
   * @return a unique container name
   */
  def containerName(instance: InvokerInstanceId, prefix: String, suffix: String): String = {
    def isAllowed(c: Char): Boolean = c.isLetterOrDigit || c == '_'

    val sanitizedPrefix = prefix.filter(isAllowed)
    val sanitizedSuffix = suffix.filter(isAllowed)

    s"${ContainerFactory.containerNamePrefix(instance)}_${containerCount.next()}_${sanitizedPrefix}_${sanitizedSuffix}"
  }

  /**
   * Creates a WhiskActivation ready to be sent via active ack.
   *
   * @param job the job that was executed
   * @param totalInterval the time it took to execute the job
   * @param response the response to return to the user
   * @return a WhiskActivation to be sent to the user
   */
  def constructWhiskActivation(job: Run,
                               initInterval: Option[Interval],
                               totalInterval: Interval,
                               isTimeout: Boolean,
                               response: ActivationResponse) = {
    val causedBy = if (job.msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    val waitTime = {
      val end = initInterval.map(_.start).getOrElse(totalInterval.start)
      Parameters(WhiskActivation.waitTimeAnnotation, Interval(job.msg.transid.meta.start, end).duration.toMillis.toJson)
    }

    val initTime = {
      initInterval.map(initTime => Parameters(WhiskActivation.initTimeAnnotation, initTime.duration.toMillis.toJson))
    }

    val binding =
      job.msg.action.binding.map(f => Parameters(WhiskActivation.bindingAnnotation, JsString(f.asString)))

    WhiskActivation(
      activationId = job.msg.activationId,
      namespace = job.msg.user.namespace.name.toPath,
      subject = job.msg.user.subject,
      cause = job.msg.cause,
      name = job.action.name,
      version = job.action.version,
      start = totalInterval.start,
      end = totalInterval.end,
      duration = Some(totalInterval.duration.toMillis),
      response = response,
      annotations = {
        Parameters(WhiskActivation.limitsAnnotation, job.action.limits.toJson) ++
          Parameters(WhiskActivation.pathAnnotation, JsString(job.action.fullyQualifiedName(false).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(job.action.exec.kind)) ++
          Parameters(WhiskActivation.timeoutAnnotation, JsBoolean(isTimeout)) ++
          causedBy ++ initTime ++ waitTime ++ binding
      })
  }

  /**
   * Partitions the activation arguments into two JsObject instances. The first is exported as intended for export
   * by the action runtime to the environment. The second is passed on as arguments to the action.
   *
   * @param content the activation arguments
   * @param initArgs set of parameters to treat as initialization arguments
   * @return A partition of the arguments into an environment variables map and the JsObject argument to the action
   */
  def partitionArguments(content: Option[JsObject], initArgs: Set[String]): (Map[String, JsValue], JsObject) = {
    content match {
      case None                         => (Map.empty, JsObject.empty)
      case Some(js) if initArgs.isEmpty => (Map.empty, js)
      case Some(js) =>
        val (env, args) = js.fields.partition(k => initArgs.contains(k._1))
        (env, JsObject(args))
    }
  }

  def unlockArguments(content: Option[JsObject],
                      lockedArgs: Map[String, String],
                      decoder: ParameterEncryption): Option[JsObject] = {
    content.map {
      case JsObject(fields) =>
        JsObject(fields.map {
          case (k, v: JsString) if lockedArgs.contains(k) => (k -> decoder.encryptor(lockedArgs(k)).decrypt(v))
          case p                                          => p
        })
    }
  }
}

object TCPPingClient {
  def props(tcp: ActorRef, containerId: String, config: ContainerProxyHealthCheckConfig, remote: InetSocketAddress) =
    Props(new TCPPingClient(tcp, containerId, remote, config))
}

class TCPPingClient(tcp: ActorRef,
                    containerId: String,
                    remote: InetSocketAddress,
                    config: ContainerProxyHealthCheckConfig)
    extends Actor {
  implicit val logging = new AkkaLogging(context.system.log)
  implicit val ec = context.system.dispatcher
  implicit var healthPingTx = TransactionId.actionHealthPing
  case object HealthPingSend

  var scheduledPing: Option[Cancellable] = None
  var failedCount = 0
  val addressString = s"${remote.getHostString}:${remote.getPort}"
  restartPing()

  private def restartPing() = {
    cancelPing() //just in case restart is called twice
    scheduledPing = Some(
      context.system.scheduler.schedule(config.checkPeriod, config.checkPeriod, self, HealthPingSend))
  }
  private def cancelPing() = {
    scheduledPing.foreach(_.cancel())
  }
  def receive = {
    case HealthPingEnabled(enabled) =>
      if (enabled) {
        restartPing()
      } else {
        cancelPing()
      }
    case HealthPingSend =>
      healthPingTx = TransactionId(systemPrefix + "actionHealth") //reset the tx id each iteration
      tcp ! Connect(remote)
    case CommandFailed(_: Connect) =>
      failedCount += 1
      if (failedCount == config.maxFails) {
        logging.error(
          this,
          s"Failed health connection to $containerId ($addressString) $failedCount times - exceeded max ${config.maxFails} failures")
        //destroy this container since we cannot communicate with it
        context.parent ! FailureMessage(
          new SocketException(s"Health connection to $containerId ($addressString) failed $failedCount times"))
        cancelPing()
        context.stop(self)
      } else {
        logging.warn(this, s"Failed health connection to $containerId ($addressString) $failedCount times")
      }

    case Connected(_, _) =>
      sender() ! Close
      if (failedCount > 0) {
        //reset in case of temp failure
        logging.info(
          this,
          s"Succeeded health connection to $containerId ($addressString) after $failedCount previous failures")
        failedCount = 0
      } else {
        logging.debug(this, s"Succeeded health connection to $containerId ($addressString)")
      }

  }
}

/** Indicates that something went wrong with an activation and the container should be removed */
trait ActivationError extends Exception {
  val activation: WhiskActivation
}

/** Indicates an activation with a non-successful response */
case class ActivationUnsuccessfulError(activation: WhiskActivation) extends ActivationError

/** Indicates reading logs for an activation failed (terminally, truncated) */
case class ActivationLogReadingError(activation: WhiskActivation) extends ActivationError
