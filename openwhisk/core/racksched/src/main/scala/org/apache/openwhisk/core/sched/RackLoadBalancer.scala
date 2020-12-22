package org.apache.openwhisk.core.sched

import akka.actor.Actor

import java.util.concurrent.atomic.LongAdder
import akka.actor.ActorRef
import akka.actor.{ActorRefFactory, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.ReachabilityEvent
import akka.cluster.ClusterEvent.ReachableMember
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.event.Logging.InfoLevel
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common.LoggingMarkers.{ForcedAfterRegularCompletionAck, ForcedCompletionAck, HealthcheckCompletionAck, RegularAfterForcedCompletionAck, RegularCompletionAck}
import org.apache.openwhisk.common.{LogMarkerToken, Logging, LoggingMarkers, MetricEmitter, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig.kafkaHosts
import org.apache.openwhisk.core.WhiskConfig.wskApiHost
import org.apache.openwhisk.core.connector.AcknowledegmentMessage
import org.apache.openwhisk.core.connector.{ActivationMessage, MessageFeed, MessageProducer, MessagingProvider}
import org.apache.openwhisk.core.containerpool.ContainerPoolConfig
import org.apache.openwhisk.core.entity.ControllerInstanceId
import org.apache.openwhisk.core.entity.MemoryLimit
import org.apache.openwhisk.core.entity.WhiskActionMetaData
import org.apache.openwhisk.core.entity.WhiskEntityStore
import org.apache.openwhisk.core.entity.size.SizeInt
import org.apache.openwhisk.core.entity.{ActivationEntityLimit, ActivationId, ExecManifest, ExecutableWhiskActionMetaData, InstanceId, InvokerInstanceId, RackSchedInstanceId, TimeLimit, UUID, WhiskActivation}
import org.apache.openwhisk.core.loadBalancer.{ActivationEntry, FeedFactory, InvocationFinishedMessage, InvocationFinishedResult, InvokerHealth, ShardingContainerPoolBalancerConfig}
import org.apache.openwhisk.spi.Spi
import org.apache.openwhisk.spi.SpiLoader
import pureconfig.loadConfigOrThrow
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.entity.types.EntityStore
import org.apache.openwhisk.core.loadBalancer.ClusterConfig
import org.apache.openwhisk.core.loadBalancer.CurrentInvokerPoolState
import org.apache.openwhisk.core.loadBalancer.InvokerActor
import org.apache.openwhisk.core.loadBalancer.InvokerPool
import org.apache.openwhisk.core.loadBalancer.InvokerPoolFactory
import org.apache.openwhisk.core.loadBalancer.InvokerState
import org.apache.openwhisk.core.loadBalancer.LoadBalancerException
import org.apache.openwhisk.core.loadBalancer.ShardingContainerPoolBalancer
import org.apache.openwhisk.core.loadBalancer.ShardingContainerPoolBalancerState

import java.nio.charset.StandardCharsets
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

trait RackLoadBalancer {
  /**
   * Publishes activation message on internal bus for an invoker to pick up.
   *
   * @param action the action to invoke
   * @param msg the activation message to publish on an invoker topic
   * @param transid the transaction id for the request
   * @return result a nested Future the outer indicating completion of publishing and
   *         the inner the completion of the action (i.e., the result)
   *         if it is ready before timeout (Right) otherwise the activation id (Left).
   *         The future is guaranteed to complete within the declared action time limit
   *         plus a grace period (see activeAckTimeoutGrace).
   */
  def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]]

  /**
   * Returns a message indicating the health of the containers and/or container pool in general.
   *
   * @return a Future[IndexedSeq[InvokerHealth]] representing the health of the pools managed by the loadbalancer.
   */
  def invokerHealth(): Future[IndexedSeq[InvokerHealth]]

  /** Gets the number of in-flight activations for a specific user. */
  def activeActivationsFor(namespace: UUID): Future[Int]

  /** Gets the number of in-flight activations in the system. */
  def totalActiveActivations: Future[Int]

  /** Gets the size of the cluster all loadbalancers are acting in */
  def clusterSize: Int = 1
}

/**
 * An Spi for providing load balancer implementations.
 */
trait RackLoadBalancerProvider extends Spi {
  def requiredProperties: Map[String, String]

  def instance(whiskConfig: WhiskConfig, instance: RackSchedInstanceId)(implicit actorSystem: ActorSystem,
                                                                        logging: Logging,
                                                                        materializer: ActorMaterializer): RackLoadBalancer

  /** Return default FeedFactory */
  def createFeedFactory(whiskConfig: WhiskConfig, instance: RackSchedInstanceId)(implicit actorSystem: ActorSystem,
                                                                                logging: Logging): FeedFactory = {

    val activeAckTopic = s"completed${instance.toString}"
    val maxActiveAcksPerPoll = 128
    val activeAckPollDuration = 1.second

    new FeedFactory {
      def createFeed(f: ActorRefFactory, provider: MessagingProvider, acker: Array[Byte] => Future[Unit]) = {
        f.actorOf(Props {
          new MessageFeed(
            "activeack",
            logging,
            provider.getConsumer(whiskConfig, activeAckTopic, activeAckTopic, maxPeek = maxActiveAcksPerPoll),
            maxActiveAcksPerPoll,
            activeAckPollDuration,
            acker)
        })
      }
    }
  }
}

/**
 * Lean loadbalancer implemetation.
 *
 * Communicates with Invoker directly without Kafka in the middle. Invoker does not exist as a separate entity, it is built together with Controller
 * Uses LeanMessagingProvider to use in-memory queue instead of Kafka
 */
class RackSimpleBalancer(config: WhiskConfig,
                         feedFactory: FeedFactory,
                         rackschedInstance: RackSchedInstanceId,
                         val invokerPoolFactory: InvokerPoolFactory,
                         implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
                    implicit actorSystem: ActorSystem,
                    logging: Logging,
                    materializer: ActorMaterializer) extends RackLoadBalancer {
  protected implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  private val entityStore: EntityStore = WhiskEntityStore.datastore()

  protected val messageProducer: MessageProducer =
    messagingProvider.getProducer(config, Some(ActivationEntityLimit.MAX_ACTIVATION_LIMIT))

  /** Build a cluster of all loadbalancers */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  /** State related to invocations and throttling */
  protected[sched] val activationSlots: TrieMap[ActivationId, ActivationEntry] = TrieMap[ActivationId, ActivationEntry]()
  protected[sched] val activationPromises: TrieMap[ActivationId, Promise[Either[ActivationId, WhiskActivation]]] =
    TrieMap[ActivationId, Promise[Either[ActivationId, WhiskActivation]]]()
  protected val activationsPerNamespace: TrieMap[UUID, LongAdder] = TrieMap[UUID, LongAdder]()
  protected val totalActivations = new LongAdder()
  protected val totalBlackBoxActivationMemory = new LongAdder()
  protected val totalManagedActivationMemory = new LongAdder()

  val lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

  val poolConfig: ContainerPoolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool)

  /** Loadbalancer interface methods */
  def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(IndexedSeq.empty[InvokerHealth])
  override def clusterSize: Int = 1

  val schedulingState: ShardingContainerPoolBalancerState = ShardingContainerPoolBalancerState()(lbConfig)

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee that
   * [[ShardingContainerPoolBalancerState.updateInvokers]] and [[ShardingContainerPoolBalancerState.updateCluster]]
   * are called exclusive of each other and not concurrently.
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }
        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  val maxPeek = 128
  private val scheduleConsumer = messagingProvider.getConsumer(config, rackschedInstance.toString,
      rackschedInstance.toString, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)

  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed("racktivation", logging, scheduleConsumer, maxPeek, 1.second, processSchedulingMessage)
  })

  /** 4. Get the ack message and parse it */
  protected[RackSimpleBalancer] def processSchedulingMessage(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    ActivationMessage.parse(new String(bytes, StandardCharsets.UTF_8)) match {
      case Success(activation) =>
        implicit val transid: TransactionId = activation.transid
        WhiskActionMetaData.resolveActionAndMergeParameters(entityStore, activation.action)
          .foreach(metadata => {
            publish(metadata.toExecutableWhiskAction.get, activation)
          })
        activationFeed ! MessageFeed.Processed

      case Failure(t) =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"failed processing top level scheduler message: $raw")

      case _ =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"Unexpected Acknowledgment message received by loadbalancer: $raw")
    }
  }

  val invokerPool: ActorRef =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  val invokerName: InvokerInstanceId = InvokerInstanceId(0, None, None, poolConfig.userMemory)

  /** 1. Publish a message to the rackLoadBalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
    val (invokersToUse, stepSizes) =
      if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
      else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)
    val chosen = if (invokersToUse.nonEmpty) {
      val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
      val homeInvoker = hash % invokersToUse.size
      val stepSize = stepSizes(hash % stepSizes.size)
      val invoker: Option[(InvokerInstanceId, Boolean)] = ShardingContainerPoolBalancer.schedule(
        action.limits.concurrency.maxConcurrent,
        action.fullyQualifiedName(true),
        invokersToUse,
        schedulingState.invokerSlots,
        action.limits.memory.megabytes,
        homeInvoker,
        stepSize)
      invoker.foreach {
        case (_, true) =>
          val metric =
            if (isBlackboxInvocation)
              LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
            else
              LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
          MetricEmitter.emitCounterMetric(metric)
        case _ =>
      }
      invoker.map(_._1)
    } else {
      None
    }

    chosen
      .map { invoker =>
        // MemoryLimit() and TimeLimit() return singletons - they should be fast enough to be used here
        val memoryLimit = action.limits.memory
        val memoryLimitInfo = if (memoryLimit == MemoryLimit()) { "std" } else { "non-std" }
        val timeLimit = action.limits.timeout
        val timeLimitInfo = if (timeLimit == TimeLimit()) { "std" } else { "non-std" }
        logging.info(
          this,
          s"scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${invoker}")
        val activationResult = setupActivation(msg, action, invoker)
        sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)
      }
      .getOrElse {
        // report the state of all invokers
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }

        logging.error(
          this,
          s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
        Future.failed(LoadBalancerException("No invokers available"))
      }
  }

  /**
   * Calculate the duration within which a completion ack must be received for an activation.
   *
   * Calculation is based on the passed action time limit. If the passed action time limit is shorter than
   * the configured standard action time limit, the latter is used to avoid too tight timeouts.
   *
   * The base timeout is multiplied with a configurable timeout factor. This dilution controls how much slack you
   * want to allow in your system before you start reporting failed activations. The default value of 2 bases
   * on invoker behavior that a cold invocation's init duration may be as long as its run duration. Higher factors
   * may account for additional wait times.
   *
   * Finally, a configurable duration is added to the diluted timeout to be lenient towards general delays / wait times.
   *
   * @param actionTimeLimit the action's time limit
   * @return the calculated time duration within which a completion ack must be received
   */
  private def calculateCompletionAckTimeout(actionTimeLimit: FiniteDuration): FiniteDuration = {
    (actionTimeLimit.max(TimeLimit.STD_DURATION) * lbConfig.timeoutFactor) + lbConfig.timeoutAddon
  }

  /**
   * 2. Update local state with the activation to be executed scheduled.
   *
   * All activations are tracked in the activationSlots map. Additionally, blocking invokes
   * are tracked in the activationPromises map. When a result is received via result ack, it
   * will cause the result to be forwarded to the caller waiting on the result, and cancel
   * the DB poll which is also trying to do the same.
   * Once the completion ack arrives, activationSlots entry will be removed.
   */
  protected def setupActivation(msg: ActivationMessage,
                                action: ExecutableWhiskActionMetaData,
                                instance: InvokerInstanceId): Future[Either[ActivationId, WhiskActivation]] = {

    // Needed for emitting metrics.
    totalActivations.increment()
    val isBlackboxInvocation = action.exec.pull
    val totalActivationMemory =
      if (isBlackboxInvocation) totalBlackBoxActivationMemory else totalManagedActivationMemory
    totalActivationMemory.add(action.limits.memory.megabytes)

    activationsPerNamespace.getOrElseUpdate(msg.user.namespace.uuid, new LongAdder()).increment()

    // Completion Ack must be received within the calculated time.
    val completionAckTimeout = calculateCompletionAckTimeout(action.limits.timeout.duration)

    // If activation is blocking, store a promise that we can mark successful later on once the result ack
    // arrives. Return a Future representing the promise to caller.
    // If activation is non-blocking, return a successfully completed Future to caller.
    val resultPromise = if (msg.blocking) {
      activationPromises.getOrElseUpdate(msg.activationId, Promise[Either[ActivationId, WhiskActivation]]()).future
    } else Future.successful(Left(msg.activationId))

    // Install a timeout handler for the catastrophic case where a completion ack is not received at all
    // (because say an invoker is down completely, or the connection to the message bus is disrupted) or when
    // the completion ack is significantly delayed (possibly dues to long queues but the subject should not be penalized);
    // in this case, if the activation handler is still registered, remove it and update the books.
    //
    // Attention: a significantly delayed completion ack means that the invoker is still busy or will be busy in future
    // with running the action. So the current strategy of freeing up the activation's memory in invoker
    // book-keeping will allow the load balancer to send more activations to the invoker. This can lead to
    // invoker overloads so that activations need to wait until other activations complete.
    activationSlots.getOrElseUpdate(
      msg.activationId, {
        val timeoutHandler = actorSystem.scheduler.scheduleOnce(completionAckTimeout) {
          processCompletion(msg.activationId, msg.transid, forced = true, isSystemError = false, instance = instance)
        }

        // please note: timeoutHandler.cancel must be called on all non-timeout paths, e.g. Success
        ActivationEntry(
          msg.activationId,
          msg.user.namespace.uuid,
          instance,
          action.limits.memory.megabytes.MB,
          action.limits.timeout.duration,
          action.limits.concurrency.maxConcurrent,
          action.fullyQualifiedName(true),
          timeoutHandler,
          isBlackboxInvocation,
          msg.blocking)
      })

    resultPromise
  }

  // Singletons for counter metrics related to completion acks
  protected val RACKSCHED_COMPLETION_ACK_REGULAR: LogMarkerToken =
    LoggingMarkers.RACKSCHED_BALANCER_COMPLETION_ACK(rackschedInstance, RegularCompletionAck)
  protected val RACKSCHED_COMPLETION_ACK_FORCED: LogMarkerToken =
    LoggingMarkers.RACKSCHED_BALANCER_COMPLETION_ACK(rackschedInstance, ForcedCompletionAck)
  protected val RACKSCHED_COMPLETION_ACK_HEALTHCHECK: LogMarkerToken =
    LoggingMarkers.RACKSCHED_BALANCER_COMPLETION_ACK(rackschedInstance, HealthcheckCompletionAck)
  protected val RACKSCHED_COMPLETION_ACK_REGULAR_AFTER_FORCED: LogMarkerToken =
    LoggingMarkers.RACKSCHED_BALANCER_COMPLETION_ACK(rackschedInstance, RegularAfterForcedCompletionAck)
  protected val RACKSCHED_COMPLETION_ACK_FORCED_AFTER_REGULAR: LogMarkerToken =
    LoggingMarkers.RACKSCHED_BALANCER_COMPLETION_ACK(rackschedInstance, ForcedAfterRegularCompletionAck)

  /** Subscribes to ack messages from the invokers (result / completion) and registers a handler for these messages. */
  private val acknowledgementFeed: ActorRef =
    feedFactory.createFeed(actorSystem, messagingProvider, processAcknowledgement)

  /** 4. Get the ack message and parse it */
  protected[sched] def processAcknowledgement(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    AcknowledegmentMessage.parse(raw) match {
      case Success(acknowledegment) =>
        acknowledegment.isSlotFree.foreach { instance =>
          processCompletion(
            acknowledegment.activationId,
            acknowledegment.transid,
            forced = false,
            isSystemError = acknowledegment.isSystemError.getOrElse(false),
            instance)
        }

        acknowledegment.result.foreach { response =>
          processResult(acknowledegment.activationId, acknowledegment.transid, response)
        }

        acknowledgementFeed ! MessageFeed.Processed

      case Failure(t) =>
        acknowledgementFeed ! MessageFeed.Processed
        logging.error(this, s"failed processing message: $raw")

      case _ =>
        acknowledgementFeed ! MessageFeed.Processed
        logging.error(this, s"Unexpected Acknowledgment message received by loadbalancer: $raw")
    }
  }

  /** 5. Process the result ack and return it to the user */
  protected def processResult(aid: ActivationId,
                              tid: TransactionId,
                              response: Either[ActivationId, WhiskActivation]): Unit = {
    // Resolve the promise to send the result back to the user.
    // The activation will be removed from the activation slots later, when the completion message
    // is received (because the slot in the invoker is not yet free for new activations).
    activationPromises.remove(aid).foreach(_.trySuccess(response))
    logging.info(this, s"received result ack for '$aid'")(tid)
  }



  /** 6. Process the completion ack and update the state */
  protected[sched] def processCompletion(aid: ActivationId,
                                                tid: TransactionId,
                                                forced: Boolean,
                                                isSystemError: Boolean,
                                                instance: InstanceId): Unit = {

    val invoker = instance match {
      case i: InvokerInstanceId => Some(i)
      case _                    => None
    }

    val invocationResult = if (forced) {
      InvocationFinishedResult.Timeout
    } else {
      // If the response contains a system error, report that, otherwise report Success
      // Left generally is considered a Success, since that could be a message not fitting into Kafka
      if (isSystemError) {
        InvocationFinishedResult.SystemError
      } else {
        InvocationFinishedResult.Success
      }
    }

    activationSlots.remove(aid) match {
      case Some(entry) =>
        totalActivations.decrement()
        val totalActivationMemory =
          if (entry.isBlackbox) totalBlackBoxActivationMemory else totalManagedActivationMemory
        totalActivationMemory.add(entry.memoryLimit.toMB * (-1))
        activationsPerNamespace.get(entry.namespaceId).foreach(_.decrement())

        invoker.foreach(releaseInvoker(_, entry))

        if (!forced) {
          entry.timeoutHandler.cancel()
          // notice here that the activationPromises is not touched, because the expectation is that
          // the active ack is received as expected, and processing that message removed the promise
          // from the corresponding map
          logging.info(this, s"received completion ack for '$aid', system error=$isSystemError")(tid)

          MetricEmitter.emitCounterMetric(RACKSCHED_COMPLETION_ACK_REGULAR)

        } else {
          // the entry has timed out; if the active ack is still around, remove its entry also
          // and complete the promise with a failure if necessary
          activationPromises
            .remove(aid)
            .foreach(_.tryFailure(new Throwable("no completion or active ack received yet")))
          val actionType = if (entry.isBlackbox) "blackbox" else "managed"
          val blockingType = if (entry.isBlocking) "blocking" else "non-blocking"
          val completionAckTimeout = calculateCompletionAckTimeout(entry.timeLimit)
          logging.warn(
            this,
            s"forced completion ack for '$aid', action '${entry.fullyQualifiedEntityName}' ($actionType), $blockingType, mem limit ${entry.memoryLimit.toMB} MB, time limit ${entry.timeLimit.toMillis} ms, completion ack timeout $completionAckTimeout from $instance")(
            tid)

          MetricEmitter.emitCounterMetric(RACKSCHED_COMPLETION_ACK_FORCED)
        }

        // Completion acks that are received here are strictly from user actions - health actions are not part of
        // the load balancer's activation map. Inform the invoker pool supervisor of the user action completion.
        // guard this
        invoker.foreach(invokerPool ! InvocationFinishedMessage(_, invocationResult))
      case None if tid == TransactionId.invokerHealth =>
        // Health actions do not have an ActivationEntry as they are written on the message bus directly. Their result
        // is important to pass to the invokerPool because they are used to determine if the invoker can be considered
        // healthy again.
        logging.info(this, s"received completion ack for health action on $instance")(tid)

        MetricEmitter.emitCounterMetric(RACKSCHED_COMPLETION_ACK_HEALTHCHECK)

        // guard this
        invoker.foreach(invokerPool ! InvocationFinishedMessage(_, invocationResult))
      case None if !forced =>
        // Received a completion ack that has already been taken out of the state because of a timeout (forced ack).
        // The result is ignored because a timeout has already been reported to the invokerPool per the force.
        // Logging this condition as a warning because the invoker processed the activation and sent a completion
        // message - but not in time.
        logging.warn(
          this,
          s"received completion ack for '$aid' from $instance which has no entry, system error=$isSystemError")(tid)

        MetricEmitter.emitCounterMetric(RACKSCHED_COMPLETION_ACK_REGULAR_AFTER_FORCED)
      case None =>
        // The entry has already been removed by a completion ack. This part of the code is reached by the timeout and can
        // happen if completion ack and timeout happen roughly at the same time (the timeout was triggered before the completion
        // ack canceled the timer). As the completion ack is already processed we don't have to do anything here.
        logging.debug(this, s"forced completion ack for '$aid' which has no entry")(tid)

        MetricEmitter.emitCounterMetric(RACKSCHED_COMPLETION_ACK_FORCED_AFTER_REGULAR)
    }
  }

  /** 3. Send the activation to the invoker */
  protected def sendActivationToInvoker(producer: MessageProducer,
                                        msg: ActivationMessage,
                                        invoker: InvokerInstanceId): Future[RecordMetadata] = {
    implicit val transid: TransactionId = msg.transid

    val topic = s"invoker${invoker.toInt}"

    MetricEmitter.emitCounterMetric(LoggingMarkers.LOADBALANCER_ACTIVATION_START)
    val start = transid.started(
      this,
      LoggingMarkers.CONTROLLER_KAFKA,
      s"posting topic '$topic' with activation id '${msg.activationId}'",
      logLevel = InfoLevel)

    producer.send(topic, msg).andThen {
      case Success(status) =>
        transid.finished(
          this,
          start,
          s"posted to ${status.topic()}[${status.partition()}][${status.offset()}]",
          logLevel = InfoLevel)
      case Failure(_) => transid.failed(this, start, s"error on posting to topic $topic")
    }
  }

  protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
    // Currently do nothing
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memoryLimit.toMB.toInt))
  }

  /** Gets the number of in-flight activations for a specific user. */
  def activeActivationsFor(namespace: UUID): Future[Int] = Future.successful(activationsPerNamespace.get(namespace).map(_.intValue).getOrElse(0))

  /** Gets the number of in-flight activations in the system. */
  def totalActiveActivations: Future[Int] = Future.successful(totalActivations.intValue)
}

object RackLoadBalancer extends RackLoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: RackSchedInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): RackSimpleBalancer = {

    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(actorRefFactory: ActorRefFactory,
                                     messagingProvider: MessagingProvider,
                                     messagingProducer: MessageProducer,
                                     sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
                                     monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(new ControllerInstanceId(instance.toString), WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, new ControllerInstanceId(instance.toString))),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"health${instance.toString}", "health", maxPeek = 128),
            monitor))
      }
    }

    new RackSimpleBalancer(whiskConfig, createFeedFactory(whiskConfig, instance), instance, invokerPoolFactory)
  }

  def requiredProperties =
    ExecManifest.requiredProperties ++
      wskApiHost ++
      kafkaHosts
}
