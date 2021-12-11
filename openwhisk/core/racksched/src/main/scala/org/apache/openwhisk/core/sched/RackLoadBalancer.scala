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
import org.apache.openwhisk.core.connector.{AcknowledegmentMessage, ActivationMessage, MessageFeed, MessageProducer, MessagingProvider}
import org.apache.openwhisk.core.containerpool.{InvokerPoolResourceType, RuntimeResources}
import org.apache.openwhisk.core.entity.{ActivationEntityLimit, ActivationId, ExecManifest, ExecutableWhiskActionMetaData, InstanceId, InvokerInstanceId, RackSchedInstanceId, TimeLimit, UUID, WhiskActionMetaData, WhiskActivation, WhiskEntityStore}
import org.apache.openwhisk.core.loadBalancer.{ActivationEntry, FeedFactory, InvocationFinishedMessage, InvocationFinishedResult, InvokerHealth, ShardingContainerPoolBalancerConfig}
import org.apache.openwhisk.spi.Spi
import org.apache.openwhisk.spi.SpiLoader
import pureconfig.loadConfigOrThrow
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.core.entity.types.EntityStore
import org.apache.openwhisk.core.invoker.SchedulingDecision
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
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{DurationInt, FiniteDuration, MINUTES}
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

    val activeAckTopic = s"completed${instance.asString}"
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

  // map which based on a tuple-key stores the previously-scheduled location and two other values (id, int0, int1) where
  // int0 represents the total scheduled messages based on this value
  // int1 represent the max number of messages scheduled using this map
  // once int0 == int1, then the key should be removed from the map.
  // in the case of an error, the scheduling message will be removed after ~5 minutes if the number of expected scheduled
  // messages does not come through.
  // the key of the map is (app activation ID, function activation ID, scheduled function name)
  val preschedLock = new ReentrantLock()
  case class PreSchedKey(appId: ActivationId, funcId: ActivationId, msgFQEN: String)
  val preScheduled: mutable.Map[PreSchedKey, (InvokerInstanceId, ActivationId, Int, Int)] = mutable.Map()

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
  protected val totalBlackBoxActivationMemory = new AtomicReference[RuntimeResources](RuntimeResources.none())
  protected val totalManagedActivationMemory = new AtomicReference[RuntimeResources](RuntimeResources.none())

  val lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

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
  private val scheduleConsumer = messagingProvider.getConsumer(config, rackschedInstance.schedTopic,
      rackschedInstance.schedTopic, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)

  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed("racktivation", logging, scheduleConsumer, maxPeek, 1.second, parseActivationMessage)
  })

  /** 4. Get the ack message and parse it */
  protected[RackSimpleBalancer] def parseActivationMessage(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    Future.fromTry(ActivationMessage.parse(raw))
      .andThen {
        case _ => activationFeed ! MessageFeed.Processed
      }.map(processSchedulingMessage).recover {
      case t => logging.error(this, s"failed processing top level scheduler message: $raw")
    }
  }

  protected[RackSimpleBalancer] def processSchedulingMessage(activation: ActivationMessage): Unit = {
    implicit val transid: TransactionId = activation.transid
    val fin = transid.started(this, LoggingMarkers.RACKSCHED_SCHED_BEGIN)
    WhiskActionMetaData.resolveActionAndMergeParameters(entityStore, activation.action, activation.appActivationId) onComplete {
      case Success(metadata) =>
        publish(metadata.toExecutableWhiskAction.get, activation)
          .onComplete(_ => transid.finished(this, fin, "finished scheduling"))
      case Failure(exception) =>
        logging.error(this, s"Failed to publish rack activation: $exception")
    }
  }

  val invokerPool: ActorRef =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  /** 1. Publish a message to the rackLoadBalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
    val (invokersToUse, stepSizes) =
      if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
      else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)
    val chosen: Option[(InvokerInstanceId, Option[ActivationId])] = if (invokersToUse.nonEmpty) {
      val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
      val homeInvoker = hash % invokersToUse.size
      val stepSize = stepSizes(hash % stepSizes.size)
      val defaultSchedule = () => ShardingContainerPoolBalancer.schedule(
          action.limits.concurrency.maxConcurrent,
          action.fullyQualifiedName(true),
          invokersToUse,
          schedulingState.invokerSlots,
          action.limits.resources.limits,
        homeInvoker,
        stepSize,
        iprt = InvokerPoolResourceType.poolFor(action),
        swappingInvoker = msg.swapFrom.map(_.source)) flatMap { v =>
        // Add additional condition that if we already have a re-route and it's our rack, that we attempt to
        // schedule it anyways and make the assumption the top balancer could not find another rack for it.
        if (v._2 && msg.rerouteFromRack.forall(f => f.toInt != rackschedInstance.toInt)) {
          None
        } else {
          Some(v._1, None, v._2)
        }
      }

      val invoker: Option[(InvokerInstanceId, Option[ActivationId], Boolean)] = msg.waitForContent match {
        case Some(content) =>
          if (content > 1 && msg.prewarmOnly.isEmpty) {
            msg.appActivationId flatMap { appId =>
              msg.functionActivationId flatMap { funcId =>
                val key = PreSchedKey(appId, funcId, msg.action.qualifiedNameWithLeadingSlash)
                try {
                  preschedLock.lock()
                  preScheduled.get(key) match {
                    case Some(value) =>
                      // this message came through the scheduler before..return the same result
                      // logging.debug(this, s"found presched ${msg.activationId}: $key")
                      if (value._3 + 1 >= value._4) {
                        // we've reached the max number of messages to schedule..let's get rid of it
                        preScheduled.remove(key)
                      } else {
                        preScheduled.put(key, (value._1, value._2, value._3 + 1, value._4))
                      }
                      Some((value._1, Some(value._2), false))
                    case None =>
                      // we have not seen the message yet..schedule the result, then store the scheduling message
                      // and set a timer to remove it
                      defaultSchedule() match {
                        case Some((value: InvokerInstanceId, _, check)) =>
                          val v = (value, msg.activationId, 1, content)
                          preScheduled.put(key, v)
                          logging.debug(this, s"putting presched ${msg.activationId}: $key #### $v")
                          actorSystem.getScheduler.scheduleOnce(FiniteDuration(5, MINUTES))(() => preScheduled.remove(key))
                          // set NONE for previous activation ID is important in order to actually send the scheduling message
                          // to the invoker.
                          Some((value, None, check))
                        case _ => None
                      }
                  }
                } finally {
                  preschedLock.unlock()
                }

              }
            }
          } else {
            defaultSchedule()
          }
        case None => defaultSchedule()
      }
      invoker.foreach {
        case (_, _, true) =>
          val metric =
            if (isBlackboxInvocation)
              LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
            else
              LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
          MetricEmitter.emitCounterMetric(metric)
        case _ =>
      }
      invoker.map { v => (v._1, v._2) }
    } else {
      None
    }
    chosen match {
      case Some((invoker, aid)) =>
        // MemoryLimit() and TimeLimit() return singletons - they should be fast enough to be used here
        val resourceLimit = action.limits.resources
        val timeLimit = action.limits.timeout
        logging.info(
          this,
          s"scheduled ${msg.activationId} to $invoker, action '${msg.action.asString}', prewarm: ${msg.prewarmOnly.isDefined}, ${resourceLimit}, time limit ${timeLimit.duration.toMillis}ms")
        val activationResult = setupActivation(msg, action, invoker)
        sendActivationToInvoker(messageProducer, msg, aid, invoker).map(_ => activationResult)
      case None =>
        logging.warn(this, s"system is overloaded. Re-routing through topsched")
        // report the state of all invokers
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }
        if (msg.rerouteFromRack.isDefined) {
          // was already re-routed and couldn't be scheduled again. To prevent scheduling loop, cancel the message and
          // mark the scheduling as failed.
          logging.error(this, s"Message was already re-routed and couldn't be scheduled again. Failing activation: ${msg.activationId}")
          Future.failed(LoadBalancerException(s"no invokers available and already re-routed. Activation ${msg.activationId} failed"))
        } else {
          messageProducer.send("topsched", msg.copy(rerouteFromRack = Some(rackschedInstance))) .recoverWith {
            case t: Throwable =>
              logging.error(
                this,
                s"failed to re-route activation ${msg.activationId} to topsched: $t. action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
              Future.failed(LoadBalancerException(s"No invokers available, couldn't re-route to topsched: $t"))
          } flatMap { x =>
            Future.successful(Future.successful(Left(msg.activationId)))
          }
        }
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
    val totalActivationResources =
      if (isBlackboxInvocation) totalBlackBoxActivationMemory else totalManagedActivationMemory
    totalActivationResources.getAndUpdate(a => a + action.limits.resources.limits)


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
          action.limits.resources.limits,
          action.limits.timeout.duration,
          action.limits.concurrency.maxConcurrent,
          action.fullyQualifiedName(true),
          timeoutHandler,
          InvokerPoolResourceType.poolFor(action),
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
      case Success(acknowledgement) =>
        acknowledgement.isSlotFree.foreach { instance =>
          processCompletion(
            acknowledgement.activationId,
            acknowledgement.transid,
            forced = false,
            isSystemError = acknowledgement.isSystemError.getOrElse(false),
            instance)
        }

        acknowledgement.result.foreach { response =>
          processResult(acknowledgement.activationId, acknowledgement.transid, response)
        }
        acknowledgementFeed ! MessageFeed.Processed
      case Failure(exception) =>
        logging.error(this, s"failed processing message: $raw")
        acknowledgementFeed ! MessageFeed.Processed
      case _ =>
        logging.error(this, s"Unexpected Acknowledgment message received by racksched: $raw")
        acknowledgementFeed ! MessageFeed.Processed


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
        totalActivationMemory.getAndUpdate(a => a - entry.resourceLimit)
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
            s"forced completion ack for '$aid', action '${entry.fullyQualifiedEntityName}' ($actionType), $blockingType, resource limit ${entry.resourceLimit}, time limit ${entry.timeLimit.toMillis} ms, completion ack timeout $completionAckTimeout from $instance")(
            tid)

          MetricEmitter.emitCounterMetric(RACKSCHED_COMPLETION_ACK_FORCED)
        }

        // Completion acks that are received here are strictly from user actions - health actions are not part of
        // the load balancer's activation map. Inform the invoker pool supervisor of the user action completion.
        // guard this
        invoker.foreach(invokerPool ! InvocationFinishedMessage(_, invocationResult))
      case None if tid == TransactionId.invokerHealth || (tid.hasParent && tid.meta.parent.get.id == TransactionId.invokerHealth.id) =>
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

  /**
   * @param producer
   * @param msg
   * @param prevActivationId if this message was previously scheduled from another activation, this is the activation id that came through previously.
   * @param invoker
   * @return
   */
  protected def sendActivationToInvoker(producer: MessageProducer,
                                        msg: ActivationMessage,
                                        prevActivationId: Option[ActivationId],
                                        invoker: InvokerInstanceId): Future[RecordMetadata] = {
    implicit val transid: TransactionId = msg.transid

    val sentMsg = msg.copy(rootControllerIndex = rackschedInstance)
    val topic = invoker.getMainTopic

    MetricEmitter.emitCounterMetric(LoggingMarkers.LOADBALANCER_ACTIVATION_START)
    val start = transid.started(
      this,
      LoggingMarkers.CONTROLLER_KAFKA,
      s"posting topic '$topic' with activation id '${sentMsg.activationId}'",
      logLevel = InfoLevel)
    sentMsg.sendResultToInvoker.foreach(args => {
      val originatingInvoker = args._1
      val originalActivationId = args._2
      val nextAid: ActivationId = prevActivationId.getOrElse(sentMsg.activationId)
      producer.send(originatingInvoker.getSchedResultTopic,
        SchedulingDecision(originalActivationId, nextAid, invoker, transid, sentMsg.parallelismIdx, sentMsg.siblings))
        .failed.foreach(exception => {
            logging.warn(this, s"Failed to send message to topic: ${originatingInvoker.getSchedResultTopic}: ${exception}")
        })
    })

    // If the prevActivationId is empty, this is a new function being scheduled. In the case where it is _not_ empty
    // this means another function which was responsible for the scheduling already came through, we really just needed
    // to send the scheduling decision (above). So, to prevent scheduling the same object twice, just run prevent
    // sending the scheduling message to the destination.
    if (prevActivationId.isEmpty) {
      producer.send(topic, sentMsg).andThen {
        case Success(status) =>
          transid.finished(
            this,
            start,
            s"posted to ${status.topic()}[${status.partition()}][${status.offset()}]",
            logLevel = InfoLevel)
        case Failure(_) => transid.failed(this, start, s"error on posting to topic $topic")
      }
    } else {
      // hack hack hack
      // some dummy value because (as of writing) result actually not used anywhere
      Future.successful(new RecordMetadata(null, 0, 0,0,0,0,0))
    }
  }

  protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
    // Currently do nothing
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.resourceLimit, entry.iprt))
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
                                     sendActivationToInvoker: (MessageProducer, ActivationMessage, Option[ActivationId], InvokerInstanceId) => Future[RecordMetadata],
                                     monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, None, i),
            messagingProvider.getConsumer(whiskConfig, instance.healthTopic, instance.healthTopic, maxPeek = 128),
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
