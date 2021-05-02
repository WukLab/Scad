package org.apache.openwhisk.core.topbalancer
import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.LongAdder
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
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.LoggingMarkers
import org.apache.openwhisk.common.MetricEmitter
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig.kafkaHosts
import org.apache.openwhisk.core.connector.{ActivationMessage, MessageConsumer, MessageFeed, MessageProducer, MessagingProvider}
import org.apache.openwhisk.core.containerpool.RuntimeResources
import org.apache.openwhisk.core.entity.{ActivationEntityLimit, ActivationId, ExecutableWhiskActionMetaData, FullyQualifiedEntityName, RackSchedInstanceId, ResourceLimit, TimeLimit, TopSchedInstanceId, UUID, WhiskActionMetaData, WhiskActivation, WhiskAuthStore, WhiskEntityStore}
import org.apache.openwhisk.core.entity.types.{AuthStore, EntityStore}
import org.apache.openwhisk.core.loadBalancer.ClusterConfig
import org.apache.openwhisk.core.loadBalancer.FeedFactory
import org.apache.openwhisk.core.loadBalancer.LoadBalancerException
import org.apache.openwhisk.core.loadBalancer.ShardingContainerPoolBalancer
import org.apache.openwhisk.core.scheduler.FinishActivation
import org.apache.openwhisk.spi.SpiLoader
import pureconfig.loadConfigOrThrow
import pureconfig._
import pureconfig.generic.auto._
import spray.json._

import java.nio.charset.StandardCharsets
import java.util.Objects
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration, MINUTES}
import scala.util.{Failure, Success, Try}

class DefaultTopBalancer(config: WhiskConfig,
                         feedFactory: FeedFactory,
                         val rackPoolFactory: RackPoolFactory,
                         val instance: TopSchedInstanceId)(implicit actorSystem: ActorSystem,
                           logging: Logging,
                           materializer: ActorMaterializer,
                           implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider],
                                                           appActivator: ActorRef)
                           extends TopBalancer {
  protected implicit val executionContext: ExecutionContext = actorSystem.dispatcher
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

  // map which based on a tuple-key stores the previously-scheduled location and two other values (id, int0, int1) where
  // int0 represents the total scheduled messages based on this value
  // int1 represent the max number of messages scheduled using this map
  // once int0 == int1, then the key should be removed from the map
  // in the case of an error, the scheduling message will be removed after ~5 minutes if the number of expected scheduled
  // messages does not come through.
  // the key of the map is (app activation ID, function activation ID, scheduled function name, parallelismIdx)
  val preScheduled: mutable.Map[(ActivationId, ActivationId, FullyQualifiedEntityName), (RackSchedInstanceId, Int, Int)] = mutable.Map.empty

  protected val messageProducer: MessageProducer =
    messagingProvider.getProducer(config, Some(ActivationEntityLimit.MAX_ACTIVATION_LIMIT))
  private val activationConsumer: MessageConsumer = messagingProvider.getConsumer(config, "topsched", "topsched", 128)
  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed("topsched", logging, activationConsumer, 4096, 1.second, processSchedulingMessage)
  })

  val state = TopBalancerState()
  protected val activationsPerNamespace = TrieMap[UUID, LongAdder]()
  protected val totalActivations = new LongAdder()
  protected val totalBlackBoxActivationResources = new AtomicReference[RuntimeResources](RuntimeResources.none())
  protected val totalManagedActivationResources = new AtomicReference[RuntimeResources](RuntimeResources.none())

  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentRackPoolState(newState) =>
        state.updateRacks(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        state.updateCluster(availableMembers.size)

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

        state.updateCluster(availableMembers.size)
    }
  }))

  val rackPool: ActorRef = rackPoolFactory.createRackPool(actorSystem,
    messagingProvider,
    messageProducer,
    sendActivationToRack,
    Some(monitor))

  implicit val entityStore: EntityStore = WhiskEntityStore.datastore()
  implicit val authStore: AuthStore = WhiskAuthStore.datastore()

  protected[DefaultTopBalancer] def processSchedulingMessage(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    Try(raw.parseJson) map { value =>
      Future.fromTry(Try(ActivationMessage.serdes.read(value)))
        .andThen {
          case _ => activationFeed ! MessageFeed.Processed
        }.map { activation =>
        implicit val transid: TransactionId = activation.transid
        WhiskActionMetaData.resolveActionAndMergeParameters(entityStore, activation.action, activation.appActivationId) onComplete {
          case Success(metadata) =>
            publish(metadata.toExecutableWhiskAction.get, activation)
          case Failure(exception) =>
            logging.error(this, s"Failed to publish rack activation: $exception")
        }
      } recover {
        case t =>
          Try(FinishActivation.serdes.read(value)).map(activation => {
            appActivator ! activation
          })
      } recover {
        case t => logging.error(this, s"failed after parsing json of topsched msg: ${t} - $raw")

      }
    } recover {
    case t => logging.error(this, s"failed processing top level scheduler message: ${t} - $raw")
    }
}

  /**
   * Publishes activation message on internal bus for an invoker to pick up.
   *
   * @param action  the action to invoke
   * @param msg     the activation message to publish on an invoker topic
   * @param transid the transaction id for the request
   * @return result a nested Future the outer indicating completion of publishing and
   *         the inner the completion of the action (i.e., the result)
   *         if it is ready before timeout (Right) otherwise the activation id (Left).
   *         The future is guaranteed to complete within the declared action time limit
   *         plus a grace period (see activeAckTimeoutGrace).
   */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
    transid.mark(this, LoggingMarkers.TOPSCHED_SCHED_BEGIN)
    val (racksToUse, stepSizes) = (state.racks, state.stepSizes)

    val hash: Int = Objects.hash(msg.user.namespace.name.toString, action.namespace.segment(1).getOrElse(action.fullyQualifiedName(false))).abs

    if (racksToUse.nonEmpty) {
      val homeInvoker = hash % racksToUse.size
      val stepSize = stepSizes(hash % stepSizes.size)
      val defaultSchedule = () =>
        DefaultTopBalancer.schedule(action.limits.concurrency.maxConcurrent,
        action.fullyQualifiedName(true),
        racksToUse,
        homeInvoker,
        stepSize)

      val rack: Option[RackSchedInstanceId] = msg.waitForContent match {
        case Some(content) =>
          if (content > 1 && msg.prewarmOnly.isEmpty) {
            msg.appActivationId flatMap { appId =>
              msg.functionActivationId flatMap { funcId =>
                val key = (appId, funcId, msg.action)
                preScheduled.get(key) match {
                  case Some(value) =>
                    // this message came through the scheduler before..return the same result
                    if (value._2 + 1 >= value._3) {
                      // we've reached the max number of messages to schedule..let's get rid of it
                      preScheduled.remove(key)
                    } else {
                      preScheduled.put(key, (value._1, value._2 + 1, value._3))
                    }
                    Some(value._1)
                  case None =>
                    // we have not seen the message yet..schedule the result, then store the scheduling message
                    // and set a timer to remove it
                    defaultSchedule() match {
                      case Some(value) =>
                        // waitForContent includes all previous nodes, but this activation msg is a parallelism copy,
                        // then one of the nodes from "waitForContent" is parallelized, so substract 1, and add the
                        // number of parallel copies
                        val maxScheds = action.relationships match {
                          case Some(value) =>
                            msg.parallelismIdx.max * value.parents.length
                          case None => msg.parallelismIdx.max
                        }
                        preScheduled.put(key, (value, 1, content + maxScheds))
                        actorSystem.getScheduler.scheduleOnce(FiniteDuration(5, MINUTES))(() => preScheduled.remove(key))
                        Some(value)
                      case _ => None
                    }
                }
              }
            }
          } else {
            defaultSchedule()
          }
        case None => defaultSchedule()

      }
      rack.map { rack =>
        val resourceLimit = action.limits.resources
        val resourceLimitInfo = if (resourceLimit == ResourceLimit()) { "std" } else { "non-std" }
        val timeLimit = action.limits.timeout
        val timeLimitInfo = if (timeLimit == TimeLimit()) { "std" } else { "non-std" }
        logging.info(
          this,
          s"sent activation to rack activation ${msg.activationId}, action '${msg.action.asString}', ns '${msg.user.namespace.name.asString}', resource limit ${resourceLimit} (${resourceLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}, prewarm: ${msg.prewarmOnly.isDefined}) to ${rack}")
        val activationResult = setupActivation(msg, action, rack)
        sendActivationToRack(messageProducer, msg, rack).map(_ => activationResult)
      }
      .getOrElse {
        // report the state of all invokers
        val rackStates = racksToUse.foldLeft(Map.empty[RackState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }

        logging.error(
          this,
          s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}', ns '${msg.user.namespace.name.asString}' - invokers to use: $rackStates")
        Future.failed(LoadBalancerException("No invokers available"))
      }
    } else {
      logging.error(
        this,
        s"The number of available racks is ${racksToUse.size}. Cannot determine the home invoker.")
        Future.failed(LoadBalancerException("No rack available!"))
    }
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
                                instance: RackSchedInstanceId): Future[Either[ActivationId, WhiskActivation]] = {

    // Needed for emitting metrics.
    totalActivations.increment()
    val isBlackboxInvocation = action.exec.pull
    val totalActivationResources =
      if (isBlackboxInvocation) totalBlackBoxActivationResources else totalManagedActivationResources
    totalActivationResources.getAndUpdate(a => a + action.limits.resources.limits)

    activationsPerNamespace.getOrElseUpdate(msg.user.namespace.uuid, new LongAdder()).increment()
    Future.successful(Left(msg.activationId))
  }

  /** 3. Send the activation to the invoker */
  protected def sendActivationToRack(producer: MessageProducer,
                                     msg: ActivationMessage,
                                     racksched: RackSchedInstanceId): Future[RecordMetadata] = {
    implicit val transid: TransactionId = msg.transid
    val topic = racksched.toString

    MetricEmitter.emitCounterMetric(LoggingMarkers.LOADBALANCER_ACTIVATION_START)
    val start = transid.started(
      this,
      LoggingMarkers.CONTROLLER_KAFKA,
      s"posting topic '$topic' with activation id '${msg.activationId}' (prewarmOnly: ${msg.prewarmOnly.isDefined})",
      logLevel = InfoLevel)
    transid.mark(this, LoggingMarkers.TOPSCHED_SCHED_END)
//    logging.debug(this, s"posting to racksched: ${msg.activationId}: ||latency: ${Interval.currentLatency()}")

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

  /**
   * Returns a message indicating the health of the racks and/or rack pool in general.
   *
   * @return a Future[IndexedSeq[Rackhealth]] representing the health of the racks managed by the loadbalancer.
   */
  override def rackHealth(): Future[IndexedSeq[RackHealth]] = Future.successful(state.rackHealth)

  /** Gets the number of in-flight activations for a specific user. */
  override def activeActivationsFor(namespace: UUID): Future[Int] = { Future.successful(0) }

  /** Gets the number of in-flight activations in the system. */
  override def totalActiveActivations: Future[Int] = { Future.successful(0) }

  def id: TopSchedInstanceId = instance

}

case class TopBalancerState(
  private var _rackHealth: IndexedSeq[RackHealth] = IndexedSeq.empty,
  private var _racks: IndexedSeq[RackHealth] = IndexedSeq.empty,
  private var _managedStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
  private var _numRacks: Int = 1
                           )(implicit logging: Logging) {


  def rackHealth: IndexedSeq[RackHealth] = _rackHealth
  def racks: IndexedSeq[RackHealth] = _racks
  def stepSizes: Seq[Int] = _managedStepSizes

  def updateRacks(newRacks: IndexedSeq[RackHealth]): Unit = {
    val oldSize = _racks.size
    val newSize = newRacks.size
    _racks = newRacks
    if (oldSize != newSize) {
      val managed = Math.max(1, Math.ceil(newSize.toDouble).toInt)
      _managedStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(managed)
    }
  }

  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_numRacks != actualSize) {
      val oldSize = _numRacks
      _numRacks = actualSize
      logging.info(
        this,
        s"loadbalancer cluster size changed from $oldSize to $actualSize active nodes.")(
        TransactionId.topsched)
    }
  }

}

object DefaultTopBalancer extends TopBalancerProvider {

  /**
   * Scans through all racks and searches for a rack to get a free slot on. If no slot can be
   * obtained, randomly picks a healthy rack.
   *
   * @param maxConcurrent concurrency limit supported by this action
   * @param racks a list of available invokers to search in, including their state
   * @param index the index to start from (initially should be the "homeInvoker"
   * @param step stable identifier of the entity to be scheduled
   * @return an invoker to schedule to or None of no invoker is available
   */
  @tailrec
  def schedule(
                maxConcurrent: Int,
                fqn: FullyQualifiedEntityName,
                racks: IndexedSeq[RackHealth],
                index: Int,
                step: Int,
                stepsDone: Int = 0)(implicit logging: Logging, transId: TransactionId): Option[RackSchedInstanceId] = {
    val numRacks = racks.size

    if (numRacks > 0) {
      val rack = racks(index)
      if (rack.status.isUsable) {
        Some(rack.id)
      } else {
        // If we've gone through all invokers
        if (stepsDone == numRacks + 1) {
          val healthyRacks = racks.filter(_.status.isUsable)
          if (healthyRacks.nonEmpty) {
            // Choose a healthy rack randomly
            val random = healthyRacks(ThreadLocalRandom.current().nextInt(healthyRacks.size)).id
            logging.warn(this, s"system is overloaded. Chose rack${random.toInt} by random assignment.")
            Some(random)
          } else {
            None
          }
        } else {
          val newIndex = (index + step) % numRacks
          schedule(maxConcurrent, fqn, racks, newIndex, step, stepsDone + 1)
        }
      }
    } else {
      None
    }
  }

  override def requiredProperties: Map[String, String] = kafkaHosts

  override def instance(whiskConfig: WhiskConfig, instance: TopSchedInstanceId)(implicit actorSystem: ActorSystem, logging: Logging, materializer: ActorMaterializer, appActivator: ActorRef): TopBalancer = {
    val rackPoolFactory = new RackPoolFactory {
      override def createRackPool(actorRefFactory: ActorRefFactory,
                                      messagingProvider: MessagingProvider,
                                      messagingProducer: MessageProducer,
                                      sendActivationToRack: (MessageProducer, ActivationMessage, RackSchedInstanceId) => Future[RecordMetadata],
                                      monitor: Option[ActorRef]): ActorRef = {

        RackPool.prepare(instance, WhiskEntityStore.datastore())
        actorRefFactory.actorOf(
          RackPool.props(
            (f, i) => f.actorOf(RackActor.props(i, instance)),
            (m, i) => sendActivationToRack(messagingProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"rackHealth${instance.asString}", "rackHealth", maxPeek = 128),
            monitor))
      }
    }
    new DefaultTopBalancer(whiskConfig, createFeedFactory(whiskConfig, instance), rackPoolFactory, instance)
  }
}
