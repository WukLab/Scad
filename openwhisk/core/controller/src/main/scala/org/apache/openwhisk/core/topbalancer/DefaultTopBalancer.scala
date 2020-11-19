package org.apache.openwhisk.core.topbalancer
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.LongAdder

import akka.actor.ActorSystem
import akka.actor.TypedActor.dispatcher
import akka.event.Logging.InfoLevel
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.LoggingMarkers
import org.apache.openwhisk.common.MetricEmitter
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.connector.MessageProducer
import org.apache.openwhisk.core.connector.MessagingProvider
import org.apache.openwhisk.core.entity.ActivationEntityLimit
import org.apache.openwhisk.core.entity.ActivationId
import org.apache.openwhisk.core.entity.ControllerInstanceId
import org.apache.openwhisk.core.entity.ExecutableWhiskActionMetaData
import org.apache.openwhisk.core.entity.FullyQualifiedEntityName
import org.apache.openwhisk.core.entity.MemoryLimit
import org.apache.openwhisk.core.entity.RackSchedInstanceId
import org.apache.openwhisk.core.entity.TimeLimit
import org.apache.openwhisk.core.entity.UUID
import org.apache.openwhisk.core.entity.WhiskActivation
import org.apache.openwhisk.core.loadBalancer.LoadBalancer
import org.apache.openwhisk.core.loadBalancer.LoadBalancerException
import org.apache.openwhisk.core.loadBalancer.ShardingContainerPoolBalancer

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

class DefaultTopBalancer(config: WhiskConfig)(implicit actorSystem: ActorSystem,
                           logging: Logging,
                           materializer: ActorMaterializer,
                           messagingProvider: MessagingProvider) extends TopBalancer {

  val state = TopBalancerState()
  protected val activationsPerNamespace = TrieMap[UUID, LongAdder]()
  protected val totalActivations = new LongAdder()
  protected val totalBlackBoxActivationMemory = new LongAdder()
  protected val totalManagedActivationMemory = new LongAdder()

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
    val (racksToUse, stepSizes) = (state.racks, state.stepSizes)
    val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
    val homeInvoker = hash % racksToUse.size
    val stepSize = stepSizes(hash % stepSizes.size)
    val rack = DefaultTopBalancer.schedule(action.limits.concurrency.maxConcurrent,
      action.fullyQualifiedName(true),
      racksToUse,
//      action.limits.memory.megabytes,
      homeInvoker,
      stepSize)

    rack.map { rack =>
      val memoryLimit = action.limits.memory
      val memoryLimitInfo = if (memoryLimit == MemoryLimit()) { "std" } else { "non-std" }
      val timeLimit = action.limits.timeout
      val timeLimitInfo = if (timeLimit == TimeLimit()) { "std" } else { "non-std" }
      logging.info(
        this,
        s"sent activation to rack activation ${msg.activationId}, action '${msg.action.asString}', ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${rack}")
      val activationResult = setupActivation(msg, action, rack)
      sendActivationToRack(messageProducer, msg, rack).map(_ => activationResult)
    }
    .getOrElse {
      // report the state of all invokers
      val invokerStates = racksToUse.foldLeft(Map.empty[RackState, Int]) { (agg, curr) =>
        val count = agg.getOrElse(curr.status, 0) + 1
        agg + (curr.status -> count)
      }

      logging.error(
        this,
        s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}', ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
      Future.failed(LoadBalancerException("No invokers available"))
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
    val totalActivationMemory =
      if (isBlackboxInvocation) totalBlackBoxActivationMemory else totalManagedActivationMemory
    totalActivationMemory.add(action.limits.memory.megabytes)

    activationsPerNamespace.getOrElseUpdate(msg.user.namespace.uuid, new LongAdder()).increment()
    Future.successful(Left(msg.activationId))
  }

  protected val messageProducer =
    messagingProvider.getProducer(config, Some(ActivationEntityLimit.MAX_ACTIVATION_LIMIT))

  /** 3. Send the activation to the invoker */
  protected def sendActivationToRack(producer: MessageProducer,
                                     msg: ActivationMessage,
                                     racksched: RackSchedInstanceId): Future[RecordMetadata] = {
    implicit val transid: TransactionId = msg.transid

    val topic = s"racksched${racksched.toInt}"

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

  /**
   * Returns a message indicating the health of the racks and/or rack pool in general.
   *
   * @return a Future[IndexedSeq[Rackhealth]] representing the health of the racks managed by the loadbalancer.
   */
  override def rackHealth(): Future[IndexedSeq[RackHealth]] = Future.successful(state.rackHealth)

  /** Gets the number of in-flight activations for a specific user. */
  override def activeActivationsFor(namespace: UUID): Future[Int] = ???

  /** Gets the number of in-flight activations in the system. */
  override def totalActiveActivations: Future[Int] = ???


}

case class TopBalancerState(
  private var _rackHealth: IndexedSeq[RackHealth] = IndexedSeq.empty,
  private var _racks: IndexedSeq[RackHealth] = IndexedSeq.empty,
  private var _managedStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0)
                           ) {
  def rackHealth: IndexedSeq[RackHealth] = _rackHealth
  def racks: IndexedSeq[RackHealth] = _racks
  def stepSizes: Seq[Int] = _managedStepSizes

}

object DefaultTopBalancer extends TopBalancerProvider {

  /**
   * Scans through all racks and searches for a rack to get a free slot on. If no slot can be
   * obtained, randomly picks a healthy rack.
   *
   * @param maxConcurrent concurrency limit supported by this action
   * @param racks a list of available invokers to search in, including their state
//   * @param dispatched semaphores for each invoker to give the slots away from
//   * @param slots Number of slots, that need to be acquired (e.g. memory in MB)
   * @param index the index to start from (initially should be the "homeInvoker"
   * @param step stable identifier of the entity to be scheduled
   * @return an invoker to schedule to or None of no invoker is available
   */
  @tailrec
  def schedule(
                maxConcurrent: Int,
                fqn: FullyQualifiedEntityName,
                racks: IndexedSeq[RackHealth],
//                dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
//                slots: Int,
                index: Int,
                step: Int,
                stepsDone: Int = 0)(implicit logging: Logging, transId: TransactionId): Option[RackSchedInstanceId] = {
    val numRacks = racks.size

    if (numRacks > 0) {
      val rack = racks(index)
      if (rack.status.isUsable) {
//        if (rack.status.isUsable && dispatched(rack.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) {
        Some(rack.id)
      } else {
        // If we've gone through all invokers
        if (stepsDone == numRacks + 1) {
          val healthyInvokers = racks.filter(_.status.isUsable)
          if (healthyInvokers.nonEmpty) {
            // Choose a healthy rack randomly
            val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
//            dispatched(random.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
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

  override def requiredProperties: Map[String, String] = ???

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem, logging: Logging, materializer: ActorMaterializer): LoadBalancer = ???
}
