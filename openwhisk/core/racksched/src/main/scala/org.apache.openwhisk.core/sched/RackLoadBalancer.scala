package org.apache.openwhisk.core.sched

import java.util.concurrent.atomic.LongAdder

import akka.actor.ActorRef
import akka.actor.TypedActor.dispatcher
import akka.actor.{ActorRefFactory, ActorSystem, Props}
import akka.event.Logging.InfoLevel
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common.LoggingMarkers.{ForcedAfterRegularCompletionAck, ForcedCompletionAck, HealthcheckCompletionAck, RegularAfterForcedCompletionAck, RegularCompletionAck}
import pureconfig.generic.auto._
import org.apache.openwhisk.common.{LogMarkerToken, Logging, LoggingMarkers, MetricEmitter, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig.wskApiHost
import org.apache.openwhisk.core.connector.{ActivationMessage, MessageFeed, MessageProducer, MessagingProvider}
import org.apache.openwhisk.core.containerpool.ContainerPoolConfig
import org.apache.openwhisk.core.entity.size.SizeInt
import org.apache.openwhisk.core.entity.{ActivationEntityLimit, ActivationId, ExecManifest, ExecutableWhiskActionMetaData, InstanceId, InvokerInstanceId, RackSchedInstanceId, TimeLimit, UUID, WhiskActivation}
import org.apache.openwhisk.core.loadBalancer.{ActivationEntry, FeedFactory, InvocationFinishedMessage, InvocationFinishedResult, InvokerHealth, ShardingContainerPoolBalancerConfig}
import org.apache.openwhisk.spi.Spi
import org.apache.openwhisk.spi.SpiLoader
import org.apache.openwhisk.utils.ExecutionContextFactory
import pureconfig.loadConfigOrThrow

import scala.collection.concurrent.TrieMap
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
                         implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
                    implicit actorSystem: ActorSystem,
                    logging: Logging,
                    materializer: ActorMaterializer) extends RackLoadBalancer {

  /** Loadbalancer interface methods */
   def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(IndexedSeq.empty[InvokerHealth])
   override def clusterSize: Int = 1

  /** State related to invocations and throttling */
  protected[sched] val activationSlots = TrieMap[ActivationId, ActivationEntry]()
  protected[sched] val activationPromises =
    TrieMap[ActivationId, Promise[Either[ActivationId, WhiskActivation]]]()
  protected val activationsPerNamespace = TrieMap[UUID, LongAdder]()
  protected val totalActivations = new LongAdder()
  protected val totalBlackBoxActivationMemory = new LongAdder()
  protected val totalManagedActivationMemory = new LongAdder()

  val lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

  val poolConfig: ContainerPoolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool)

  val invokerName = InvokerInstanceId(0, None, None, poolConfig.userMemory)

  /** 1. Publish a message to the rackLoadBalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    /** 2. Update local state with the activation to be executed scheduled. */
    val activationResult = setupActivation(msg, action, invokerName)
    sendActivationToInvoker(messageProducer, msg, invokerName).map(_ => activationResult)

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

  protected val messageProducer =
    messagingProvider.getProducer(config, Some(ActivationEntityLimit.MAX_ACTIVATION_LIMIT))

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

  /** Creates an invoker for executing user actions. There is only one invoker in the lean model. */
  private def makeALocalThreadedInvoker(): Unit = {
    implicit val ec = ExecutionContextFactory.makeCachedThreadPoolExecutionContext()
//    val limitConfig: ConcurrencyLimitConfig = loadConfigOrThrow[ConcurrencyLimitConfig](ConfigKeys.concurrencyLimit)
//    SpiLoader.get[InvokerProvider].instance(config, invokerName, messageProducer, poolConfig, limitConfig)
  }

  makeALocalThreadedInvoker()

  protected val invokerPool: ActorRef = actorSystem.actorOf(Props.empty)

  protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
    // Currently do nothing
  }

  /** Gets the number of in-flight activations for a specific user. */
  override def activeActivationsFor(namespace: UUID): Future[Int] = Future.successful(activationsPerNamespace.get(namespace).map(_.intValue).getOrElse(0))

  /** Gets the number of in-flight activations in the system. */
  override def totalActiveActivations: Future[Int] = Future.successful(totalActivations.intValue)
}

object RackLoadBalancer extends RackLoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: RackSchedInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): RackSimpleBalancer = {

    new RackSimpleBalancer(whiskConfig, createFeedFactory(whiskConfig, instance), instance)
  }

  def requiredProperties =
    ExecManifest.requiredProperties ++
      wskApiHost
}
