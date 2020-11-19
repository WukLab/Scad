package org.apache.openwhisk.core.sched

import akka.actor.ActorRef
import akka.actor.{ActorRefFactory, ActorSystem, Props}
import akka.stream.ActorMaterializer
import pureconfig.generic.auto._
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig.wskApiHost
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.connector.{MessageFeed, MessagingProvider}
import org.apache.openwhisk.core.containerpool.ContainerPoolConfig
import org.apache.openwhisk.core.entity.ActivationId
import org.apache.openwhisk.core.entity.ControllerInstanceId
import org.apache.openwhisk.core.entity.ExecManifest
import org.apache.openwhisk.core.entity.ExecutableWhiskActionMetaData
import org.apache.openwhisk.core.entity.InvokerInstanceId
import org.apache.openwhisk.core.entity.UUID
import org.apache.openwhisk.core.entity.WhiskActivation
import org.apache.openwhisk.core.loadBalancer.ActivationEntry
import org.apache.openwhisk.core.loadBalancer.CommonLoadBalancer
import org.apache.openwhisk.core.loadBalancer.InvokerHealth
import org.apache.openwhisk.core.loadBalancer.{FeedFactory, LoadBalancer}
import org.apache.openwhisk.spi.Spi
import org.apache.openwhisk.spi.SpiLoader
import org.apache.openwhisk.utils.ExecutionContextFactory
import pureconfig.loadConfigOrThrow

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

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

  def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                         logging: Logging,
                                                                         materializer: ActorMaterializer): LoadBalancer

  /** Return default FeedFactory */
  def createFeedFactory(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
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
                         rackschedInstance: ControllerInstanceId,
                         implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
                    implicit actorSystem: ActorSystem,
                    logging: Logging,
                    materializer: ActorMaterializer)
  extends CommonLoadBalancer(config, feedFactory, rackschedInstance) {

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(IndexedSeq.empty[InvokerHealth])
  override def clusterSize: Int = 1

  val poolConfig: ContainerPoolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool)

  val invokerName = InvokerInstanceId(0, None, None, poolConfig.userMemory)

  /** 1. Publish a message to the loadbalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    /** 2. Update local state with the activation to be executed scheduled. */
    val activationResult = setupActivation(msg, action, invokerName)
    sendActivationToInvoker(messageProducer, msg, invokerName).map(_ => activationResult)

  }

  /** Creates an invoker for executing user actions. There is only one invoker in the lean model. */
  private def makeALocalThreadedInvoker(): Unit = {
    implicit val ec = ExecutionContextFactory.makeCachedThreadPoolExecutionContext()
//    val limitConfig: ConcurrencyLimitConfig = loadConfigOrThrow[ConcurrencyLimitConfig](ConfigKeys.concurrencyLimit)
//    SpiLoader.get[InvokerProvider].instance(config, invokerName, messageProducer, poolConfig, limitConfig)
  }

  makeALocalThreadedInvoker()

  override protected val invokerPool: ActorRef = actorSystem.actorOf(Props.empty)

  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
    // Currently do nothing
  }

  override protected def emitMetrics() = {
    super.emitMetrics()
  }
}

object RackLoadBalancer extends RackLoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = {

    new RackSimpleBalancer(whiskConfig, createFeedFactory(whiskConfig, instance), instance)
  }

  def requiredProperties =
    ExecManifest.requiredProperties ++
      wskApiHost
}
