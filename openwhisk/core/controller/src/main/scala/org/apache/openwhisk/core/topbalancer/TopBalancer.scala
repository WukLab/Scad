package org.apache.openwhisk.core.topbalancer

import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.connector.MessagingProvider
import org.apache.openwhisk.core.entity.ActivationId
import org.apache.openwhisk.core.entity.ControllerInstanceId
import org.apache.openwhisk.core.entity.ExecutableWhiskActionMetaData
import org.apache.openwhisk.core.entity.RackSchedInstanceId
import org.apache.openwhisk.core.entity.TopSchedInstanceId
import org.apache.openwhisk.core.entity.UUID
import org.apache.openwhisk.core.entity.WhiskActivation
import org.apache.openwhisk.core.loadBalancer.FeedFactory
import org.apache.openwhisk.spi.Spi

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class RackHealth(val id: RackSchedInstanceId, val status: RackState) {
  override def equals(obj: scala.Any): Boolean = obj match {
    case that: RackHealth => that.id == this.id && that.status == this.status
    case _                   => false
  }

  override def toString = s"RackHealth($id, $status)"

}

trait TopBalancer {
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
   * Returns a message indicating the health of the racks and/or rack pool in general.
   *
   * @return a Future[IndexedSeq[Rackhealth]] representing the health of the racks managed by the loadbalancer.
   */
  def rackHealth(): Future[IndexedSeq[RackHealth]]

  /** Gets the number of in-flight activations for a specific user. */
  def activeActivationsFor(namespace: UUID): Future[Int]

  /** Gets the number of in-flight activations in the system. */
  def totalActiveActivations: Future[Int]

  /** Gets the size of the cluster all loadbalancers are acting in */
  def clusterSize: Int = 1

  def id: TopSchedInstanceId
}

trait TopBalancerProvider extends Spi {
  def requiredProperties: Map[String, String]

  def instance(whiskConfig: WhiskConfig, instance: TopSchedInstanceId)(implicit actorSystem: ActorSystem,
                                                                       logging: Logging,
                                                                       materializer: ActorMaterializer): TopBalancer

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
