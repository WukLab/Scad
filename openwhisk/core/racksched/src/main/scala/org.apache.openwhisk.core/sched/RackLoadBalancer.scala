package org.apache.openwhisk.core.sched

import akka.actor.{ActorRefFactory, ActorSystem, Props}
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector.{MessageFeed, MessagingProvider}
import org.apache.openwhisk.core.entity.RackSchedInstanceId
import org.apache.openwhisk.core.loadBalancer.{FeedFactory, LoadBalancer}
import org.apache.openwhisk.spi.Spi

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class RackLoadBalancer {

}

/**
 * An Spi for providing load balancer implementations.
 */
trait RackLoadBalancerProvider extends Spi {
  def requiredProperties: Map[String, String]

  def instance(whiskConfig: WhiskConfig, instance: RackSchedInstanceId)(implicit actorSystem: ActorSystem,
                                                                        logging: Logging,
                                                                        materializer: ActorMaterializer): LoadBalancer

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
