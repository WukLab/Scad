package org.apache.openwhisk.core.topbalancer

import akka.actor.{Actor, ActorSystem}
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.database.{ActivationStoreProvider, CacheChangeNotification, UserContext}
import org.apache.openwhisk.core.entity.{ActivationId, WhiskActivation}
import org.apache.openwhisk.core.scheduler.{FinishActivation, IncompleteActivation}
import org.apache.openwhisk.spi.SpiLoader

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

/**
 * This actor receives messages which allow it to track and store activations for "applications"
 *
 *
 * an IncompleteActivation will store the activation in memory temporarily. a FinishActivation
 * message will record and store the final activation result in the database
 *
 * @param actorSystem
 * @param materializer
 * @param logging
 * @param ec
 * @param cacheChangeNotification
 */
case class AppActivator()(implicit val actorSystem: ActorSystem, materializer: ActorMaterializer, val logging: Logging, ec: ExecutionContext, cacheChangeNotification: Option[CacheChangeNotification] = None) extends Actor {

  implicit val transid: TransactionId = TransactionId.appActivator
  private val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, materializer, logging)

  val incompleteActivations: collection.mutable.Map[ActivationId, IncompleteActivation] = collection.mutable.Map()

   def receive: Receive = {
    case info: IncompleteActivation =>
      incompleteActivations.put(info.id, info)
    case fin: FinishActivation =>
      incompleteActivations.get(fin.id).map(incomplete => {
        val now = Instant.now()
        val act = WhiskActivation(
          incomplete.entityPath,
          incomplete.actionName,
          incomplete.user.subject,
          fin.id,
          Instant.ofEpochMilli(incomplete.startTimeEpochMillis),
          now,
          response = fin.response,
          duration = Some(now.toEpochMilli - incomplete.startTimeEpochMillis)
        )
        activationStore.store(act, UserContext(incomplete.user)) flatMap { docInfo =>
//          logging.debug(this, s"application activation ${fin.id} stored with $docInfo")

          Future.successful(())
        } recoverWith {
          case t: Throwable =>
            logging.warn(this, s"application activation store ${fin.id} FAILED with $t")
            Future.successful(())
        }
        incompleteActivations.remove(fin.id)
      })
  }
}
