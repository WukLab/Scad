package org.apache.openwhisk.core.containerpool

import akka.actor._
import org.apache.openwhisk.core.entity.ActivationId
import cats.implicits._

import scala.collection.mutable

case class TransportAddress(base: String, config: Map[String, String] = Map.empty) {
  def toConfigString(configBase: String = "") : String =
    configBase + base + config.map { case (k, v) => s"$k,$v;" }.mkString
}

object TransportAddress {
  def TCPTransport(trans: String, ip: String, port: Int) : TransportAddress = {
    TransportAddress(trans, Map("url" -> s"$ip:${port.toString}"))
  }
}


class CorunningAddressBook(pool: ActorRef) {
  type TransportId = (ActivationId, String)

  val addressBook : mutable.Map[TransportId, Either[Seq[ActivationId], TransportAddress]] = mutable.Map.empty

  def postWait(activationId: ActivationId, transportName: String) : Option[TransportAddress] = {
    val transId = (activationId, transportName)

    val newEntity = addressBook.getOrElse(transId, Left(Seq.empty))
                               .leftMap(_ :+ activationId)

    addressBook.update(transId, newEntity)
    newEntity.toOption
  }

  def signalReady(activationId: ActivationId,
                  transportName: String,
                  transportAddress: TransportAddress): Unit = {
    val transId = (activationId, transportName)

    addressBook.get(transId)
      .map {
        case Left(list) =>
          list.map(pool ! TransportReady(_, transportName, transportAddress)) // Send activations to the
        // TODO: log error for right
        case Right(_) => Seq.empty
      }

    addressBook.update(transId, Right(transportAddress))
  }

  def remove(activationId: ActivationId): Unit = {
    addressBook.retain { case ((id, _), _) => id != activationId }
  }

}
