package org.apache.openwhisk.core.containerpool

import akka.actor._
import org.apache.openwhisk.core.entity.ActivationId
import cats.implicits._

import scala.collection.mutable

case class TransportAddress(base: String, config: Map[String, String] = Map.empty) {
  def toConfigString : String = toConfigString("")
  def toConfigString(configBase: String) : String =
    configBase + base + config.map { case (k, v) => s"$k,$v;" }.mkString
  def +(rhs: TransportAddress): TransportAddress =
    TransportAddress(base + rhs.base, config ++ rhs.config)
}

case class TransportRequest(name: String, activationId: ActivationId, address: TransportAddress)
object TransportRequest {
  def apply(name: String, impl: String, activationId : ActivationId): TransportRequest =
    apply(name, activationId, TransportAddress(s"$name;$impl;"))
}

object TransportAddress {
  def TCPTransport(ip: String, port: Int) : TransportAddress = {
    TransportAddress("", Map("url" -> s"$ip:${port.toString}"))
  }
}


class CorunningAddressBook(pool: ActorRef) {
  type TransportId = (ActivationId, String)

  val addressBook : mutable.Map[TransportId, Either[Seq[TransportRequest], TransportAddress]] = mutable.Map.empty

  def postWait(activationId: ActivationId, request: TransportRequest) : Option[TransportAddress] = {
    val transId = (activationId, request.name)

    val newEntity = addressBook.getOrElse(transId, Left(Seq.empty))
                               .leftMap(_ :+ request)

    addressBook.update(transId, newEntity)
    newEntity
      .toOption
      .map(_ + request.address)
  }

  def signalReady(activationId: ActivationId,
                  transportName: String,
                  transportAddress: TransportAddress): Unit = {
    val transId = (activationId, transportName)

    addressBook.get(transId)
      .map {
        case Left(list) =>
          list.map { case TransportRequest(_, aid, address) =>
            pool ! TransportReady(aid, address + transportAddress)
          } // Send activations to the
        // TODO: log error for right
        case Right(_) => Seq.empty
      }

    addressBook.update(transId, Right(transportAddress))
  }

  def remove(activationId: ActivationId): Unit = {
    addressBook.retain { case ((id, _), _) => id != activationId }
  }

}
