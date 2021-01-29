package org.apache.openwhisk.core.containerpool

import akka.actor._
import org.apache.openwhisk.core.entity.ActivationId

import scala.collection.mutable

case class TransportAddress(url : String, config : Map[String, String] = Map.empty)


class CorunningAddressBook(pool: ActorRef) {
  type TransportId = (ActivationId, String)

  val addressBook : mutable.Map[TransportId, Either[Seq[ActivationId], TransportAddress]] = mutable.Map.empty

  def postWait(activationId: ActivationId, transportName: String) : Option[TransportAddress] = {
    val transId = (activationId, transportName)

    val newEntity = addressBook.getOrElse(transId, Left(Seq.empty)) match {
      case Left(seq)    => Left(seq :+ activationId)
      case res@Right(_) => res
    }

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
    addressBook.filterKeys(_._1 == activationId)
  }

}
