package org.apache.openwhisk.core.containerpool

import akka.actor._
import org.apache.openwhisk.core.entity.ActivationId
import cats.implicits._
import org.apache.openwhisk.common.Logging

import scala.collection.mutable

case class TransportAddress(base: String, config: Map[String, String] = Map.empty) {
  def toConfigString : String = config.map { case (k, v) => s"$k,$v;" }.mkString
  def toFullString : String = toFullString("")
  def toFullString(configBase: String) : String =
    configBase + base + toConfigString
  def +(rhs: TransportAddress): TransportAddress =
    TransportAddress(base + rhs.base, config ++ rhs.config)
}

object TransportAddress {
  def TCPTransport(ip: String, port: String) : TransportAddress = {
    TransportAddress("", Map("url" -> s"tcp://$ip:${port}"))
  }
  def Base(base: String): TransportAddress = {
    TransportAddress(base)
  }
  def empty: TransportAddress = TransportAddress("")
}

case class TransportRequest(name: String,
                            activationId: ActivationId,
                            address: TransportAddress,
                            create: Boolean)
object TransportRequest {
  def config(name: String, impl: String, activationId : ActivationId): TransportRequest =
    apply(name, activationId, TransportAddress(s"$name;$impl;"), false)
}


class CorunningAddressBook(pool: ContainerPool)(implicit logging: Logging) {
  type TransportId = (ActivationId, String)

  val addressBook : mutable.Map[TransportId, Either[Seq[TransportRequest], TransportAddress]] = mutable.Map.empty
  val preparedSignals : mutable.Map[ActorRef, (ActivationId, String, TransportAddress)] = mutable.Map.empty

  def postWait(activationId: ActivationId, request: TransportRequest) : TransportAddress = {
    val transId = (activationId, request.name)
    logging.warn(this, s"registering ${activationId}, $request")

    val newEntity = addressBook.getOrElse(transId, Left(Seq.empty))
                               .leftMap(_ :+ request)

    addressBook.update(transId, newEntity)
    // return basic transport for new requests
    request.address + newEntity.toOption.getOrElse(TransportAddress.empty)
  }

  def prepareSignal(containerRef: ActorRef,
                    activationId: ActivationId,
                    transportName : String,
                    transportAddress: TransportAddress): Unit = {
    logging.warn(this, s"preparing ${activationId}, ${transportName} with address ${transportAddress}")
    preparedSignals += containerRef -> (activationId, transportName, transportAddress)
  }

  def signalReady(containerRef: ActorRef, containerIp: String): Unit = {
    preparedSignals
      .remove(containerRef)
      .foreach { case (aid, name, addr) =>
        signalReady(aid, name, TransportAddress.TCPTransport(containerIp, addr.base)) }
    // For TCP only
  }

  def signalReady(activationId: ActivationId,
                  transportName: String,
                  transportAddress: TransportAddress): Unit = {
    val transId = (activationId, transportName)

    logging.warn(this, s"signaling ${activationId}, ${transportName} with address ${transportAddress}")
    logging.warn(this, s"map $addressBook")

    addressBook.get(transId)
      .map {
        case Left(list) =>
          list.map { request =>
            val sourceAid = request.activationId
            logging.warn(this, s"notifying actor ${pool.activationMap(request.activationId)}")
            pool.activationMap(sourceAid) ! LibdTransportConfig(sourceAid, request, transportAddress) }
        // TODO: log error for right
        case Right(_) => Seq.empty
      }

    addressBook.update(transId, Right(transportAddress))
  }

  def remove(activationId: ActivationId): Unit = {
    addressBook.retain { case ((id, _), _) => id != activationId }
  }

}
