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
  def TCPTransport(ip: String, port: String) : TransportAddress =
    TransportAddress("", Map("url" -> s"tcp://$ip:${port}"))
  def ProxyTransport(peerInfo: String) : TransportAddress =
    TransportAddress("", Map("peerinfo" -> peerInfo))
  def Base(base: String): TransportAddress =
    TransportAddress(base)
  def empty: TransportAddress = TransportAddress("")
}

// name: target Name, id: Target Id
// TODO: Warining: firlds has diffferent meaninig in proxy or non proxy mode.
// Proxy: (aid,name) for local element's given transport
// NonProxy: (aid, name) for **remote element**
// TODO: change operation to Enum
abstract class TransportRequestOp

case class TransportRequest(name: String,
                            activationId: ActivationId,
                            address: TransportAddress,
                            operation: TransportRequestOp) {
  def toProxyAddress = ProxyAddress(activationId, name)
}
object TransportRequest {
  object Create extends TransportRequestOp
  object Config extends TransportRequestOp
  object GetMessage extends TransportRequestOp

  def getMessage(activationId: ActivationId): TransportRequest =
    apply("", activationId, TransportAddress.empty, GetMessage)
  def config(name: String, impl: String, activationId : ActivationId): TransportRequest =
    apply(name, activationId, TransportAddress(s"$name;$impl;"), Config)
  def configPar(name: String,
                parallelism : Int,
                impl: String,
                activationId : ActivationId): TransportRequest =
    apply(name, activationId, TransportAddress(s"$name@$parallelism;$impl;"), Config)
}

// address book for containers
abstract class AddressBook(implicit logging: Logging) {
  type TransportId = (ActivationId, String)
  // def postWait(activationId: ActivationId, request: TransportRequest) : TransportAddress
}

// client for proxy nodes
class ActorProxyAddressBook(override val proxy: ProxyNode)(implicit logging: Logging)
  extends AddressBook with ProxyClient[(ActorRef, TransportRequest)] {
  val pendingRequests = mutable.Map.empty[ProxyAddress, ProxyAddress]
  val pendingData = mutable.Map.empty[ProxyAddress, ((ActorRef, TransportRequest), TransportAddress)]

  override def proxyReceive(sender: ProxyAddressBase, message: Serializable,
                            messageId: (ActorRef, TransportRequest)) = {

    val (actor, request) = messageId
    actor ! LibdTransportConfig(request.activationId, request, message.asInstanceOf[TransportAddress])
  }

  def requestAddress(request: TransportRequest) = ProxyAddress(request.activationId, request.name)

  /* useless for a reply: reply do not need dest address */
  def prepareReply(src: ProxyAddress, dst: ProxyAddress) =
    pendingData.remove(src) match {
      case Some((tag, info)) => postSendRecv(src, dst.masked(m3=false), tag, info)
      case None => pendingRequests += src -> dst
    }

  /**
   * After getting a communication channel, register a send and recv
   * @param actor
   * @param src
   * @param info
   */
  def finishReply(actor: ActorRef, src: ProxyAddress, info: TransportAddress) = {
    val request = TransportRequest.config(src.transport,"rdma_uverbs_proxy",src.aid)
    pendingRequests.get(src) match {
      case Some(dst) =>
        postSendRecv(src, dst.masked(m3 = false), (actor, request), info)
      case None =>
        pendingData += src -> ((actor, request), info)
    }
  }

  /**
   * Remove from address book, and post a release message
   * @param src
   */
  def release(src: ProxyAddress) = {
    pendingRequests.remove(src).foreach { dst =>
      val releaseAddr = dst.copy(transport = s"${dst.transport}@release")
                           .masked(m3 = false)
      proxy.postSend(src, releaseAddr, MemoryPoolEnd())
    }
  }
  def releaseAll(aid: ActivationId) = {
    logging.info(this, s"Release called for aid ${aid}")
    pendingRequests
      .filterKeys(_.aid == aid)
      .foreach { case (src, _) => release(src) }
  }

}

class CorunningAddressBook(pool: ContainerPool)(implicit logging: Logging) extends AddressBook {
  val preparedSignals : mutable.Map[ActorRef, (ActivationId, String, TransportAddress)] = mutable.Map.empty
  val addressBook : mutable.Map[TransportId, Either[Seq[TransportRequest], TransportAddress]] = mutable.Map.empty

  def postWait(activationId: ActivationId, request: TransportRequest) : TransportAddress = {
    val transId = (activationId, request.name)
    logging.warn(this, s"registering ${activationId}, $request")

    val newEntity = addressBook.getOrElse(transId, Left(Seq.empty))
                               .leftMap(_ :+ request)

    addressBook.update(transId, newEntity)
    // return basic transport for new requests
    request.address + newEntity.toOption.getOrElse(TransportAddress.empty)
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

  def remove(activationId: ActivationId): Unit = {
    addressBook.retain { case ((id, _), _) => id != activationId }
  }

}
