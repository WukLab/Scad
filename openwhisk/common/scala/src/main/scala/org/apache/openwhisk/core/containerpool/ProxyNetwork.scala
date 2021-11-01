package org.apache.openwhisk.core.containerpool

import scala.collection.mutable


// defines the proxy network address
case class ProxyAddress(instance: String, element: String, parallelism: Int, port: Int = 0) {
  def port(port: Int) = copy(port = port)
  def of(element: String, parallelism: Int) = copy(element = element, parallelism = parallelism)
  def of(parallelism : Int) = copy(parallelism = parallelism)
}

case class ProxyAddressMasked(
                            override val instance: String,
                            override val element: String,
                            override val parallelism: Int,
                            override val port: Int = 0,
                            mask : Vector[Boolean] = Vector.fill(4)(true)
                           ) extends ProxyAddress(instance, element, parallelism, port) {
  def maskMatch(other: ProxyAddress) = {
    val pred = Vector(
      instance == other.instance,
      element == other.element,
      parallelism == other.parallelism,
      port == other.port
    )

    // not (not pred && mask) => pred or not mask
    (pred, mask).zipped.forall(_ || !_)
  }

}

trait ProxyClient[T] {
  val proxy: ProxyNode
  // interface for receiving
  def proxyReceive(sender: ProxyAddress, message: Array[Byte], messageId: T)
}

// T is for sth like tags or seq numbers
class ProxyNode(localAddress: String) {
  // Function, Instance, Element, Parallelism
  type ProxyNodeAddr = String

  type MessageId = Any
  type Message = Array[Byte]

  type MessagePair = { type T; val c: (ProxyClient[T], T, Option[Message]) }

  val clients = mutable.HashMap.empty[ProxyAddressMasked, ProxyClient[_]]

  def addClient() = ???
  def removeClient() = ???

  // Local posted but not routed
  val sendOutBox = mutable.HashMap.empty[ProxyAddressMasked, Message]
  // Local Posted Recv
  val recvOutBox = mutable.HashMap.empty[ProxyAddress, MessagePair]
  // External Message -> local node
  val inBox = mutable.HashMap.empty[ProxyAddressMasked, Message]

  def postSend(dest: ProxyAddress, message: Message) = {
    // check routeTable

  }
  def postRecv[T](addr: ProxyAddress, client: ProxyClient[T], id: T) = {
    // check inBox queue
    val mp = new {type T = T, val c = (client, id, None)}

  }
  def inBound(src: ProxyAddress, dest: ProxyAddressMasked, message: Message): Unit = {
    recvOutBox
      .find { case (k,_) => dest.maskMatch(k) }
    match {
      case Some((k, mp)) =>
        val (client, id, reply) = mp.c
        reply match {
          case Some(msg) => postSend(src, msg)
          case None => client.proxyReceive(dest, message, id)
        }
        recvOutBox.remove(k)
      case None => inBox += dest -> message
    }
  }

  // post a recv and auto send
  def postReply() = ???

  // routing table
  val routeTable = mutable.HashMap.empty[ProxyAddress,ProxyNodeAddr]

  def addRoute(proxyAddr: ProxyAddress, proxyNodeAddr: ProxyNodeAddr) = {
    // check SendQueue
  }
  def removeRoute(proxyAddr: ProxyAddress, mask : Boolean = false) = {

  }
}
