package org.apache.openwhisk.core.containerpool

import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.handler.codec.serialization.{ClassResolvers, ObjectDecoder, ObjectEncoder}

import scala.collection.mutable

abstract class ProxyAddress(val instance: String, val element: String, val parallelism: Int, val port: Int = 0) {
  def masked = ProxyAddressMasked(instance,element,parallelism,port)

}

// defines the proxy network address
case class ProxyAddressStandard(override val instance: String, override val element: String, override val parallelism: Int, override val port: Int = 0) extends ProxyAddress(instance, element, parallelism, port) {
  def port(port: Int) = copy(port = port)
  def of(element: String, parallelism: Int) = copy(element = element, parallelism = parallelism)
  def of(parallelism : Int) = copy(parallelism = parallelism)
}

case class ProxyMessage(src: ProxyAddress, dst: ProxyAddressMasked, message: Array[Byte])

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
  def port(port: Int) = copy(port = port)
  def of(element: String, parallelism: Int) = copy(element = element, parallelism = parallelism)
  def of(parallelism : Int) = copy(parallelism = parallelism)

}

trait ProxyClient[T] {
  val proxy: ProxyNode
  // interface for receiving
  def proxyReceive(sender: ProxyAddress, message: Array[Byte], messageId: T)
}

class ProxyNode(serverPort: Int,
                nodeTable: Map[String, (String, Int)], // name -> addr, port
                defaultRoute : String,
               ) {
  // Function, Instance, Element, Parallelism
  type ProxyNodeAddr = String
  type Message = Array[Byte]

  trait MessagePair { type Tag; val c: (ProxyClient[Tag], Tag, Option[Message]) }

  // clients
//  val clients = mutable.HashMap.empty[ProxyAddressMasked, ProxyClient[_]]
//
//  def addClient() = ???
//  def removeClient() = ???

  // Local posted but not routed
  // not used for now
  val sendOutBox = mutable.HashMap.empty[ProxyAddressMasked, Message]
  // Local Posted Recv
  val recvOutBox = mutable.HashMap.empty[ProxyAddress, MessagePair]
  // External Message -> local node
  val inBox = mutable.HashMap.empty[ProxyAddressMasked, (ProxyAddress, Message)]

  def postSend(src: ProxyAddress, dst: ProxyAddressMasked, message: Message) = {
    // check routeTable
    val proxyMsg = ProxyMessage(src, dst, message)
    route(dst)(proxyMsg)
  }
  def postRecv[T](addr: ProxyAddress, client: ProxyClient[T], id: T) = {
    // check inBox queue
    val mp = new MessagePair {
      override type Tag = T
      override val c = (client, id, None)
    }
    doRecv(addr, mp)
  }
  // post a recv and auto send
  def postReply[T](addr: ProxyAddress, client: ProxyClient[T], id: T, message: Message) = {
    val mp = new MessagePair {
      override type Tag = T
      override val c = (client, id, Some(message))
    }
    doRecv(addr, mp)
      .foreach { case (a, m) => postSend(addr, a.masked, m) }
  }

  // return: if success recv, return value and sender address
  def doRecv(addr: ProxyAddress, pair : MessagePair): Option[(ProxyAddress, Message)] =
    inBox.find ( _._1.maskMatch(addr) )
         .map(_._2)
         .orElse {
           // post recv
           recvOutBox += addr -> pair
           None
         }


  def inBound(src: ProxyAddress, dest: ProxyAddressMasked, message: Message): Unit = {
    recvOutBox
      .find { case (k,_) => dest.maskMatch(k) }
    match {
        // Match a local receive box
      case Some((k, mp)) =>
        val (client, id, reply) = mp.c
        reply match {
          case Some(msg) => postSend(src = k, dst = src.masked, msg)
          case None => client.proxyReceive(dest, message, id)
        }
        recvOutBox.remove(k)
      case None => inBox += dest -> (src, message)
    }
  }

  // routing
  var routingTable = List.empty[(ProxyAddressMasked, String)]
  def prependRoute(rule: ProxyAddressMasked, destination: String): Unit =
    routingTable = (rule, destination) :: routingTable
  def remoteRoutes(rule: ProxyAddressMasked): Unit =
    routingTable = routingTable.filterNot { case (a, _) => rule.maskMatch(a) }

  // interface function for operating on a request. routing
  var clientChannels = Map.empty[String, Channel]
  // server channel for "close" function
  var serverChannel = Option.empty[Channel]

  def route(dst: ProxyAddress)(msg: ProxyMessage): Unit = {
    routingTable
      .find { case (mask, _) => mask.maskMatch(dst) }
      .map(_._2)
      .getOrElse(defaultRoute) match {
        case "local" => inBound(msg.src, msg.dst, msg.message)
        case dest    => clientChannels.get(dest).foreach(_.writeAndFlush(msg))
      }
  }

  // Interfaces
  def serve = {
    // create connections with invokers
    // listen on local port

    // thread 2. listen on invokers, storing them to a list
    val bs = new ServerBootstrap()
    val group = new NioEventLoopGroup()

    bs.group(group)
      .channel(classOf[NioServerSocketChannel])
      .handler(new ChannelInitializer[NioServerSocketChannel] {
        override def initChannel(ch: NioServerSocketChannel): Unit = {
          val pipeline = ch.pipeline()

          pipeline.addLast(new ObjectEncoder())
          pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)))
          pipeline.addLast(new SimpleChannelInboundHandler[ProxyMessage]() {
            override def channelRead0(ctx: ChannelHandlerContext, msg: ProxyMessage): Unit =
              route(msg.dst)(msg)
          })
        }
      })

    serverChannel = Option(bs.bind(serverPort).sync().channel())

    // start network clients
    val clientBs = new Bootstrap()
    val clientGroup = new NioEventLoopGroup()

    clientBs.group(clientGroup)
            .channel(classOf[NioSocketChannel])
            .handler(new ChannelInitializer[NioSocketChannel] {
              override def initChannel(ch: NioSocketChannel): Unit = {
                val pipeline = ch.pipeline()

                pipeline.addLast(new ObjectEncoder())
                pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)))
              }
            })

    // call .channel will block
    clientChannels = nodeTable.mapValues { case (addr, port) => clientBs.connect(addr, port).channel() }
  }

  def close = {
    serverChannel.foreach(_.closeFuture().sync())
    clientChannels.foreach(_._2.closeFuture().sync())
  }
}
