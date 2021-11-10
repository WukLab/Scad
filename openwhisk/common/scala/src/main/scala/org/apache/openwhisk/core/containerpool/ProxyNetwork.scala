package org.apache.openwhisk.core.containerpool

import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.handler.codec.serialization.{ClassResolvers, ObjectDecoder, ObjectEncoder}

import scala.collection.mutable


// defines the proxy network address
case class ProxyAddress(instance: String, element: String, parallelism: Int, port: Int = 0) {
  def port(port: Int) = copy(port = port)
  def of(element: String, parallelism: Int) = copy(element = element, parallelism = parallelism)
  def of(parallelism : Int) = copy(parallelism = parallelism)
}

case class ProxyMessage(dst: ProxyAddress)

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
  type Message = Array[Byte]

  // Client[Tag], Tag,
  type MessagePair = { type Tag; val c: (ProxyClient[Tag], Tag, Option[Message]) }

  val clients = mutable.HashMap.empty[ProxyAddressMasked, ProxyClient[_]]

  def addClient() = ???
  def removeClient() = ???

  // Local posted but not routed
  val sendOutBox = mutable.HashMap.empty[ProxyAddressMasked, Message]
  // Local Posted Recv
  val recvOutBox = mutable.HashMap.empty[ProxyAddress, MessagePair]
  // External Message -> local node
  val inBox = mutable.HashMap.empty[ProxyAddressMasked, Message]

  def postSend(dst: ProxyAddress, message: Message) = {
    // check routeTable
    val proxyMsg = ProxyMessage(dst)
    route(dst)(proxyMsg)
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

  // interface function for operating on a request. routing

  type RouteHandler = ProxyMessage => Unit

  val nodeTable = Map.empty[String, ProxyNodeAddr]
  var channelTable = Map.empty[String, Channel]
  var serverChannel = Option.empty[Channel]

  def routeLocal(msg: ProxyMessage) =
    inBound(msg.dst, msg.dst, None)
  def routeForward(addr: ProxyNodeAddr)(msg: ProxyMessage) =
    channelTable.get(addr).foreach(_.writeAndFlush(msg))
  def route(dst: ProxyAddress): RouteHandler = {
    // lookup the routing table
    routeLocal()
  }

  // Interfaces
  def serve() = {
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

    val serverPort = 1234
    // start server channel
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
    channelTable = nodeTable.mapValues { clientBs.connect(_).channel() }
  }
}
