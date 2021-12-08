package org.apache.openwhisk.core.containerpool

import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.handler.codec.serialization.{ClassResolvers, ObjectDecoder, ObjectEncoder}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity.ActivationId

import scala.collection.mutable

abstract class ProxyAddressBase {
  val aid: ActivationId
  val transport: String
  val port: Int
}

// defines the proxy network address
case class ProxyAddress(aid: ActivationId, transport: String, port: Int = 0) extends ProxyAddressBase {
  def port(port: Int) = copy(port = port)
  def masked(m1: Boolean = true, m2: Boolean = true, m3: Boolean = true) =
    ProxyAddressMasked(aid, transport, port, Vector(m1,m2,m3))
}

case class ProxyAddressMasked(aid: ActivationId, transport: String, port: Int = 0,
                              mask : Vector[Boolean] = Vector.fill(3)(true)) extends ProxyAddressBase {
  def maskMatch(other: ProxyAddressBase) = {
    val pred = Vector(
      aid == other.aid,
      transport == other.transport,
      port == other.port)

    // not (not pred && mask) => pred or not mask
    (pred, mask).zipped.forall(_ || !_)
  }
}

case class ProxyMessage(src: ProxyAddress, dst: ProxyAddressMasked, message: Serializable)

trait ProxyClient[T] {
  val proxy: ProxyNode
  // interface for receiving
  def proxyReceive(sender: ProxyAddressBase, message: Serializable, messageId: T)
  def postSendRecv(src: ProxyAddress, dst: ProxyAddressMasked, id: T, message: Serializable) =
    proxy.postSendRecv(src, dst, this, id, message)
  def postReply(addr: ProxyAddress, id: T, message: Serializable) =
    proxy.postReply(addr, this, id, message)
  def postRecv(addr: ProxyAddress, id: T) =
    proxy.postRecv(addr, this, id)
}

class ProxyNode(serverPort: Int,
                nodeTable: Map[String, (String, Int)], // name -> addr, port
                defaultRoute : String,
               )(implicit logging: Logging) {
  // Function, Instance, Element, Parallelism
  type Message = Serializable

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

  // TODO: insert when call those "post" functions
  def postSend(src: ProxyAddress, dst: ProxyAddressMasked, message: Message) = {
    logging.debug(this, s"[MPT] send $message: $src -> $dst")
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
  def postSendRecv[T](src: ProxyAddress, dst: ProxyAddressMasked,
                      client: ProxyClient[T], id: T, message: Message) = {
    val mp = new MessagePair {
      override type Tag = T
      override val c = (client, id, Some(message))
    }
    postRecv(src, client, id)
    postSend(src, dst, message)
  }
  def postRecvSend[T](addr: ProxyAddress, client: ProxyClient[T], id: T, message: Message) =
    postReply(addr, client, id, message)
  def postReply[T](addr: ProxyAddress, client: ProxyClient[T], id: T, message: Message) = {
    val mp = new MessagePair {
      override type Tag = T
      override val c = (client, id, Some(message))
    }

    // doRecv will trigger callback
    doRecv(addr, mp)
      .foreach { case (a, _) => postSend(addr, a.masked(), message) }
  }

  // return: if success recv, return value and sender address
  def doRecv(addr: ProxyAddress, pair : MessagePair): Option[(ProxyAddress, Message)] = {
    // Store all addresses into local
    prependRoute(addr.masked(m3 = false), "local")
    val (client, id, _) = pair.c
    logging.debug(this, s"[MPT] recv from $addr <- ${pair.c}")
    inBox.find(_._1.maskMatch(addr))
         .map { case (_, p@(a, m)) =>
           client.proxyReceive(a, m, id)
           logging.debug(this, s"[MPT] recv fordward")
           p
         }
         .orElse {
           // post recv
           recvOutBox += addr -> pair
           logging.debug(this, s"[MPT] recv stashed")
           None
         }
  }


  def inBound(src: ProxyAddress, dest: ProxyAddressMasked, message: Message): Unit = {
    logging.debug(this, s"[MPT] inbound message $message: $src -> $dest")
    recvOutBox
      .find { case (k,_) => dest.maskMatch(k) }
    match {
        // Match a local receive box
      case Some((k, mp)) =>
        val (client, id, reply) = mp.c
        reply.foreach { msg =>
          postSend(src = k, dst = src.masked(), msg)
        }
        client.proxyReceive(dest, message, id)
        recvOutBox.remove(k)
        logging.debug(this, s"[MPT] inbound message forwarded")
      case None =>
        inBox += dest -> (src, message)
        logging.debug(this, s"[MPT] inbound message stashed")
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

  def route(dst: ProxyAddressBase)(msg: ProxyMessage): Unit = {
    logging.debug(this, s"[MPT] routing message $msg -> $dst")
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

    logging.debug(this, s"[MPT] Starting Server on port ${serverPort}")
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
    logging.debug(this, s"Connecting to tables")
  }

  def close = {
    serverChannel.foreach(_.closeFuture().sync())
    clientChannels.foreach(_._2.closeFuture().sync())
  }
}
