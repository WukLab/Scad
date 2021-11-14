package org.apache.openwhisk.core.containerpool


import java.util

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.epoll.{EpollDomainSocketChannel, EpollEventLoopGroup}
import io.netty.channel.unix.{DomainSocketAddress, UnixChannel}
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToByteEncoder}

import scala.collection.mutable


case class MPSelectMsg(size: Long = 0, op_code: Int = 0, status: Int = 0,
                       id: Int = 0, conn_id: Int = 0, msg: Option[Array[Byte]] = None)

object MPOpCode {
  val ALLOC = 0x0
  val FREE = 0x1
  val EXTEND = 0x2
  val OPEN = 0x10
  val CLOSE = 0x11
}

object MPSelectMsg {
  def decoder = new ByteToMessageDecoder {
    override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {

      val size = in.readLong()
      val op_code = in.readUnsignedShort()

      val status = in.readShort()
      val id = in.readShort()
      val conn_id = in.readShort()

      val bytes = if (status > 0) { // msg size is true
        val bytes = new Array[Byte](status)
        in.getBytes(in.readerIndex(), bytes)
        Option(bytes)
      } else None

      val msg = MPSelectMsg(size, op_code, status, id, conn_id, bytes)
      out.add(msg)
    }
  }

  def encoder = new MessageToByteEncoder[MPSelectMsg]() {
    override def encode(ctx: ChannelHandlerContext, msg: MPSelectMsg, out: ByteBuf): Unit = {
      out.writeLong(msg.size)
      out.writeShort(msg.op_code)
      out.writeShort(msg.status)
      out.writeShort(msg.id)
      out.writeShort(msg.conn_id)

      msg.msg.foreach (out.writeBytes(_))
    }
  }

}

class MemoryPoolClientHandler(client: MemoryPoolClient) extends ChannelInitializer[UnixChannel] {
  override def initChannel(ch: UnixChannel): Unit = {
    val pipeline = ch.pipeline()

    pipeline.addLast("decode-msg", MPSelectMsg.decoder)
    pipeline.addLast("encode-msg", MPSelectMsg.encoder)
    pipeline.addLast("handle-resp", client.messageHandler)

  }
}

// Handler for
class MemoryPoolClient()(ch: UnixChannel) extends ProxyClient[(Int, Int)] {

  // for async operations
  type Direction = Boolean
  type RKey = Array[Byte]
  val InBound = true
  val OutBound = false
  type ConnectionInfo = (Direction, RKey)

  type ElementId = Int
  type ConnectionId = Int

  val infos : mutable.Map[ElementId, mutable.Map[ConnectionId, ConnectionInfo]] =
    mutable.HashMap.empty

  val cmplQueue : mutable.Queue[MPSelectMsg] = mutable.Queue.empty

  val elementMap = mutable.HashMap.empty[String, Int]
  val elementQueue = mutable.Queue.empty[String]

  // actions
  def alloc(element: String, size: Long, preAllocatedConnections : Int = 1) = {
    val req = MPSelectMsg(op_code = MPOpCode.ALLOC, size = size,
      id = elementMap.getOrElse(element, -1), conn_id = preAllocatedConnections)
    ch.writeAndFlush(req)
  }
  def free() = ???
  def extend() = ???
  def open(rKey: RKey, element: String, connId : Int = -1) = {
    // just error if not exist
    val id = elementMap.get(element).get
    open(rKey, id, connId)
  }
  def open(rKey: RKey, elementId: Int, connId : Int = -1) = {
    // just error if not exist
    val req = MPSelectMsg(op_code = MPOpCode.OPEN, status = rKey.length,
      id = elementId, conn_id = connId, msg = Option(rKey))
    ch.writeAndFlush(req)
  }
  def close() = ???

  def messageHandler = new SimpleChannelInboundHandler[MPSelectMsg]() {
    override def channelRead0(ctx: ChannelHandlerContext, msg: MPSelectMsg): Unit = msg.op_code match {
      case MPOpCode.ALLOC =>
        // get Local key, add to proxy network; if match, send again
        msg.msg.foreach { proxyReceive((msg.id, msg.conn_id), _) }
      case MPOpCode.OPEN  =>
        // get remote key, remove the entry from proxy
      case _              =>
        // just check if is success
    }
  }

  override def proxyReceive(sender: ProxyAddress, message: Array[Byte], messageId: (Int, Int)) = {

  }

//  override val proxy = _
}

class MemoryPoolEndPoint(socketFile : String, client: MemoryPoolClient) {
  val bs = new Bootstrap()
  val group = new EpollEventLoopGroup()

  bs.group(group)
    .channel(classOf[EpollDomainSocketChannel])
    .handler(new MemoryPoolClientHandler(client))

  val launch = bs
    .connect(new DomainSocketAddress(socketFile))
    .channel()
    .closeFuture()
    .sync()
}
