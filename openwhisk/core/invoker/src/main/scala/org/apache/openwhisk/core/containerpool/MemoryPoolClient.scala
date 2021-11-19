package org.apache.openwhisk.core.containerpool


import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.epoll.{EpollDomainSocketChannel, EpollEventLoopGroup}
import io.netty.channel.unix.{DomainSocketAddress, UnixChannel}
import io.netty.channel._
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToByteEncoder}
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity.ActivationId

import java.util
import java.util.Base64
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

case class MemoryPoolEnd()

// Handler for
// (Int, Int) -> MessageId, ConnID
class MemoryPoolClient(val proxy: ProxyNode) extends ProxyClient[(Int, Int)] {
  // for async operations
  type RKey = Array[Byte]

  type ElementId = Int
  type ConnectionId = Int

  var ch : Channel = null
  def setChannel(channel: Channel) = ch = channel

  // increasing counter
  var currentElementId : Int = 0
  def newElementId: Int = {
    currentElementId += 1
    currentElementId
  }

  // -> (id, referenceCount)
  val activationIdMap = mutable.Map.empty[Int, (ActivationId, String, Int)]

  // For Async request, do not use for now
  val cmplQueue : mutable.Queue[MPSelectMsg] = mutable.Queue.empty

  // C library APIs
  def alloc(aid: ActivationId, transport: String, size: Long, preAllocatedConnections : Int = 1) = {
    val elementId = newElementId
    activationIdMap += elementId -> (aid, transport, preAllocatedConnections)

    val req = MPSelectMsg(op_code = MPOpCode.ALLOC, size = size,
      id = elementId, conn_id = preAllocatedConnections)
    ch.writeAndFlush(req)
    elementId
  }
  def free() = ???
  def extend() = ???
  def open(rKey: RKey, elementId: Int, connId : Int) = {
    // insert the command
    // just error if not exist
    val req = MPSelectMsg(op_code = MPOpCode.OPEN, status = rKey.length,
      id = elementId, conn_id = connId, msg = Option(rKey))
    ch.writeAndFlush(req)
  }
  def close(elementId: Int) = {
    val req = MPSelectMsg(op_code = MPOpCode.CLOSE, id = elementId);
    ch.writeAndFlush(req)
  }

  def messageHandler = new SimpleChannelInboundHandler[MPSelectMsg]() {
    override def channelRead0(ctx: ChannelHandlerContext, msg: MPSelectMsg): Unit = msg.op_code match {
      case MPOpCode.ALLOC =>
        // get Local key, add to proxy network; if match, send again
        msg.msg.foreach { bytes =>
          val peerInfo = Base64.getEncoder.encodeToString(bytes)
          val message = TransportAddress.ProxyTransport(peerInfo)
          val (aid, trans, _) = activationIdMap.get(msg.id).get
          postReply(ProxyAddress(aid, trans), (msg.id, msg.conn_id), message)
        }
      case MPOpCode.OPEN  =>
        // OPEN request will always carry the new info, do not request
      case _              =>
        // just check if is success
    }
  }

  // Means that some one has shared the message
  override def proxyReceive(sender: ProxyAddressBase,
                            message: Serializable,
                            messageId: (Int, Int)): Unit = {
    val (elementId, connId) = messageId

    message match {
      case m: TransportAddress =>
        val peerInfo = m.asInstanceOf[TransportAddress].config.get("peerinfo").get
        open(Base64.getDecoder.decode(peerInfo), elementId, connId)
      case _: MemoryPoolEnd =>
        release(elementId)
    }


  }

  // Public APIs
  // initialization and Run
  def initRun(r: ActivationMessage) = {
    val parallelism = r.siblings.size
    // allocate
    // TODO: hardcode for now
    val trans = "memory"
    // post release hooks
    val elementId = alloc(r.activationId, trans, parallelism)
    val releaseTrans = s"${trans}@release"
    (0 until parallelism).map {connId =>
      val address = ProxyAddress(aid = r.activationId, transport = releaseTrans, port = connId)
      postRecv(address, (elementId, connId))
    }

  }

  def release(elementId: Int) = {
    activationIdMap
      .get(elementId)
      .foreach { case t@(_, _, references) =>
        val newRef = references - 1
        activationIdMap.update(elementId, t.copy(_3 = newRef))
        if (newRef == 0)
          close(elementId)
      }
  }

}

class MemoryPoolEndPoint(socketFile : String, proxy: ProxyNode) {
  val bs = new Bootstrap()
  val group = new EpollEventLoopGroup()

  val client = new MemoryPoolClient(proxy)

  bs.group(group)
    .channel(classOf[EpollDomainSocketChannel])
    .handler(new MemoryPoolClientHandler(client))

  val launch = bs
    .connect(new DomainSocketAddress(socketFile))
    .channel()

  client.setChannel(launch)
}
