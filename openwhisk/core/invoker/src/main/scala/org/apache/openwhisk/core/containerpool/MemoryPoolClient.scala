package org.apache.openwhisk.core.containerpool


import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.epoll.{EpollDomainSocketChannel, EpollEventLoopGroup}
import io.netty.channel.unix.{DomainSocketAddress, UnixChannel}
import io.netty.channel._
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToByteEncoder}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity._

import java.util
import java.util.Base64
import scala.collection.mutable


case class MPSelectMsg(size: Long = 0, op_code: Int = 0, msg_size: Int = 0,
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

      val size = in.readLongLE()
      val op_code = in.readUnsignedShortLE()

      val msg_size = in.readShortLE()
      val id = in.readShortLE()
      val conn_id = in.readShortLE()

      val bytes = if (msg_size > 0) { // msg size is true
        val bytes = new Array[Byte](msg_size)
        in.readBytes(bytes)
        Option(bytes)
      } else None

      val msg = MPSelectMsg(size, op_code, msg_size, id, conn_id, bytes)
      out.add(msg)
    }
  }

  def encoder = new MessageToByteEncoder[MPSelectMsg]() {
    override def encode(ctx: ChannelHandlerContext, msg: MPSelectMsg, out: ByteBuf): Unit = {
      out.writeLongLE(msg.size)
      out.writeShortLE(msg.op_code)
      out.writeShortLE(msg.msg_size)
      out.writeShortLE(msg.id)
      out.writeShortLE(msg.conn_id)

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
class MemoryPoolClient(val proxy: ProxyNode)(implicit logging: Logging) extends ProxyClient[(Int, Int)] {
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

    logging.debug(this, s"[MPT] allocting id: ${elementId} -> ($aid, $transport, $preAllocatedConnections)")

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
    val req = MPSelectMsg(op_code = MPOpCode.OPEN, msg_size = rKey.length,
      id = elementId, conn_id = connId, msg = Option(rKey))
    ch.writeAndFlush(req)
  }
  def close(elementId: Int) = {
    val req = MPSelectMsg(op_code = MPOpCode.CLOSE, id = elementId);
    ch.writeAndFlush(req)
    // TODO: send reply to scheduler?
  }

  def messageHandler = new SimpleChannelInboundHandler[MPSelectMsg]() {
    override def channelRead0(ctx: ChannelHandlerContext, msg: MPSelectMsg): Unit = msg.op_code match {
      case MPOpCode.ALLOC =>
        logging.debug(this, s"[MPT] Getting ALLOC reply ${msg}")
        // get Local key, add to proxy network; if match, send again
        msg.msg.foreach { bytes =>
          val peerInfo = Base64.getEncoder.encodeToString(bytes)
          val message = TransportAddress.ProxyTransport(peerInfo)
          logging.debug(this, s"[MPT] Encoded Message $peerInfo")
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

  // Public APIs
  // initialization and Run
  def initRun(activationId: ActivationId, trans: String, size: Long, parallelism: Int) = {
    // alloc
    val elementId = alloc(activationId, trans, size, parallelism)
    // post release hooks
    val releaseTrans = s"${trans}@release"
    (0 until parallelism).map {connId =>
      val address = ProxyAddress(aid = activationId, transport = releaseTrans, port = connId)
      postRecv(address, (elementId, connId))
    }

  }

}

class MemoryPoolEndPoint(socketFile : String, proxy: ProxyNode)(implicit logging: Logging) {
  val bs = new Bootstrap()
  val group = new EpollEventLoopGroup()

  val client = new MemoryPoolClient(proxy)

  bs.group(group)
    .channel(classOf[EpollDomainSocketChannel])
    .handler(new MemoryPoolClientHandler(client))

  val launch = bs
    .connect(new DomainSocketAddress(socketFile))
    .sync()
    .channel()

  client.setChannel(launch)

  // APIs
  def initRun(activationId: ActivationId, transportName: String, size: Long, parallelism: Int) =
    client.initRun(activationId, transportName, size, parallelism)

  // Hardcode for now
  def initRun(r: Run) =
    client.initRun(r.msg.activationId, "memory", LibdAPIs.Action.getSize(r.action), r.msg.siblings.size)

  def initRunTest(parallelism: Int) = {
    val aid = ActivationId.generate()
    initRun(aid, "memory", 1024*1024*64, parallelism)
    aid
  }
}
