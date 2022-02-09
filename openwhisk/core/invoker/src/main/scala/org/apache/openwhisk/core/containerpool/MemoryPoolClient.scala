package org.apache.openwhisk.core.containerpool


import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.epoll.{EpollDomainSocketChannel, EpollEventLoopGroup}
import io.netty.channel.unix.{DomainSocketAddress, UnixChannel}
import io.netty.channel._
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToByteEncoder}
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.ack.MessagingActiveAck
import org.apache.openwhisk.core.connector.{ActivationMessage, ResultMessage}
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
class MemoryPoolClient(val proxy: ProxyNode, acker: MessagingActiveAck)(implicit logging: Logging) extends ProxyClient[(Int, Int)] {
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
  val activationIdMap = mutable.Map.empty[Int, (ActivationMessage, String, Int, TransactionId)]

  // For Async request, do not use for now
  val cmplQueue : mutable.Queue[MPSelectMsg] = mutable.Queue.empty

  // C library APIs
  def alloc(act: ActivationMessage, transport: String, size: Long, preAllocatedConnections : Int = 1)(implicit transid: TransactionId) = {
    val elementId = newElementId
    activationIdMap += elementId -> (act, transport, preAllocatedConnections, transid)

    logging.debug(this, s"[MPT] allocting id: ${elementId} -> (${act.activationId}, $transport, $preAllocatedConnections)")

    val req = MPSelectMsg(op_code = MPOpCode.ALLOC, size = size,
      id = elementId, conn_id = preAllocatedConnections)
    ch.writeAndFlush(req)
    elementId
  }
  def allocConn(elementId: Int, numConns: Int = 1) = {
    val req = MPSelectMsg(op_code = MPOpCode.ALLOC, size = 0,
      id = elementId, conn_id = numConns)
    ch.writeAndFlush(req)
    elementId
  }
  def extend() = ???
  def open(rKey: RKey, elementId: Int, connId : Int) = {
    // insert the command
    // just error if not exist
    val req = MPSelectMsg(op_code = MPOpCode.OPEN, msg_size = rKey.length,
      id = elementId, conn_id = connId, msg = Option(rKey))
    ch.writeAndFlush(req)
  }
  def free(elementId: Int) = {
    activationIdMap.get(elementId) match {
      case Some((act, _, _, transid)) =>
        val activation = WhiskActivation(
          act.action.path,
          act.action.name,
          act.user.subject,
          act.activationId,
          java.time.Instant.now(),
          java.time.Instant.now(),
          cause = act.cause,
          ActivationResponse.success(),
        )
        val ack = ResultMessage(transid, activation)
        acker(transid, activation, blockingInvoke = false, act.rootControllerIndex, act.user.namespace.uuid, ack)
      case None => ???

    }
    activationIdMap -= elementId
    val req = MPSelectMsg(op_code = MPOpCode.FREE, id = elementId);
    logging.info(this, s"[MPT] Sending Ack to racksched.. CLOSE $req")
    ch.writeAndFlush(req)
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
          val (act, trans, _, _) = activationIdMap.get(msg.id).get
          postReply(ProxyAddress(act.activationId, trans), (msg.id, msg.conn_id), message)
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
    logging.debug(this, s"[MPT] Getting Message $message, for $elementId.$connId")

    message match {
      case m: TransportAddress =>
        val peerInfo = m.config.get("peerinfo").get
        open(Base64.getDecoder.decode(peerInfo), elementId, connId)
        // TODO: current logic is we do allocate extra QPs (for extra connections)
        // TODO: check this. should be fine without parallelism
        allocConn(elementId)
      case _: MemoryPoolEnd =>
        release(elementId)
    }
  }

  def release(elementId: Int) = {
    activationIdMap
      .get(elementId)
      // Release References!
      .foreach { case t@(_, _, references, _) =>
        val newRef = references - 1
        activationIdMap.update(elementId, t.copy(_3 = newRef))
        if (newRef == 0)
          free(elementId)
      }
  }

  // Public APIs
  // initialization and Run
  def initRun(act: ActivationMessage, trans: String, size: Long, parallelism: Int)(implicit transid: TransactionId) = {
    // TODO: check this!
    val numConns = if (parallelism > 0) parallelism else 1
    // alloc
    val elementId = alloc(act, trans, size, numConns)
    // post release hooks
    val releaseTrans = s"${trans}@release"
    (0 until numConns).map {connId =>
      val address = ProxyAddress(aid = act.activationId, transport = releaseTrans, port = connId)
      postRecv(address, (elementId, connId))
    }

  }

}

class MemoryPoolEndPoint(socketFile : String, proxy: ProxyNode, acker: MessagingActiveAck)(implicit logging: Logging) {
  val bs = new Bootstrap()
  val group = new EpollEventLoopGroup()

  val client = new MemoryPoolClient(proxy, acker)

  bs.group(group)
    .channel(classOf[EpollDomainSocketChannel])
    .handler(new MemoryPoolClientHandler(client))

  val launch = bs
    .connect(new DomainSocketAddress(socketFile))
    .sync()
    .channel()

  client.setChannel(launch)

  // APIs
  def initRun(act: ActivationMessage, transportName: String, size: Long, parallelism: Int)(implicit transid: TransactionId) =
    client.initRun(act, transportName, size, parallelism)

  // Hardcode for now
  def initRun(r: Run)(implicit transid: TransactionId) =
    client.initRun(r.msg, "memory", LibdAPIs.Action.getSize(r.msg, r.action), r.msg.siblings.map(_.size).getOrElse(1))

  def initRunTest(parallelism: Int)(implicit transid: TransactionId) = {
    val aid = ActivationId.generate()
//    initRun(aid, "memory", 1024*1024*64, parallelism)
    aid
  }
}
