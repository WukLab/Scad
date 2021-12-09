package org.apache.openwhisk.core.containerpool

import akka.http.scaladsl.model.HttpMethods
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.entity.{ActivationId, ElementType, WhiskActionLike}
import spray.json._
import DefaultJsonProtocol._
import org.apache.openwhisk.core.connector.RunningActivation
import org.apache.openwhisk.core.entity.ElementType.ElementType

case class LibdMessagesReply(ok: Boolean, messages: Map[String, String])
object LibdMessagesReply extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[LibdMessagesReply] = jsonFormat2(LibdMessagesReply.apply)
}

trait LibdAPIs[T <: Container] {

  self : T =>

  def addAction(serverUrl: String,
                actionName: String,
                activationId: ActivationId,
                transports: Option[Seq[String]]
               )(implicit transactionId: TransactionId) = {

    val body = JsObject(
      "server_url"    -> JsString(serverUrl),
      "name"          -> JsString(actionName),
      "transports"    -> transports.toJson
    )

    callLibd(HttpMethods.POST, body, resourcePath = s"action/${activationId.toString}")
  }

  def addTransport(activationId: ActivationId, transport : String)
                  (implicit transactionId: TransactionId = TransactionId.unknown) = {
    val body = JsObject("transport" -> JsString(transport))
    callLibd(HttpMethods.POST, body, resourcePath = s"action/${activationId.toString}/transport")
  }

  def configTransport(activationId: ActivationId, transportName: String, transport : String
                     )(implicit transactionId: TransactionId = TransactionId.unknown) = {
    val body = JsObject(
      "name" -> JsString(transportName),
      "durl" -> JsString(transport))
    callLibd(HttpMethods.PUT, body, resourcePath = s"action/${activationId.toString}/transport/${transportName}")
  }

  def getMessages(activationId: ActivationId)
                 (implicit transactionId: TransactionId = TransactionId.unknown) = {
    val body = JsObject()
    callLibd(HttpMethods.POST, body, resourcePath = s"action/${activationId.toString}/messages")
  }

}

object LibdAPIs {

  object Action {
    def mix(env: Map[String, JsValue])
                  (serverUrl: String,
                   actionName: String,
                   transports: Option[Seq[String]],
                   profile: Option[String]
                  ): Map[String, JsValue] = {
      env ++ Map(
        "server_url" -> JsString(serverUrl),
        "name"       -> JsString(actionName),
        "transports" -> transports.getOrElse(Seq.empty).toJson,
      ) ++ profile.map(s => Map("profile" -> JsString(s))).getOrElse(Map.empty)
    }

    def getSize(actionLike: WhiskActionLike) = {
      actionLike.limits.resources.limits.mem.toBytes
    }
  }

  object Transport {

    // This function should select a port for listening, currently is empty
    def getDefaultTransport(action : WhiskActionLike, useRdma: Boolean) : Option[Seq[String]] =
      action.porusParams.runtimeType
        .flatMap {
          case ElementType.Memory =>
            // TODO: change those to parameters
            val name = "memory"
            val impl = if (useRdma) "rdma_uverbs_server" else "rdma_tcp_server"
            val port = 2333
            val size = action.limits.resources.limits.mem.toBytes
            Some(Seq(s"${name};${impl};url,tcp://*:${port};size,${size};"))
          case ElementType.Compute =>
            val mergedMem: Long = action.porusParams.withMerged.filter(p => p.elem.equals(ElementType.Memory)).map(_.resources.mem.toBytes).sum
            if (mergedMem > 0) {
              // TODO: multiple merged mems?
              Some(Seq(s"memory;rdma_local;url,RDMA_LOCAL;size,${action.limits.resources.limits.mem.toBytes + mergedMem};"))
            } else {
              None
            }
          case _        => None
        }

    def getName(ra: RunningActivation): String = ra.objName.split('/').last
    def getImpl(ra: RunningActivation, runtimeType: ElementType): String = runtimeType match {
      case ElementType.Memory => s"rdma_${ra.transportImpl.toLowerCase()}_server"
      case _                  => s"rdma_${ra.transportImpl.toLowerCase()}"
    }

    def getPort(ra: RunningActivation): Int = 2333
    def needWait(runtimeType : ElementType) : Boolean = runtimeType match {
      case ElementType.Memory => false
      case _                  => true
    }
    def needSignal(runtimeType : ElementType) : Boolean = runtimeType match {
      case ElementType.Memory => true
      case _                  => false
    }

  }

  def configTransport(transport : String, config: Map[String,String]) =
    transport + config.map { case (k, v) => s"$k:$v;" }.mkString("")

}
