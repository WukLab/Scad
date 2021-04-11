package org.apache.openwhisk.core.containerpool

import akka.http.scaladsl.model.HttpMethods
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.entity.{ActivationId, WhiskActionLike}
import spray.json._
import DefaultJsonProtocol._
import org.apache.openwhisk.core.connector.RunningActivation

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

}

object LibdAPIs {

  object Action {
    def mix(env: Map[String, JsValue])
                  (serverUrl: String,
                   actionName: String,
                   transports: Option[Seq[String]]): Map[String, JsValue] = {
      env ++ Map(
        "server_url" -> JsString(serverUrl),
        "name"       -> JsString(actionName),
        "transports" -> transports.toJson
      )
    }
  }

  object Transport {

    // This function should select a port for listening, currently is empty
    def getDefaultTransport(action : WhiskActionLike) : Option[Seq[String]] =
      action.runtimeType
        .flatMap {
          case "memory" =>
            // TODO: change those to parameters
            val name = "memory"
            val impl = "rdma_tcp_server"
            val port = 2333
            val size = action.limits.resources.limits.mem.toBytes
            Some(Seq(s"${name};${impl};url,tcp://*:${port};size,${size};"))
          case _        => None
        }

    def getName(ra: RunningActivation): String = "memory"
    def getImpl(ra: RunningActivation, runtimeType: String): String = runtimeType match {
      case "memory" => s"rdma_${ra.transportImpl.toLowerCase()}_server"
      case _        => s"rdma_${ra.transportImpl.toLowerCase()}"
    }

    def getPort(ra: RunningActivation): Int = 2333
    def needWait(runtimeType : String) : Boolean = runtimeType match {
      case "memory" => false
      case _        => true
    }
    def needSignal(runtimeType : String) : Boolean = runtimeType match {
      case "memory" => true
      case _        => false
    }

  }

  def configTransport(transport : String, config: Map[String,String]) =
    transport + config.map { case (k, v) => s"$k:$v;" }.mkString("")

}