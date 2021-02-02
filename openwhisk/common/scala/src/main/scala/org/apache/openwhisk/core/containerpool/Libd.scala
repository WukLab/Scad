package org.apache.openwhisk.core.containerpool

import akka.http.scaladsl.model.HttpMethods
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.entity.ActivationId

import spray.json._
import DefaultJsonProtocol._

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

  def addTransport(activationId: ActivationId, transport : String)(implicit transactionId: TransactionId) = {
    val body = JsObject("transport" -> JsString(transport))
    callLibd(HttpMethods.POST, body, resourcePath = s"action/${activationId.toString}/transport")
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
    def getName(url : String): String = url.split(';')(0)
    def getPort(url : String): Int = 2333
    def needWait(url : String) : Boolean = true
    def needSignal(url : String) : Boolean = true

  }

  def configTransport(transport : String, config: Map[String,String]) =
    transport + config.map { case (k, v) => s"$k:$v;" }.mkString("")

}