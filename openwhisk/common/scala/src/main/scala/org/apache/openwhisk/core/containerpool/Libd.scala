package org.apache.openwhisk.core.containerpool

import akka.http.scaladsl.model.HttpMethods
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.entity.ActivationId

import spray.json._

trait LibdAPIs[T <: Container] {

  self : T =>

  def addAction(serverUrl: String,
                actionName: String,
                activationId: ActivationId,
                transports: Option[Seq[String]]
               )(implicit transactionId: TransactionId) = {

    val body = JsObject(
      "server_url" -> JsString(serverUrl),
      "name" -> JsString(actionName),
      "activation_id" -> activationId.toJson
    )

    callLibd(HttpMethods.POST, body, resourcePath = s"action/${activationId.toString}")
  }

  def addTransport(activationId: ActivationId, transport : String)(implicit transactionId: TransactionId) = {
    val body = JsObject("transport" -> JsString(transport))
    callLibd(HttpMethods.POST, body, resourcePath = s"action/${activationId.toString}/transport")
  }

}