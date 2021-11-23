package org.apache.openwhisk.core.controller.test

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import spray.json.DefaultJsonProtocol._
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.containerpool.RuntimeResources
import org.apache.openwhisk.core.swap.SwapObject
import org.apache.openwhisk.core.controller.SwapApi
import org.apache.openwhisk.core.database.UserContext
import org.apache.openwhisk.core.entity.{ActivationId, ByteSize, ControllerInstanceId, EntityPath, Identity, InvokerInstanceId, TopSchedInstanceId}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SwapApiTests extends ControllerTestCommon with SwapApi {

  behavior of "Swap API"

  val creds: Identity = WhiskAuthHelpers.newIdentity()
  val context: UserContext = UserContext(creds)
  val namespace: EntityPath = EntityPath(creds.subject.asString)
  val basePath = s"/${EntityPath.DEFAULT}/${collection.path}"

  override protected val topsched: ControllerInstanceId = new TopSchedInstanceId("0")

  //// PUT /swap
  it should "return empty list when no actions exist" in {
    implicit val tid: TransactionId = transid()

    val swap = SwapObject("test/action", InvokerInstanceId(0, None, None, RuntimeResources.none()), ActivationId.generate(), ActivationId.generate(), ByteSize.fromString("512M"))
    val content = SwapObject.serdes.write(swap).asJsObject

    Put(basePath + "/aaaa", content) ~> Route.seal(routes(creds)) ~> check {
      status should be(OK)
      //      responseAs[List[JsObject]] shouldBe 'empty
    }
  }
}
