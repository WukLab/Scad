package org.apache.openwhisk.core.entity.test

import org.apache.openwhisk.core.containerpool.RuntimeResources
import org.apache.openwhisk.core.entity.size.SizeInt
import org.apache.openwhisk.core.entity.{ControllerInstanceId, InstanceId, RackSchedInstanceId}
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner
import spray.json.{JsNumber, JsObject, JsString}

import scala.util.Success

@RunWith(classOf[JUnitRunner])
class RackSchedInstanceIdTests extends FlatSpec with Matchers {


  behavior of "RackSchedInstanceIdTests"

  val defaultUserResources: RuntimeResources = RuntimeResources(0, 1024.MB, 0.B)


  it should "serialize and deserialize RackschedInstanceId" in {
    val i = RackSchedInstanceId(0, defaultUserResources)
    i.serialize shouldBe JsObject(
      "asString" -> JsString(i.asString),
      "instance" -> JsNumber(i.instance),
      "resources" -> i.resources.toJson,
      "instanceType" -> JsString(i.instanceType)).compactPrint
    i.serialize shouldBe i.toJson.compactPrint
    InstanceId.parse(i.serialize) shouldBe Success(i)
  }

  it should "serialize and deserialize RackschedInstanceId to ControllerInstanceId" in {
    val i = RackSchedInstanceId(0, defaultUserResources)
    ControllerInstanceId.parse(i.serialize) shouldBe Success(i)
  }
}
