package org.apache.openwhisk.core.entity.test

import org.apache.openwhisk.core.connector.PingRackMessage
import org.apache.openwhisk.core.containerpool.{InvokerPoolResources, RuntimeResources}
import org.apache.openwhisk.core.entity.ByteSize
import org.apache.openwhisk.core.entity.RackSchedInstanceId
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
//import org.scalatest.BeforeAndAfterEach
//import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers


@RunWith(classOf[JUnitRunner])
class MessageTest extends FlatSpec with Matchers {
  behavior of "rack messages"


  it should "parse rack ping" in {
    val inst = new RackSchedInstanceId(0, new RuntimeResources(0, ByteSize.fromString("0B"), ByteSize.fromString("0B")));
    val x = PingRackMessage(inst, InvokerPoolResources.none)
    val output = x.serialize
    println(output)
//    val result = PingRackMessage.parse(output)
  }
}
