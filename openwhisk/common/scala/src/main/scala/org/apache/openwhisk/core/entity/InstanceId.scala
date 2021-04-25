/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.entity

import org.apache.openwhisk.core.containerpool.RuntimeResources
import org.apache.openwhisk.core.entity.RackSchedInstanceId.rackSchedHealthTopic
import spray.json.{DefaultJsonProtocol, JsNumber, JsObject, JsString, JsValue, RootJsonFormat, deserializationError}
import spray.json._

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * An instance id representing an invoker
 *
 * @param instance a numeric value used for the load balancing and Kafka topic creation
 * @param uniqueName an identifier required for dynamic instance assignment by Zookeeper
 * @param displayedName an identifier that is required for the health protocol to correlate Kafka topics with invoker container names
 */
case class InvokerInstanceId(val instance: Int,
                             uniqueName: Option[String] = None,
                             displayedName: Option[String] = None,
                             val resources: RuntimeResources)
    extends InstanceId {
  def toInt: Int = instance
  def getMainTopic: String = s"invoker${toInt}"
  def getDepInputTopic: String = s"invoker${toInt}-depInput"
  def getSchedResultTopic: String = s"invoker${toInt}-schedResult"

  override val instanceType = "invoker"

  override val source = s"$instanceType$instance"

  override val toString: String = (Seq("invoker" + instance) ++ uniqueName ++ displayedName).mkString("/")

  override val toJson: JsValue = InvokerInstanceId.serdes.write(this)

}

class ControllerInstanceId(val asString: String) extends InstanceId {
  validate(asString)
  override val instanceType = "controller"

  override val source = s"$instanceType$asString"

  override val toString: String = source

  override val toJson: JsValue = ControllerInstanceId.serdes.write(this)
}

class TopSchedInstanceId(override val asString: String) extends ControllerInstanceId(asString) {
  validate(asString)
  override val instanceType = "topsched"

  def topic: String = s"topsched"
}

object InvokerInstanceId extends DefaultJsonProtocol {
  def parse(c: String): Try[InvokerInstanceId] = Try(serdes.read(c.parseJson))

  implicit val serdes = new RootJsonFormat[InvokerInstanceId] {
    override def write(i: InvokerInstanceId): JsValue = {
      val fields = new ListBuffer[(String, JsValue)]
      fields ++= List("instance" -> JsNumber(i.instance))
      fields ++= List("resources" -> i.resources.toJson)
      fields ++= List("instanceType" -> JsString(i.instanceType))
      i.uniqueName.foreach(uniqueName => fields ++= List("uniqueName" -> JsString(uniqueName)))
      i.displayedName.foreach(displayedName => fields ++= List("displayedName" -> JsString(displayedName)))
      JsObject(fields.toSeq: _*)
    }

    override def read(json: JsValue): InvokerInstanceId = {
      val instance = fromField[Int](json, "instance")
      val uniqueName = fromField[Option[String]](json, "uniqueName")
      val displayedName = fromField[Option[String]](json, "displayedName")
      val resources = RuntimeResources.serdes.read(json.asJsObject.fields("resources"))
      val instanceType = fromField[String](json, "instanceType")

      if (instanceType == "invoker") {
        new InvokerInstanceId(instance, uniqueName, displayedName, resources)
      } else {
        deserializationError("could not read InvokerInstanceId")
      }
    }
  }
}

case class RackSchedInstanceId(instance: Int,
                               resources: RuntimeResources,
                               uniqueName: Option[String] = None,
                               displayName: Option[String] = None) extends ControllerInstanceId(instance.toString) {
  override val instanceType = "racksched"

  def toInt: Int = instance

  override val source = s"$instanceType.$instance"

  override val toString: String = (Seq("racksched" + instance) ++ uniqueName ++ displayName).mkString("/")

  override val toJson: JsValue = RackSchedInstanceId.serdes.write(this)

  def healthTopic: String = rackSchedHealthTopic(instance)
}

object RackSchedInstanceId extends DefaultJsonProtocol {

  def rackSchedHealthTopic(id: Int): String = {
    "health" + id
  }

  def parse(c: String): Try[RackSchedInstanceId] = Try(serdes.read(c.parseJson))

  implicit val serdes: RootJsonFormat[RackSchedInstanceId] = new RootJsonFormat[RackSchedInstanceId] {
    override def write(i: RackSchedInstanceId): JsValue = {
      val fields = new ListBuffer[(String, JsValue)]
      fields ++= List("asString" -> JsString(i.instance.toString))
      fields ++= List("resources" -> i.resources.toJson)
      fields ++= List("instanceType" -> JsString(i.instanceType))
      i.uniqueName.foreach(uniqueName => fields ++= List("uniqueName" -> JsString(uniqueName)))
      i.displayName.foreach(displayedName => fields ++= List("displayName" -> JsString(displayedName)))
      JsObject(fields.toSeq: _*)
    }

    override def read(json: JsValue): RackSchedInstanceId = {
      val instance = Integer.parseInt(fromField[String](json, "asString"))
      val uniqueName = fromField[Option[String]](json, "uniqueName")
      val displayName = fromField[Option[String]](json, "displayName")
      val resources = json.asJsObject.fields.get("resources") match {
        case Some(value) => RuntimeResources.serdes.read(value)
        case None => RuntimeResources.none()
      }
      val instanceType = fromField[String](json, "instanceType")

      if (instanceType == "racksched") {
        new RackSchedInstanceId( instance, resources, uniqueName, displayName)
      } else {
        deserializationError("could not read InvokerInstanceId")
      }
    }
  }
}

object ControllerInstanceId extends DefaultJsonProtocol {
  def parse(c: String): Try[ControllerInstanceId] = Try(serdes.read(c.parseJson))

  implicit val serdes = new RootJsonFormat[ControllerInstanceId] {
    override def write(c: ControllerInstanceId): JsValue =
      JsObject("asString" -> JsString(c.asString), "instanceType" -> JsString(c.instanceType))

    override def read(json: JsValue): ControllerInstanceId = {
      json.asJsObject.getFields("asString", "instanceType") match {
        case Seq(JsString(asString), JsString(instanceType)) =>
          instanceType match {
            case "controller" => {
              new ControllerInstanceId(asString)
            }
            case "topsched" => {
              new TopSchedInstanceId(asString)
            }
            case "racksched" => {
              RackSchedInstanceId.serdes.read(json)
            }
            case _ => deserializationError("could not read ControllerInstanceId")
          }
        case Seq(JsString(asString)) =>
          new ControllerInstanceId(asString)
        case _ =>
          deserializationError("could not read ControllerInstanceId")
      }
    }
  }
}

object TopSchedInstanceId extends DefaultJsonProtocol {
  def parse(c: String): Try[TopSchedInstanceId] = Try(serdes.read(c.parseJson))

  implicit val serdes = new RootJsonFormat[TopSchedInstanceId] {
    override def write(c: TopSchedInstanceId): JsValue =
      JsObject("asString" -> JsString(c.asString), "instanceType" -> JsString(c.instanceType))

    override def read(json: JsValue): TopSchedInstanceId = {
      json.asJsObject.getFields("asString", "instanceType") match {
        case Seq(JsString(asString), JsString(instanceType)) =>
          if (instanceType == "topsched") {
            new TopSchedInstanceId(asString)
          } else {
            deserializationError("could not read ControllerInstanceId")
          }
        case Seq(JsString(asString)) =>
          new TopSchedInstanceId(asString)
        case _ =>
          deserializationError("could not read ControllerInstanceId")
      }
    }
  }
}

trait InstanceId {

  // controller ids become part of a kafka topic, hence allow only certain characters
  // see https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L29
  private val LEGAL_CHARS = "[a-zA-Z0-9._-]+"

  // reserve some number of characters as the prefix to be added to topic names
  private val MAX_NAME_LENGTH = 249 - 121

  def serialize: String = InstanceId.serdes.write(this).compactPrint

  def validate(asString: String): Unit =
    require(
      asString.length <= MAX_NAME_LENGTH && asString.matches(LEGAL_CHARS),
      s"$instanceType instance id contains invalid characters")

  val instanceType: String

  val source: String

  val toJson: JsValue
}

object InstanceId extends DefaultJsonProtocol {
  def parse(i: String): Try[InstanceId] = Try(serdes.read(i.parseJson))

  implicit val serdes = new RootJsonFormat[InstanceId] {
    override def write(i: InstanceId): JsValue = i.toJson

    override def read(json: JsValue): InstanceId = {
      val JsObject(field) = json
      field
        .get("instanceType")
        .map(_.convertTo[String] match {
          case "invoker" =>
            json.convertTo[InvokerInstanceId]
          case "controller" =>
            json.convertTo[ControllerInstanceId]
          case "racksched" =>
            json.convertTo[RackSchedInstanceId]
          case "topsched" =>
            json.convertTo[TopSchedInstanceId]
          case _ =>
            deserializationError("could not read InstanceId")
        })
        .getOrElse(deserializationError("could not read InstanceId"))
    }
  }
}
