package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.core.entity.{ByteSize, SizeUnits}
import org.apache.openwhisk.core.entity.size.SizeInt
import spray.json.{DefaultJsonProtocol, DeserializationException, JsNumber, JsObject, JsValue, JsonParser, RootJsonFormat}

import scala.util.Try

abstract class ResourceType[T](val amount: T) {
}

/**
 *
 * @param amount represents a number of physical CPUs a container occupies
 */
class Cpu(amount: Double) extends ResourceType[Double](amount){
}

/**
 * @param amount represents a maximum amount of local memory that a container occupies
 */
class Memory(amount: ByteSize) extends ResourceType[ByteSize](amount){
  val test: ByteSize = amount
}

/**
 * @param amount represents an amount of "far" (non-local) memory a container occupies
 */
class FarMemory(amount: ByteSize) extends ResourceType[ByteSize](amount) {
}

/**
 * @param amount represents an amount of local storage that a container occupies
 */
class Storage(amount: ByteSize) extends ResourceType[ByteSize](amount){
}

class RuntimeResources(cpus: Double = 1, memSize: ByteSize = 0.B, storageSize: ByteSize = 0.B) {
  val cpu: Cpu = new Cpu(cpus)
  val mem: Memory = new Memory(memSize)
  val storage: Storage = new Storage(storageSize)

  val toJson: JsValue = RuntimeResources.serdes.write(this)
}

object RuntimeResources extends DefaultJsonProtocol {
  def parse(i: String): Try[RuntimeResources] = Try(serdes.read(JsonParser(i)))

  def none(): RuntimeResources = {
    new RuntimeResources(0, ByteSize.fromString("0B"), ByteSize.fromString("0B"))
  }

  implicit val serdes = new RootJsonFormat[RuntimeResources] {
    override def write(i: RuntimeResources): JsValue = JsObject(
      "cpu" -> JsNumber(i.cpu.amount),
      "mem" -> JsNumber(i.mem.amount.toBytes.toLong),
      "storage" -> JsNumber(i.storage.amount.toBytes.toLong),
    )

    override def read(json: JsValue): RuntimeResources = {
      json.asJsObject.getFields("cpu", "mem", "storage") match {
        case Seq(JsNumber(cpu), JsNumber(mem), JsNumber(storage)) =>
          new RuntimeResources(cpu.toInt,
            ByteSize.apply(mem.toLong, SizeUnits.BYTE),
            ByteSize.apply(storage.toLong, SizeUnits.BYTE))
        case _ => throw new DeserializationException("Color expected")
      }
    }
  }
}
