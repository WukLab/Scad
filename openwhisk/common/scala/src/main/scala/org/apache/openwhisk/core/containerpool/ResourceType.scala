package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.core.entity.{ByteSize, SizeUnits}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsNumber, JsObject, JsValue, JsonParser, RootJsonFormat}

import scala.util.Try

abstract class ResourceType[T](var amount: T) {
}
class Cpu(amount: Int) extends ResourceType[Int](amount){
}
class Memory(amount: ByteSize) extends ResourceType[ByteSize](amount){
}
class FarMemory(amount: ByteSize) extends ResourceType[ByteSize](amount) {
}
class Storage(amount: ByteSize) extends ResourceType[ByteSize](amount){
}

class RuntimeResources(cpus: Int, memSize: ByteSize, storageSize: ByteSize) {
  val cpu: Cpu = new Cpu(cpus)
  var mem: Memory = new Memory(memSize)
  var storage: Storage = new Storage(storageSize)

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
      "mem" -> JsNumber(i.mem.amount.size.toByte),
      "storage" -> JsNumber(i.storage.amount.size.toByte),
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
