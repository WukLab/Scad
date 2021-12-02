package org.apache.openwhisk.core.containerpool

import org.apache.openwhisk.core.entity.{ByteSize, SizeUnits}
import org.apache.openwhisk.core.entity.size.SizeInt
import spray.json.{DefaultJsonProtocol, JsValue, JsonParser, RootJsonFormat}

import scala.util.Try

/**
 * Represents a set of resources to be utilized by an object
 *
 * @param cpu the fraction of the host CPU that this is allowed to utilize. 1.0 is the entire system. 0.0 is none.
 * @param mem the memory limit for the object
 * @param storage the storage size limit for the object
 */
case class RuntimeResources(cpu: Double = 1, mem: ByteSize = 0.B, storage: ByteSize = 0.B) extends Ordered[RuntimeResources] {

  def +(other: RuntimeResources): RuntimeResources = {
    RuntimeResources(cpu + other.cpu, mem + other.mem, storage + other.storage)
  }

  def -(other: RuntimeResources): RuntimeResources = {
    RuntimeResources(cpu - other.cpu, mem - other.mem, storage - other.storage)
  }

  def ==(other: RuntimeResources): Boolean = {
    cpu == other.cpu && mem == other.mem && storage == other.storage
  }

  def *(other: Int): RuntimeResources = {
    RuntimeResources(cpu * other, mem * other, storage * other)
  }

  def /(other: Int): RuntimeResources = {
    RuntimeResources(cpu / other, mem / other, storage / other)
  }

  def /(other: RuntimeResources): RuntimeResources = {
    RuntimeResources(cpu / other.cpu, ByteSize((mem / other.mem).floor.toLong, SizeUnits.BYTE), ByteSize((storage / other.storage).floor.toLong, SizeUnits.BYTE))
  }

  /**
   * Return whether or not any resource values of the given object equals or exceeds a resource value from another object
   * if any one of mem, cpu, or storage exceeds then this function returns false.
   */
  def exceedsAnyOf(other: RuntimeResources): Boolean = {
    cpu > other.cpu || mem > other.mem || storage > other.storage
  }

  override def toString = s"{CPU: ${cpu} | MEM: ${mem.toMB}MB | STORAGE: ${storage.toMB}MB}"

  val toJson: JsValue = RuntimeResources.serdes.write(this)

  // todo(zac) update this definition to use all resources or define a better ordering.
  override def compare(that: RuntimeResources): Int = {
    this.mem.compare(that.mem)
  }
}

object RuntimeResources extends DefaultJsonProtocol {

  def parse(i: String): Try[RuntimeResources] = Try(serdes.read(JsonParser(i)))

  def none(): RuntimeResources = {
   RuntimeResources(0, 0.B, 0.B)
  }

  def mem(amount: ByteSize): RuntimeResources = {
    RuntimeResources(0, amount, 0.B)
  }

  implicit val serdes: RootJsonFormat[RuntimeResources] = jsonFormat(RuntimeResources.apply, "cpu", "mem", "storage")
}
