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

import spray.json._

import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.containerpool.RuntimeResources
import pureconfig._
import pureconfig.generic.auto._

case class ConfigResources(cpu: Double, mem: String, storage: String) {
  def toRuntimeResources = RuntimeResources(cpu, ByteSize.fromString(mem), ByteSize.fromString(storage))
}
case class ResourceLimitConfig(min: ConfigResources, max: ConfigResources, std: ConfigResources)

/**
 * ResourceLimit encapsulates allowed runtime resources for an object. The limits must be within a
 * permissible range (by default [128MB, 512MB]).
 *
 * It is a value type (hence == is .equals, immutable and cannot be assigned null).
 * The constructor is private so that argument requirements are checked and normalized
 * before creating a new instance.
 *
 * @param limits the memory limit in megabytes for the action
 */
protected[entity] class ResourceLimit private (val limits: RuntimeResources) {
  override def toString: String = limits.toString
}

protected[core] object ResourceLimit extends ArgNormalizer[ResourceLimit] {
  val config: ResourceLimitConfig = loadConfigOrThrow[ResourceLimitConfig](ConfigKeys.resource)

  /** These values are set once at the beginning. Dynamic configuration updates are not supported at the moment. */
  protected[core] val MIN_RESOURCES: RuntimeResources = config.min.toRuntimeResources
  protected[core] val MAX_RESOURCES: RuntimeResources = config.max.toRuntimeResources
  protected[core] val STD_RESOURCES: RuntimeResources = config.std.toRuntimeResources

  /** A singleton MemoryLimit with default value */
  protected[core] val standardResourceLimit: ResourceLimit = ResourceLimit(STD_RESOURCES)

  /** Gets MemoryLimit with default value */
  protected[core] def apply(): ResourceLimit = standardResourceLimit

  /**
   * Creates resources for limits, iff limit is within permissible range.
   *
   * @param megabytes the limit in megabytes, must be within permissible range
   * @return MemoryLimit with limit set
   * @throws IllegalArgumentException if limit does not conform to requirements
   */
  @throws[IllegalArgumentException]
  protected[core] def apply(resources: RuntimeResources): ResourceLimit = {
    Seq(
      (resources.mem.toMB.toDouble, MIN_RESOURCES.mem.toMB.toDouble, MAX_RESOURCES.mem.toMB.toDouble, "memory"),
      (resources.cpu, MIN_RESOURCES.cpu, MAX_RESOURCES.cpu, "cpu"),
      (resources.storage.toMB.toDouble, MIN_RESOURCES.storage.toMB.toDouble, MAX_RESOURCES.storage.toMB.toDouble, "storage")
    ).foreach( x => {
      require(x._1 >= x._2, s"${x._4} ${x._1} below allowed threshold of ${x._2}")
      require(x._1 <= x._3, s"${x._4} ${x._1} exceeds allowed threshold of ${x._3}")
    })

    new ResourceLimit(resources)
  }

  override protected[core] implicit val serdes = new RootJsonFormat[ResourceLimit] {
    def write(m: ResourceLimit) = RuntimeResources.serdes.write(m.limits)

    def read(value: JsValue) = ResourceLimit(RuntimeResources.serdes.read(value))

  }
}

