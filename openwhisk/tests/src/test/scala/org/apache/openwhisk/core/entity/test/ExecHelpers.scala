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

package org.apache.openwhisk.core.entity.test

import org.scalatest.Matchers
import org.scalatest.Suite
import common.StreamLogging
import common.WskActorSystem
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.containerpool.RuntimeResources
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.ArgNormalizer.trim
import org.apache.openwhisk.core.entity.ExecManifest._
import org.apache.openwhisk.core.entity.size._
import spray.json._
import spray.json.DefaultJsonProtocol._

trait ExecHelpers extends Matchers with WskActorSystem with StreamLogging {
  self: Suite =>

  private val config = new WhiskConfig(ExecManifest.requiredProperties)
  ExecManifest.initialize(config) should be a Symbol("success")

  protected val NODEJS10 = "nodejs:10"
  protected val SWIFT4 = "swift:4.2"
  protected val BLACKBOX = "blackbox"
  protected val JAVA_DEFAULT = "java:8"

  private def attFmt[T: JsonFormat] = Attachments.serdes[T]

  protected def imageName(name: String) =
    ExecManifest.runtimesManifest.runtimes.flatMap(_.versions).find(_.kind == name).get.image

  protected def js10Old(code: String, main: Option[String] = None) = {
    CodeExecAsString(
      RuntimeManifest(
        NODEJS10,
        imageName(NODEJS10),
        default = Some(true),
        deprecated = Some(false),
        stemCells = Some(List(StemCell(2, RuntimeResources(1.0, 256.MB, 512.MB))))),
      trim(code),
      main.map(_.trim))
  }

  protected def js10(code: String, main: Option[String] = None) = {
    val attachment = attFmt[String].read(code.trim.toJson)
    val manifest = ExecManifest.runtimesManifest.resolveDefaultRuntime(NODEJS10).get

    CodeExecAsAttachment(manifest, attachment, main.map(_.trim), Exec.isBinaryCode(code))
  }

  protected def jsDefault(code: String, main: Option[String] = None) = {
    js10(code, main)
  }

  protected def js10MetaDataOld(main: Option[String] = None, binary: Boolean) = {
    CodeExecMetaDataAsString(
      RuntimeManifest(
        NODEJS10,
        imageName(NODEJS10),
        default = Some(true),
        deprecated = Some(false),
        stemCells = Some(List(StemCell(2, RuntimeResources(1.0, 256.MB, 0.B))))),
      binary,
      main.map(_.trim))
  }

  protected def js10MetaData(main: Option[String] = None, binary: Boolean) = {
    val manifest = ExecManifest.runtimesManifest.resolveDefaultRuntime(NODEJS10).get

    CodeExecMetaDataAsAttachment(manifest, binary, main.map(_.trim))
  }

  protected def javaDefault(code: String, main: Option[String] = None) = {
    val attachment = attFmt[String].read(code.trim.toJson)
    val manifest = ExecManifest.runtimesManifest.resolveDefaultRuntime(JAVA_DEFAULT).get

    CodeExecAsAttachment(manifest, attachment, main.map(_.trim), Exec.isBinaryCode(code))
  }

  protected def javaMetaData(main: Option[String] = None, binary: Boolean) = {
    val manifest = ExecManifest.runtimesManifest.resolveDefaultRuntime(JAVA_DEFAULT).get

    CodeExecMetaDataAsAttachment(manifest, binary, main.map(_.trim))
  }

  protected def swift(code: String, main: Option[String] = None) = {
    val attachment = attFmt[String].read(code.trim.toJson)
    val manifest = ExecManifest.runtimesManifest.resolveDefaultRuntime(SWIFT4).get

    CodeExecAsAttachment(manifest, attachment, main.map(_.trim), Exec.isBinaryCode(code))
  }

  protected def sequence(components: Vector[FullyQualifiedEntityName]) = SequenceExec(components)

  protected def sequenceMetaData(components: Vector[FullyQualifiedEntityName]) = SequenceExecMetaData(components)

  protected def bb(image: String) = BlackBoxExec(ExecManifest.ImageName(trim(image)), None, None, false, false)

  protected def bb(image: String, code: String, main: Option[String] = None) = {
    val (codeOpt, binary) =
      if (code.trim.nonEmpty) (Some(attFmt[String].read(code.toJson)), Exec.isBinaryCode(code))
      else (None, false)
    BlackBoxExec(ExecManifest.ImageName(trim(image)), codeOpt, main, false, binary)
  }

  protected def blackBoxMetaData(image: String, main: Option[String] = None, binary: Boolean) = {
    BlackBoxExecMetaData(ExecManifest.ImageName(trim(image)), main, false, binary)
  }

  protected def actionLimits(memory: RuntimeResources, concurrency: Int): ActionLimits =
    ActionLimits(resources = ResourceLimit(memory), concurrency = ConcurrencyLimit(concurrency))
}
