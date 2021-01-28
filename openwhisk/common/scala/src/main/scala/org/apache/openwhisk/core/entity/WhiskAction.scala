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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.Base64
import akka.http.scaladsl.model.ContentTypes

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import spray.json._
import spray.json.DefaultJsonProtocol._
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.database.ArtifactStore
import org.apache.openwhisk.core.database.DocumentFactory
import org.apache.openwhisk.core.database.CacheChangeNotification
import org.apache.openwhisk.core.entity.Attachments._
import org.apache.openwhisk.core.entity.WhiskActivation.instantSerdes
import org.apache.openwhisk.core.entity.types.EntityStore


object SeqJsonWriter {
  implicit def seqWriterToWriter[T :JsonWriter] = new JsonWriter[Seq[T]] {
    def write(list: Seq[T]) = JsArray(list.map(_.toJson).toVector)
  }
}

case class WhiskApplication(namespace: EntityPath,
                            override val name: EntityName,
                            functions: List[WhiskEntityReference],
                            parameters: Parameters = Parameters(),
                            limits: ActionLimits = ActionLimits(),
                            version: SemVer = SemVer(),
                            publish: Boolean = false,
                            annotations: Parameters = Parameters(),
                            override val updated: Instant = WhiskEntity.currentMillis()
                           ) extends WhiskEntity(name, "application") {
  /**
   * The representation as JSON, e.g. for REST calls. Does not include id/rev.
   */
  override def toJson: JsObject = JsObject(
    "namespace" -> namespace.toJson,
    "name" -> name.toJson,
    "functions" -> functions.toJson,
    "parameters" -> parameters.toJson,
    "limits" -> limits.toJson,
    "version" -> version.toJson,
    "publish" -> publish.toJson,
    "annotations" -> annotations.toJson,
    "updated" -> updated.toJson,
  )


  /**
   * Merges parameters (usually from package) with existing action parameters.
   * Existing parameters supersede those in p.
   */
  def inherit(p: Parameters): WhiskApplication = copy(parameters = p ++ parameters).revision[WhiskApplication](rev)

  def inherit(p: Parameters, binding: Option[EntityPath] = None) =
    copy(parameters = p ++ parameters).revision[WhiskApplication](rev)

  def lookupFunctions(entityStore: EntityStore)(implicit transid: TransactionId, ec: ExecutionContext): Future[List[WhiskFunction]] = {
    Future.sequence(functions map { func =>
      WhiskFunction.get(entityStore, func.toFQEN().toDocId)
    })
  }
}

object WhiskApplication extends DocumentFactory[WhiskApplication] with WhiskEntityQueries[WhiskApplication] with DefaultJsonProtocol {
  override val collectionName: String = "applications"
  override implicit val serdes: RootJsonFormat[WhiskApplication] = jsonFormat(
    WhiskApplication.apply,
    "namespace",
    "name",
    "functions",
    "parameters",
    "limits",
    "version",
    "publish",
    "annotations",
    "updated"
  )

  protected[core] def resolveApplication(entityStore: EntityStore, fullyQualifiedName: FullyQualifiedEntityName)(
    implicit ec: ExecutionContext,
    transid: TransactionId): Future[WhiskApplication] = {
    // first check that there is a package to be resolved
    val entityPath = fullyQualifiedName.path
    if (entityPath.defaultPackage) {
      // this is the default package, nothing to resolve
      WhiskApplication.get(entityStore, fullyQualifiedName.toDocId)
    } else {
      // there is a package to be resolved
      val pkgDocid = fullyQualifiedName.path.toDocId
      val actionName = fullyQualifiedName.name
      val wp = WhiskPackage.resolveBinding(entityStore, pkgDocid, mergeParameters = true)
      wp flatMap { resolvedPkg =>
        // fully resolved name for the action
        val fqnAction = resolvedPkg.fullyQualifiedName(withVersion = false).add(actionName)
        // get the whisk action associate with it and inherit the parameters from the package/binding
        WhiskApplication.get(entityStore, fqnAction.toDocId) map {
          _.inherit(resolvedPkg.parameters)
        }
      }
    }
  }
}

case class WhiskFunction(namespace: EntityPath,
                          override val name: EntityName,
                          objects: List[WhiskEntityReference],
                          parameters: Parameters = Parameters(),
                          limits: ActionLimits = ActionLimits(),
                          version: SemVer = SemVer(),
                          publish: Boolean = false,
                          annotations: Parameters = Parameters(),
                          override val updated: Instant = WhiskEntity.currentMillis(),
                          override val parents: Option[Seq[WhiskEntityReference]] = None,
                          children: Option[Seq[WhiskEntityReference]] = None,
                          parentApp: WhiskEntityReference) extends WhiskEntity(name, "function") {
  /**
   * The representation as JSON, e.g. for REST calls. Does not include id/rev.
   */
  override def toJson: JsObject = JsObject(
    "namespace" -> namespace.toJson,
    "name" -> name.toJson,
    "objects" -> objects.toJson,
    "parameters" -> parameters.toJson,
    "limits" -> limits.toJson,
    "version" -> version.toJson,
    "publish" -> publish.toJson,
    "annotations" -> annotations.toJson,
    "updated" -> updated.toJson,
    "parents" -> parents.toJson,
    "children" -> children.toJson,
    "parentApp" -> parentApp.toJson,
  )

  def getReference(): WhiskEntityReference = {
    WhiskEntityReference(namespace, name)
  }

  def lookupObjectMetadata(entityStore: EntityStore)(implicit transid: TransactionId, ec: ExecutionContext): Future[List[WhiskActionMetaData]] = {
    Future.sequence(objects map { func =>
      WhiskActionMetaData.get(entityStore, func.toFQEN().toDocId)
    })
  }
}

object WhiskFunction extends DocumentFactory[WhiskFunction] with WhiskEntityQueries[WhiskFunction]
  with DefaultJsonProtocol {
  override val collectionName: String = "functions"
  override implicit val serdes: RootJsonFormat[WhiskFunction] = jsonFormat(
    WhiskFunction.apply,
    "namespace",
    "name",
    "objects",
    "parameters",
    "limits",
    "version",
    "publish",
    "annotations",
    "updated",
    "parents",
  "children",
  "parentApp",
  )
}

/**
 * ActionLimitsOption mirrors ActionLimits but makes both the timeout and memory
 * limit optional so that it is convenient to override just one limit at a time.
 */
case class ActionLimitsOption(timeout: Option[TimeLimit],
                              resources: Option[ResourceLimit],
                              logs: Option[LogLimit],
                              concurrency: Option[ConcurrencyLimit])

case class WhiskFunctionPut(objects: Option[List[WhiskActionPut]] = None,
                            parameters: Option[Parameters] = None,
                            limits: Option[ActionLimits] = None,
                            version: Option[SemVer] = None,
                            publish: Option[Boolean] = None,
                            annotations: Option[Parameters] = None,
                            parents: Option[Seq[String]] = None,
                            children: Option[Seq[String]] = None,
                            name: Option[String] = None) {
  def toWhiskFunction(namespace: EntityPath, objects: List[WhiskEntityReference], updated: Instant,
                      parents: Option[Seq[WhiskEntityReference]], children: Option[Seq[WhiskEntityReference]],
                      parentApp: WhiskEntityReference): WhiskFunction = {
    val lims = limits getOrElse ActionLimits()
    val params = parameters getOrElse Parameters()
    val ver = version getOrElse SemVer()
    val annotate = annotations getOrElse Parameters()
    val pub = publish getOrElse false
    val updated = Instant.now()
    WhiskFunction(namespace, EntityName(name.get), objects, params, lims, ver, pub,
      annotate, updated, parents, children, parentApp = parentApp)
  }
}

object WhiskFunctionPut extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[WhiskFunctionPut] = jsonFormat9(WhiskFunctionPut.apply)
}

case class WhiskApplicationPut(functions: List[WhiskFunctionPut],
                               parameters: Option[Parameters] = None,
                               limits: Option[ActionLimits] = None,
                               version: Option[SemVer] = None,
                               publish: Option[Boolean] = None,
                               annotations: Option[Parameters] = None)

object WhiskApplicationPut extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[WhiskApplicationPut] = jsonFormat6(WhiskApplicationPut.apply)
}


/**
 * WhiskActionPut is a restricted WhiskAction view that eschews properties
 * that are auto-assigned or derived from URI: namespace and name. It
 * also replaces limits with an optional counterpart for convenience of
 * overriding only one value at a time.
 */
case class WhiskActionPut(exec: Option[Exec] = None,
                          parameters: Option[Parameters] = None,
                          limits: Option[ActionLimitsOption] = None,
                          version: Option[SemVer] = None,
                          publish: Option[Boolean] = None,
                          annotations: Option[Parameters] = None,
                          delAnnotations: Option[Array[String]] = None,
                          relationships: Option[WhiskActionRelationshipPut] = None,
                          name: Option[String] = Some("default")) {

  protected[core] def replace(exec: Exec) = {
    WhiskActionPut(Some(exec), parameters, limits, version, publish, annotations, relationships = relationships)
  }

  /**
   * Resolves sequence components if they contain default namespace.
   */
  protected[core] def resolve(userNamespace: Namespace): WhiskActionPut = {
    exec map {
      case SequenceExec(components) =>
        val newExec = SequenceExec(components map { c =>
          FullyQualifiedEntityName(c.path.resolveNamespace(userNamespace), c.name)
        })
        WhiskActionPut(Some(newExec), parameters, limits, version, publish, annotations, relationships = relationships)
      case _ => this
    } getOrElse this
  }
}

abstract class WhiskActionLike(override val name: EntityName) extends WhiskEntity(name, "action") {
  def exec: Exec
  def parameters: Parameters
  def limits: ActionLimits

  /** @return true iff action has appropriate annotation. */
  def hasFinalParamsAnnotation = {
    annotations.getAs[Boolean](Annotations.FinalParamsAnnotationName) getOrElse false
  }

  /** @return a Set of immutable parameternames */
  def immutableParameters =
    if (hasFinalParamsAnnotation) {
      parameters.definedParameters
    } else Set.empty[String]

  def toJson =
    JsObject(
      "namespace" -> namespace.toJson,
      "name" -> name.toJson,
      "exec" -> exec.toJson,
      "parameters" -> parameters.toJson,
      "limits" -> limits.toJson,
      "version" -> version.toJson,
      "publish" -> publish.toJson,
      "annotations" -> annotations.toJson)

  def getReference(): WhiskEntityReference = {
    WhiskEntityReference(namespace, name)
  }
}

abstract class WhiskActionLikeMetaData(override val name: EntityName) extends WhiskActionLike(name) {
  override def exec: ExecMetaDataBase
}

case class WhiskEntityReference(namespace: EntityPath, name: EntityName) {

  protected[core] def toFQEN(): FullyQualifiedEntityName = {
    FullyQualifiedEntityName(namespace, name)
  }
}

object WhiskEntityReference extends DefaultJsonProtocol {

  def apply(entityName: FullyQualifiedEntityName): WhiskEntityReference = this(entityName.path, entityName.name)

  implicit val serdes: RootJsonFormat[WhiskEntityReference] = jsonFormat2(WhiskEntityReference.apply)
}

case class WhiskActionRelationship(
                                    dependents: Seq[WhiskEntityReference],
                                    parents: Seq[WhiskEntityReference],
                                    corunning: Seq[WhiskEntityReference]
                                  ) {
  def toRelationshipPut(): WhiskActionRelationshipPut = {
    WhiskActionRelationshipPut(dependents.map(_.toString), parents.map(_.toString), corunning.map(_.toString))
  }
}

object WhiskActionRelationship extends DefaultJsonProtocol {
  def empty = WhiskActionRelationship(Seq.empty, Seq.empty, Seq.empty)

  implicit val serdes: RootJsonFormat[WhiskActionRelationship] = jsonFormat3(WhiskActionRelationship.apply)
}


// For creation of DAGs, the WhiskAction's name given in the upload can be used here to define
// the relationships that a particular action (object) may have. They will be converted from String
// to a WhiskEntityReference upon upload.
case class WhiskActionRelationshipPut(
                                       dependents: Seq[String],
                                       parents: Seq[String],
                                       corunning: Seq[String]
                                     )

object WhiskActionRelationshipPut extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[WhiskActionRelationshipPut] = jsonFormat3(WhiskActionRelationshipPut.apply)
}


/**
 * A WhiskAction provides an abstraction of the meta-data
 * for a whisk action.
 *
 * The WhiskAction object is used as a helper to adapt objects between
 * the schema used by the database and the WhiskAction abstraction.
 *
 * @param namespace the namespace for the action
 * @param name the name of the action
 * @param exec the action executable details
 * @param parameters the set of parameters to bind to the action environment
 * @param limits the limits to impose on the action
 * @param version the semantic version
 * @param publish true to share the action or false otherwise
 * @param annotations the set of annotations to attribute to the action
 * @param updated the timestamp when the action is updated
 * @throws IllegalArgumentException if any argument is undefined
 */
@throws[IllegalArgumentException]
case class WhiskAction(namespace: EntityPath, //name
                       override val name: EntityName,//name
                       exec: Exec, // runtime
                       parameters: Parameters = Parameters(),
                       limits: ActionLimits = ActionLimits(),
                       version: SemVer = SemVer(),
                       publish: Boolean = false,
                       annotations: Parameters = Parameters(),
                       override val updated: Instant = WhiskEntity.currentMillis(),
                       relationships: Option[WhiskActionRelationship] = None,
                       parentFunc: Option[WhiskEntityReference] = None)
    extends WhiskActionLike(name) {

  require(exec != null, "exec undefined")
  require(limits != null, "limits undefined")

  /**
   * Merges parameters (usually from package) with existing action parameters.
   * Existing parameters supersede those in p.
   */
  def inherit(p: Parameters): WhiskAction = copy(parameters = p ++ parameters).revision[WhiskAction](rev)

  /**
   * Resolves sequence components if they contain default namespace.
   */
  protected[core] def resolve(userNamespace: Namespace): WhiskAction = {
    resolve(userNamespace.name)
  }

  /**
   * Resolves sequence components if they contain default namespace.
   */
  protected[core] def resolve(userNamespace: EntityName): WhiskAction = {
    exec match {
      case SequenceExec(components) =>
        val newExec = SequenceExec(components map { c =>
          FullyQualifiedEntityName(c.path.resolveNamespace(userNamespace), c.name)
        })
        copy(exec = newExec).revision[WhiskAction](rev)
      case _ => this
    }
  }

  def toExecutableWhiskAction: Option[ExecutableWhiskAction] = exec match {
    case codeExec: CodeExec[_] =>
      Some(
        ExecutableWhiskAction(namespace, name, codeExec, parameters, limits, version, publish, annotations, relationships = relationships, parentFunc = parentFunc)
          .revision[ExecutableWhiskAction](rev))
    case _ => None
  }

  /**
   * This the action summary as computed by the database view.
   * Strictly used in view testing to enforce alignment.
   */
  override def summaryAsJson: JsObject = {
    val binary = exec match {
      case c: CodeExec[_] => c.binary
      case _              => false
    }

    JsObject(
      super.summaryAsJson.fields +
        ("limits" -> limits.toJson) +
        ("exec" -> JsObject("binary" -> JsBoolean(binary))))
  }
}

@throws[IllegalArgumentException]
case class WhiskActionMetaData(namespace: EntityPath,
                               override val name: EntityName,
                               exec: ExecMetaDataBase,
                               parameters: Parameters = Parameters(),
                               limits: ActionLimits = ActionLimits(),
                               version: SemVer = SemVer(),
                               publish: Boolean = false,
                               annotations: Parameters = Parameters(),
                               override val updated: Instant = WhiskEntity.currentMillis(),
                               binding: Option[EntityPath] = None,
                               relationships: Option[WhiskActionRelationship] = None,
                               parentFunc: Option[WhiskEntityReference] = None)
    extends WhiskActionLikeMetaData(name) {

  require(exec != null, "exec undefined")
  require(limits != null, "limits undefined")

  /**
   * Merges parameters (usually from package) with existing action parameters.
   * Existing parameters supersede those in p.
   */
  def inherit(p: Parameters, binding: Option[EntityPath] = None) =
    copy(parameters = p ++ parameters, binding = binding).revision[WhiskActionMetaData](rev)

  /**
   * Resolves sequence components if they contain default namespace.
   */
  protected[core] def resolve(userNamespace: Namespace): WhiskActionMetaData = {
    exec match {
      case SequenceExecMetaData(components) =>
        val newExec = SequenceExecMetaData(components map { c =>
          FullyQualifiedEntityName(c.path.resolveNamespace(userNamespace.name), c.name)
        })
        copy(exec = newExec).revision[WhiskActionMetaData](rev)
      case _ => this
    }
  }

  def toExecutableWhiskAction = exec match {
    case execMetaData: ExecMetaData =>
      Some(
        ExecutableWhiskActionMetaData(
          namespace,
          name,
          execMetaData,
          parameters,
          limits,
          version,
          publish,
          annotations,
          binding,
          relationships = relationships,
          parentFunc = parentFunc)
          .revision[ExecutableWhiskActionMetaData](rev))
    case _ =>
      None
  }
}

/**
 * Variant of WhiskAction which only includes information necessary to be
 * executed by an Invoker.
 *
 * exec is typed to CodeExec to guarantee executability by an Invoker.
 *
 * Note: Two actions are equal regardless of their DocRevision if there is one.
 * The invoker uses action equality when matching actions to warm containers.
 * That means creating an action, invoking it, then deleting/recreating/reinvoking
 * it will reuse the previous container. The delete/recreate restores the SemVer to 0.0.1.
 *
 * @param namespace the namespace for the action
 * @param name the name of the action
 * @param exec the action executable details
 * @param parameters the set of parameters to bind to the action environment
 * @param limits the limits to impose on the action
 * @param version the semantic version
 * @param publish true to share the action or false otherwise
 * @param annotations the set of annotations to attribute to the action
 * @param binding the path of the package binding if any
 * @throws IllegalArgumentException if any argument is undefined
 */
@throws[IllegalArgumentException]
case class ExecutableWhiskAction(namespace: EntityPath,
                                 override val name: EntityName,
                                 exec: CodeExec[_],
                                 parameters: Parameters = Parameters(),
                                 limits: ActionLimits = ActionLimits(),
                                 version: SemVer = SemVer(),
                                 publish: Boolean = false,
                                 annotations: Parameters = Parameters(),
                                 binding: Option[EntityPath] = None,
                                 relationships: Option[WhiskActionRelationship] = Some(WhiskActionRelationship.empty),
                                 parentFunc: Option[WhiskEntityReference] = None
                                )
    extends WhiskActionLike(name) {

  require(exec != null, "exec undefined")
  require(limits != null, "limits undefined")

  /**
   * Gets initializer for action. This typically includes the code to execute,
   * or a zip file containing the executable artifacts.
   *
   * @param env optional map of properties to be exported to the environment
   */
  def containerInitializer(env: Map[String, JsValue] = Map.empty): JsObject = {
    val code = Option(exec.codeAsJson).filter(_ != JsNull).map("code" -> _)
    val envargs = if (env.nonEmpty) {
      val stringifiedEnvVars = env.map {
        case (k, v: JsString)  => (k, v)
        case (k, JsNull)       => (k, JsString.empty)
        case (k, JsBoolean(v)) => (k, JsString(v.toString))
        case (k, JsNumber(v))  => (k, JsString(v.toString))
        case (k, v)            => (k, JsString(v.compactPrint))
      }

      Some("env" -> JsObject(stringifiedEnvVars))
    } else None

    val base =
      Map("name" -> name.toJson, "binary" -> exec.binary.toJson, "main" -> exec.entryPoint.getOrElse("main").toJson)

    JsObject(base ++ envargs ++ code)
  }

  def toWhiskAction =
    WhiskAction(namespace, name, exec, parameters, limits, version, publish, annotations, relationships = relationships, parentFunc = parentFunc)
      .revision[WhiskAction](rev)
}

@throws[IllegalArgumentException]
case class ExecutableWhiskActionMetaData(namespace: EntityPath,
                                         override val name: EntityName,
                                         exec: ExecMetaData,
                                         parameters: Parameters = Parameters(),
                                         limits: ActionLimits = ActionLimits(),
                                         version: SemVer = SemVer(),
                                         publish: Boolean = false,
                                         annotations: Parameters = Parameters(),
                                         binding: Option[EntityPath] = None,
                                         relationships: Option[WhiskActionRelationship] = None,
                                         parentFunc: Option[WhiskEntityReference] = None,
                                         )
    extends WhiskActionLikeMetaData(name) {

  require(exec != null, "exec undefined")
  require(limits != null, "limits undefined")

  def toWhiskAction =
    WhiskActionMetaData(namespace, name, exec, parameters, limits, version, publish, annotations, updated, relationships = relationships, parentFunc = parentFunc)
      .revision[WhiskActionMetaData](rev)

  /**
   * Some fully qualified name only if there's a binding, else None.
   */
  def bindingFullyQualifiedName: Option[FullyQualifiedEntityName] =
    binding.map(ns => FullyQualifiedEntityName(ns, name, None))
}

object WhiskAction extends DocumentFactory[WhiskAction] with WhiskEntityQueries[WhiskAction] with DefaultJsonProtocol {
  import WhiskActivation.instantSerdes

  val execFieldName = "exec"
  val requireWhiskAuthHeader = "x-require-whisk-auth"

  override val collectionName = "actions"
  override val cacheEnabled = true

  override implicit val serdes = new spray.json.RootJsonFormat[WhiskAction] {
    override def write(obj: WhiskAction): JsValue = jsonFormat(
        WhiskAction.apply,
        "namespace",
        "name",
        "exec",
        "parameters",
        "limits",
        "version",
        "publish",
        "annotations",
        "updated",
       "relationships",
       "parentFunc").write(obj)

    override def read(json: JsValue): WhiskAction = {
      WhiskAction(
        fromField[EntityPath](json, "namespace"),
        fromField[EntityName](json, "name"),
        fromField[Exec](json, "exec"),
        fromField[Parameters](json, "parameters"),
        fromField[ActionLimits](json, "limits"),
        fromField[SemVer](json, "version"),
        fromField[Boolean](json, "publish"),
        fromField[Parameters](json, "annotations"),
        fromField[Instant](json, "updated"),
        fromField[Option[WhiskActionRelationship]](json, "relationships"),
        fromField[Option[WhiskEntityReference]](json, "parentFunc")
      )
    }
  }


  // overriden to store attached code
  override def put[A >: WhiskAction](db: ArtifactStore[A], doc: WhiskAction, old: Option[WhiskAction])(
    implicit transid: TransactionId,
    notifier: Option[CacheChangeNotification]): Future[DocInfo] = {

    def putWithAttachment(code: String, binary: Boolean, exec: AttachedCode) = {
      implicit val logger = db.logging
      implicit val ec = db.executionContext

      val oldAttachment = old.flatMap(getAttachment)
      val (bytes, attachmentType) = if (binary) {
        (Base64.getDecoder.decode(code), ContentTypes.`application/octet-stream`)
      } else {
        (code.getBytes(UTF_8), ContentTypes.`text/plain(UTF-8)`)
      }
      val stream = new ByteArrayInputStream(bytes)
      super.putAndAttach(
        db,
        doc
          .copy(parameters = doc.parameters.lock(ParameterEncryption.singleton.default))
          .revision[WhiskAction](doc.rev),
        attachmentUpdater,
        attachmentType,
        stream,
        oldAttachment,
        Some { a: WhiskAction =>
          a.copy(exec = exec.inline(code.getBytes(UTF_8)))
        })
    }

    Try {
      require(db != null, "db undefined")
      require(doc != null, "doc undefined")
    } map { _ =>
      doc.exec match {
        case exec @ CodeExecAsAttachment(_, Inline(code), _, binary) =>
          putWithAttachment(code, binary, exec)
        case exec @ BlackBoxExec(_, Some(Inline(code)), _, _, binary) =>
          putWithAttachment(code, binary, exec)
        case _ =>
          super.put(
            db,
            doc
              .copy(parameters = doc.parameters.lock(ParameterEncryption.singleton.default))
              .revision[WhiskAction](doc.rev),
            old)
      }
    } match {
      case Success(f) => f
      case Failure(f) => Future.failed(f)
    }
  }

  // overriden to retrieve attached code
  override def get[A >: WhiskAction](
    db: ArtifactStore[A],
    doc: DocId,
    rev: DocRevision = DocRevision.empty,
    fromCache: Boolean)(implicit transid: TransactionId, mw: Manifest[WhiskAction]): Future[WhiskAction] = {

    implicit val ec = db.executionContext

    val inlineActionCode: WhiskAction => Future[WhiskAction] = { action =>
      def getWithAttachment(attached: Attached, binary: Boolean, exec: AttachedCode) = {
        val boas = new ByteArrayOutputStream()
        val wrapped = if (binary) Base64.getEncoder().wrap(boas) else boas

        getAttachment[A](db, action, attached, wrapped, Some { a: WhiskAction =>
          wrapped.close()
          val newAction = a.copy(exec = exec.inline(boas.toByteArray))
          newAction.revision(a.rev)
          newAction
        })
      }

      action.exec match {
        case exec @ CodeExecAsAttachment(_, attached: Attached, _, binary) =>
          getWithAttachment(attached, binary, exec)
        case exec @ BlackBoxExec(_, Some(attached: Attached), _, _, binary) =>
          getWithAttachment(attached, binary, exec)
        case _ =>
          Future.successful(action)
      }
    }
    super.getWithAttachment(db, doc, rev, fromCache, attachmentHandler, inlineActionCode)
  }

  def attachmentHandler(action: WhiskAction, attached: Attached): WhiskAction = {
    def checkName(name: String) = {
      require(
        name == attached.attachmentName,
        s"Attachment name '${attached.attachmentName}' does not match the expected name '$name'")
    }
    val eu = action.exec match {
      case exec @ CodeExecAsAttachment(_, Attached(attachmentName, _, _, _), _, _) =>
        checkName(attachmentName)
        exec.attach(attached)
      case exec @ BlackBoxExec(_, Some(Attached(attachmentName, _, _, _)), _, _, _) =>
        checkName(attachmentName)
        exec.attach(attached)
      case exec => exec
    }
    action.copy(exec = eu).revision[WhiskAction](action.rev)
  }

  def attachmentUpdater(action: WhiskAction, updatedAttachment: Attached): WhiskAction = {
    action.exec match {
      case exec: AttachedCode =>
        action.copy(exec = exec.attach(updatedAttachment)).revision[WhiskAction](action.rev)
      case _ => action
    }
  }

  def getAttachment(action: WhiskAction): Option[Attached] = {
    action.exec match {
      case CodeExecAsAttachment(_, a: Attached, _, _)  => Some(a)
      case BlackBoxExec(_, Some(a: Attached), _, _, _) => Some(a)
      case _                                           => None
    }
  }

  override def del[Wsuper >: WhiskAction](db: ArtifactStore[Wsuper], doc: DocInfo)(
    implicit transid: TransactionId,
    notifier: Option[CacheChangeNotification]): Future[Boolean] = {
    Try {
      require(db != null, "db undefined")
      require(doc != null, "doc undefined")
    }.map { _ =>
      val fa = super.del(db, doc)
      implicit val ec = db.executionContext
      fa.flatMap { _ =>
        super.deleteAttachments(db, doc)
      }
    } match {
      case Success(f) => f
      case Failure(f) => Future.failed(f)
    }
  }

  /**
   * Resolves an action name if it is contained in a package.
   * Look up the package to determine if it is a binding or the actual package.
   * If it's a binding, rewrite the fully qualified name of the action using the actual package path name.
   * If it's the actual package, use its name directly as the package path name.
   */
  def resolveAction(db: EntityStore, fullyQualifiedActionName: FullyQualifiedEntityName)(
    implicit ec: ExecutionContext,
    transid: TransactionId): Future[FullyQualifiedEntityName] = {
    // first check that there is a package to be resolved
    val entityPath = fullyQualifiedActionName.path
    if (entityPath.defaultPackage) {
      // this is the default package, nothing to resolve
      Future.successful(fullyQualifiedActionName)
    } else {
      // there is a package to be resolved
      val pkgDocId = fullyQualifiedActionName.path.toDocId
      val actionName = fullyQualifiedActionName.name
      WhiskPackage.resolveBinding(db, pkgDocId) map {
        _.fullyQualifiedName(withVersion = false).add(actionName)
      }
    }
  }

  /**
   * Resolves an action name if it is contained in a package.
   * Look up the package to determine if it is a binding or the actual package.
   * If it's a binding, rewrite the fully qualified name of the action using the actual package path name.
   * If it's the actual package, use its name directly as the package path name.
   * While traversing the package bindings, merge the parameters.
   */
  def resolveActionAndMergeParameters(entityStore: EntityStore, fullyQualifiedName: FullyQualifiedEntityName)(
    implicit ec: ExecutionContext,
    transid: TransactionId): Future[WhiskAction] = {
    // first check that there is a package to be resolved
    val entityPath = fullyQualifiedName.path
    if (entityPath.defaultPackage) {
      // this is the default package, nothing to resolve
      WhiskAction.get(entityStore, fullyQualifiedName.toDocId)
    } else {
      // there is a package to be resolved
      val pkgDocid = fullyQualifiedName.path.toDocId
      val actionName = fullyQualifiedName.name
      val wp = WhiskPackage.resolveBinding(entityStore, pkgDocid, mergeParameters = true)
      wp flatMap { resolvedPkg =>
        // fully resolved name for the action
        val fqnAction = resolvedPkg.fullyQualifiedName(withVersion = false).add(actionName)
        // get the whisk action associate with it and inherit the parameters from the package/binding
        WhiskAction.get(entityStore, fqnAction.toDocId) map {
          _.inherit(resolvedPkg.parameters)
        }
      }
    }
  }
}

object WhiskActionMetaData
    extends DocumentFactory[WhiskActionMetaData]
    with WhiskEntityQueries[WhiskActionMetaData]
    with DefaultJsonProtocol {

  import WhiskActivation.instantSerdes

  override val collectionName = "actions"
  override val cacheEnabled = true

  override implicit val serdes = new spray.json.RootJsonFormat[WhiskActionMetaData] {
    override def write(obj: WhiskActionMetaData): JsValue = jsonFormat(
      WhiskActionMetaData.apply,
      "namespace",
      "name",
      "exec",
      "parameters",
      "limits",
      "version",
      "publish",
      "annotations",
      "updated",
      "binding",
      "relationships",
      "parentFunc").write(obj)

    override def read(json: JsValue): WhiskActionMetaData = {
      WhiskActionMetaData(
        fromField[EntityPath](json, "namespace"),
        fromField[EntityName](json, "name"),
        fromField[ExecMetaDataBase](json, "exec"),
        fromField[Parameters](json, "parameters"),
        fromField[ActionLimits](json, "limits"),
        fromField[SemVer](json, "version"),
        fromField[Boolean](json, "publish"),
        fromField[Parameters](json, "annotations"),
        fromField[Instant](json, "updated"),
        fromField[Option[EntityPath]](json, "binding"),
        fromField[Option[WhiskActionRelationship]](json, "relationships"),
        fromField[Option[WhiskEntityReference]](json, "parentFunc"),
      )
    }
  }

  /**
   * Resolves an action name if it is contained in a package.
   * Look up the package to determine if it is a binding or the actual package.
   * If it's a binding, rewrite the fully qualified name of the action using the actual package path name.
   * If it's the actual package, use its name directly as the package path name.
   */
  def resolveAction(db: EntityStore, fullyQualifiedActionName: FullyQualifiedEntityName)(
    implicit ec: ExecutionContext,
    transid: TransactionId): Future[FullyQualifiedEntityName] = {
    // first check that there is a package to be resolved
    val entityPath = fullyQualifiedActionName.path
    if (entityPath.defaultPackage) {
      // this is the default package, nothing to resolve
      Future.successful(fullyQualifiedActionName)
    } else {
      // there is a package to be resolved
      val pkgDocId = fullyQualifiedActionName.path.toDocId
      val actionName = fullyQualifiedActionName.name
      WhiskPackage.resolveBinding(db, pkgDocId) map {
        _.fullyQualifiedName(withVersion = false).add(actionName)
      }
    }
  }

  /**
   * Resolves an action name if it is contained in a package.
   * Look up the package to determine if it is a binding or the actual package.
   * If it's a binding, rewrite the fully qualified name of the action using the actual package path name.
   * If it's the actual package, use its name directly as the package path name.
   * While traversing the package bindings, merge the parameters.
   */
  def resolveActionAndMergeParameters(entityStore: EntityStore, fullyQualifiedName: FullyQualifiedEntityName)(
    implicit ec: ExecutionContext,
    transid: TransactionId): Future[WhiskActionMetaData] = {
    // first check that there is a package to be resolved
    val entityPath = fullyQualifiedName.path
    if (entityPath.defaultPackage) {
      // this is the default package, nothing to resolve
      WhiskActionMetaData.get(entityStore, fullyQualifiedName.toDocId)
    } else {
      // there is a package to be resolved
      val pkgDocid = fullyQualifiedName.path.toDocId
      val actionName = fullyQualifiedName.name
      val wp = WhiskPackage.resolveBinding(entityStore, pkgDocid, mergeParameters = true)
      wp flatMap { resolvedPkg =>
        // fully resolved name for the action
        val fqnAction = resolvedPkg.fullyQualifiedName(withVersion = false).add(actionName)
        // get the whisk action associate with it and inherit the parameters from the package/binding
        WhiskActionMetaData.get(entityStore, fqnAction.toDocId) map {
          _.inherit(
            resolvedPkg.parameters,
            if (fullyQualifiedName.path.equals(resolvedPkg.fullPath)) None
            else Some(fullyQualifiedName.path))
        }
      }
    }
  }
}

object ActionLimitsOption extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[ActionLimitsOption] = jsonFormat4(ActionLimitsOption.apply)

  def fromActionLimit(obj: ActionLimits): ActionLimitsOption = {
    ActionLimitsOption(Some(obj.timeout), Some(obj.resources), Some(obj.logs), Some(obj.concurrency))
  }
}

object WhiskActionPut extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[WhiskActionPut] = jsonFormat9(WhiskActionPut.apply)

  def fromWhiskAction(obj: WhiskAction): WhiskActionPut = {
    WhiskActionPut(Some(obj.exec), Some(obj.parameters), Some(ActionLimitsOption.fromActionLimit(obj.limits)),
      Some(obj.version), Some(obj.publish), Some(obj.annotations), None, obj.relationships.map(_.toRelationshipPut()), name = Some(obj.name.toString()))
  }
}
