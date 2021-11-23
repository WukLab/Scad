package org.apache.openwhisk.core.swap

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.containerpool.RuntimeResources
import org.apache.openwhisk.core.database.NoDocumentException
import org.apache.openwhisk.core.entity.size.SizeInt
import org.apache.openwhisk.core.entity.types.EntityStore
import org.apache.openwhisk.core.entity.{ActionLimits, ActivationId, BasicAuthenticationAuthKey, ByteSize, CodeExecAsString, ElementType, EntityName, ExecManifest, Identity, InvokerInstanceId, Namespace, PorusParams, ResourceLimit, Secret, Subject, UUID, WhiskAction}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * An object representing a request to schedule a new swap object
 *
 * @param originalAction the fully qualified name of the object requesting the swap space. e.g. {namespace}/action
 * @param source the invoker where the swap request is coming from
 * @param functionActivationId the activation ID belonging to the function which is requesting this swap
 * @param appActivationId the application ID of the application requesting this swap
 */
case class SwapObject(originalAction: String, source: InvokerInstanceId, functionActivationId: ActivationId, appActivationId: ActivationId, mem: ByteSize, user: Identity)

object SwapObject extends DefaultJsonProtocol with SprayJsonSupport {

  implicit val serdes: RootJsonFormat[SwapObject] = jsonFormat6(SwapObject.apply)

  val swapObjectIdentity: Identity = {
    val whiskSystem = "whisk.system"
    val uuid = UUID()
    Identity(Subject(whiskSystem), Namespace(EntityName(whiskSystem), uuid), BasicAuthenticationAuthKey(uuid, Secret()))
  }

  def swapAction(memory: ByteSize = 128.MB): Option[WhiskAction] =
    ExecManifest.runtimesManifest.resolveDefaultRuntime("python:3").map { manifest =>
      new WhiskAction(
        namespace = swapObjectIdentity.namespace.name.toPath,
        name = EntityName(s"swapAction"),
        exec = CodeExecAsString(manifest, """def main(_, action):\n    t = action.get_transport('memory', 'rdma_server')\n    t.serve()""", None),
        porusParams = PorusParams(runtimeType = Some(ElementType.Memory)),
        limits = ActionLimits(resources = ResourceLimit(RuntimeResources(1.0, memory, 512.MB))))
    }

  def createSwapAction(db: EntityStore, action: WhiskAction): Future[Unit] = {
    implicit val tid: TransactionId = TransactionId.loadbalancer
    implicit val ec: ExecutionContext = db.executionContext
    implicit val logging: Logging = db.logging

    WhiskAction
      .get(db, action.docid)
      .flatMap { oldAction =>
        WhiskAction.put(db, action.revision(oldAction.rev), Some(oldAction))(tid, notifier = None)
      }
      .recover {
        case _: NoDocumentException => WhiskAction.put(db, action, old = None)(tid, notifier = None)
      }
      .map(_ => {})
      .andThen {
        case Success(_) => logging.info(this, "swap object action now exists")
        case Failure(e) => logging.error(this, s"error creating swap object action: $e")
      }
  }

  def prepare(entityStore: EntityStore): Unit = {
    swapAction()
      .map {
        // Await the creation of the test action; on failure, this will abort the constructor which should
        // in turn abort the startup of the controller.
        a =>
          Await.result(createSwapAction(entityStore, a), 1.minute)
      }
      .orElse {
        throw new IllegalStateException(
          "cannot create test action for invoker health because runtime manifest is not valid")
      }
  }
}
