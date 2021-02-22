package org.apache.openwhisk.core.topbalancer

import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.database.NoDocumentException
import org.apache.openwhisk.core.entity.types.EntityStore
import org.apache.openwhisk.core.entity.{ActionLimits, ActivationId, BasicAuthenticationAuthKey, CodeExecAsString, EntityName, ExecManifest, ExecutableWhiskActionMetaData, Identity, InvokerInstanceId, Namespace, ResourceLimit, Secret, Subject, UUID, WhiskAction}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

case class SwapObject(originalAction: String, source: InvokerInstanceId, functionActivationId: ActivationId, appActivationId: ActivationId)

object SwapObject extends DefaultJsonProtocol {

  implicit val serdes: RootJsonFormat[SwapObject] = jsonFormat4(SwapObject.apply)

  val swapObjectIdentity: Identity = {
    val whiskSystem = "whisk.system"
    val uuid = UUID()
    Identity(Subject(whiskSystem), Namespace(EntityName(whiskSystem), uuid), BasicAuthenticationAuthKey(uuid, Secret()))
  }

  def swapAction(): Option[WhiskAction] =
    ExecManifest.runtimesManifest.resolveDefaultRuntime("nodejs:default").map { manifest =>
      new WhiskAction(
        namespace = swapObjectIdentity.namespace.name.toPath,
        name = EntityName(s"swapAction"),
        exec = CodeExecAsString(manifest, """function main(params,action) {let t = action.get_transport('server','rdma_server');let ret = t.serve();return {payload: 'serve'};}""", None),
        limits = ActionLimits(resources = ResourceLimit(ResourceLimit.MIN_RESOURCES)))
    }
  def swapActionMetadata(): Option[ExecutableWhiskActionMetaData] = ???

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
