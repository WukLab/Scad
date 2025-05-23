package org.apache.openwhisk.core.scheduler

import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.connector.{Message, ParallelismInfo, RunningActivation}
import org.apache.openwhisk.core.entity.{ActivationId, ActivationLogs, ActivationResponse, EntityName, EntityPath, ExecutableWhiskActionMetaData, Identity, Parameters, SemVer, WhiskActionMetaData, WhiskEntityReference, WhiskFunction}
import org.apache.openwhisk.core.entity.types.EntityStore
import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object DagExecutor {
  /**
   * Invoke a DAG function.
   *
   * @param func the whisk function which is to be invoked
   * @param entityStore the entity store
   * @param invocation a function which takes three arguments (the function, the new function's activation id, and the
   *                   set of corunning object  activation IDs for the beginning of this new function invocation
   * @param transid
   * @param ex
   * @param logging
   * @tparam T
   * @return
   */
  def executeFunction[T](func: WhiskFunction, entityStore: EntityStore,
                         invocation: (ExecutableWhiskActionMetaData, ActivationId, Seq[RunningActivation], RunningActivation) => Future[T],
                         useRdma: Boolean,
                        )(implicit transid: TransactionId, ex: ExecutionContext, logging: Logging): Future[Unit] = {
    // Start the first objects of the first functions within the application.
    val funcId = ActivationId.generate()
    func.lookupObjectMetadata(entityStore) map { objs =>
      // Find all objects which don't aren't dependencies for any other part of the DAG
      // (the starting set of functions, e.g. corunning)
      val objMap: Map[WhiskEntityReference, WhiskActionMetaData] = objs.map(o => o.getReference() -> o).toMap
      val startingObjs = objMap.keySet.to[collection.mutable.Set]
      objMap.values.foreach { o =>
        o.porusParams.relationships map { r =>
          startingObjs --= r.dependents
        }
      }
      if (startingObjs.size <= 0) {
        return Future.failed(new RuntimeException("Object Application not a DAG"))
      }
      // this one should be immutable, and we will make a copy for each new object we plan to schedule with that
      // particular object's activation id removed from the copied set.
      val objSeq = startingObjs.toSeq
      val activations: Seq[RunningActivation] = objSeq.map(o => RunningActivation(o.toFQEN().toString, ActivationId.generate(), ParallelismInfo(0, 1), useRdma))
      val siblingSet: Set[RunningActivation] = activations.toSet
      val x = objSeq zip activations map { obj =>
        (objMap(obj._1).toExecutableWhiskAction, obj._2)
      } map { execObj =>
        invocation.apply(execObj._1.get, funcId, (siblingSet - execObj._2).toSeq, execObj._2)
      }
    } recoverWith {
      case t: Throwable =>
        logging.error(this, s"failed to recover when launching DAG objects: $t")
        t.printStackTrace()
        Future.failed(t)
    }
  }
}

case class IncompleteActivation(id: ActivationId, startTimeEpochMillis: Long, entityPath: EntityPath, actionName: EntityName, user: Identity) extends Message {
  /**
   * Serializes message to string. Must be idempotent.
   */
  override def serialize: String = IncompleteActivation.serdes.write(this).compactPrint
}

object IncompleteActivation extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[IncompleteActivation] = jsonFormat5(IncompleteActivation.apply)
}

case class FinishActivation(id: ActivationId,
                            response: ActivationResponse,
                            logs: ActivationLogs = ActivationLogs(),
                            version: SemVer = SemVer(),
                            annotations: Parameters = Parameters()) extends Message {
  /**
   * Serializes message to string. Must be idempotent.
   */
  override def serialize: String = FinishActivation.serdes.write(this).compactPrint
}

object FinishActivation extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[FinishActivation] = jsonFormat(FinishActivation.apply,
    "id",
    "response",
    "logs",
    "version",
    "annotations")

  def parse(msg: String): Try[FinishActivation] = Try(serdes.read(msg.parseJson))
}