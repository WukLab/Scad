package org.apache.openwhisk.core.topbalancer

import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.entity.types.EntityStore
import org.apache.openwhisk.core.entity.{ActivationId, ExecutableWhiskActionMetaData, WhiskActionMetaData, WhiskEntityReference, WhiskFunction}

import scala.concurrent.{ExecutionContext, Future}

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
                         invocation: (ExecutableWhiskActionMetaData, ActivationId, Set[ActivationId]) => Future[T])(
    implicit transid: TransactionId, ex: ExecutionContext, logging: Logging): Future[Unit] = {
    // Start the first objects of the first functions within the application.
    val funcId = ActivationId.generate()
    func.lookupObjectMetadata(entityStore) map { objs =>
      // Find all objects which don't aren't dependencies for any other part of the DAG
      // (the starting set of functions, e.g. corunning)
      val objMap: Map[WhiskEntityReference, WhiskActionMetaData] = objs.map(o => o.getReference() -> o).toMap
      val startingObjs = objMap.keySet.to[collection.mutable.Set]
      objMap.values.foreach { o =>
        o.relationships map { r =>
          startingObjs --= r.dependents
        }
      }
      if (startingObjs.size <= 0) {
        return Future.failed(new RuntimeException("Object Application not a DAG"))
      }
      // this one should be immutable, and we will make a copy for each new object we plan to schedule with that
      // particular object's activation id removed from the copied set.
      val activationIds: Set[ActivationId] = startingObjs.map(_ => ActivationId.generate()).toSet
      val x = startingObjs zip activationIds map { obj =>
        (objMap(obj._1).toExecutableWhiskAction, obj._2)
      } map { execObj =>
        invocation.apply(execObj._1.get, funcId, activationIds - execObj._2)
      }
    } recoverWith {
      case t: Throwable => {
        logging.debug(this, s"failed to recover when launching DAG objects: $t")
        Future.failed(t)
      }
    }
  }
}