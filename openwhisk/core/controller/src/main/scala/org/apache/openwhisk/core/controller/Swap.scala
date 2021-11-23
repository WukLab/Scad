package org.apache.openwhisk.core.controller

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, OK}
import akka.http.scaladsl.server.{Directive, Directive1, Directives, RequestContext, Route, RouteResult}
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.database.{ActivationStore, CacheChangeNotification}
import org.apache.openwhisk.core.entitlement.Privilege.PUT
import org.apache.openwhisk.core.entitlement.{Collection, Privilege, Resource}
import org.apache.openwhisk.core.entity.{ActivationId, ControllerInstanceId, DocRevision, EntityName, EntityPath, Identity, WhiskAction, WhiskActionMetaData}
import org.apache.openwhisk.core.entity.types.EntityStore
import org.apache.openwhisk.core.swap.SwapObject
import spray.json.JsObject

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * This represents the API for scheduling new swap objects.
 */
trait SwapApi extends Directives with AuthorizedRouteProvider with AuthenticatedRouteProvider with WhiskServices {
  services: WhiskServices =>

  protected val topsched: ControllerInstanceId

  protected override val collection: Collection = Collection(Collection.SWAP)

  /** An actor system for timed based futures. */
  protected implicit val actorSystem: ActorSystem

  /** Database service to CRUD actions. */
  protected val entityStore: EntityStore

  /** Notification service for cache invalidation. */
  protected implicit val cacheChangeNotification: Some[CacheChangeNotification]

  /** Database service to get activations. */
  protected val activationStore: ActivationStore

  protected implicit val logging: Logging

  override protected lazy val entityOps: Directive[Unit] = get | put | post | delete

  /** Entity normalizer to JSON object. */
  import RestApiCommons.emptyEntityToJsObject

  /** JSON response formatter. */
//  import RestApiCommons.jsonDefaultResponsePrinter


  /** Extracts and validates entity name from the matched path segment. */
  override protected def entityname(segment: String): Directive1[String] = extract(_ => segment)

  override protected def innerRoutes(user: Identity, ns: EntityPath)(implicit transid: TransactionId): Route = {
    (entityPrefix & entityOps & requestMethod) { (segment, m) =>
      // matched /namespace/collection/entity
      (entityname(segment) & pathEnd) { name =>
        authorizeAndDispatch(m, user, Resource(ns, collection, Some(name)))
      }
    }
  }

  protected def dispatchOp(user: Identity, op: Privilege, resource: Resource)(
    implicit transid: TransactionId): RequestContext => Future[RouteResult] = {
    resource.entity match {
      case Some(EntityName(name)) =>
        op match {
          case PUT =>
            entity(as[JsObject]) { swap =>
              val swapObject = SwapObject.serdes.read(swap)
              logging.debug(this, s"scheduling new swap for $name:$swapObject")
              onComplete(scheduleSwap(swapObject, user)) {
                case Success(id) => complete(OK, id.toJsObject)
                case Failure(exception) => complete(InternalServerError, exception)
              }
            }
          case _ => reject
        }
      case _ => reject
    }
  }

  protected def activationMsgFromObj(swap: SwapObject, user: Identity)(implicit transid: TransactionId): ActivationMessage = {
    ActivationMessage(
      transid,
      EntityPath(swap.originalAction).toFullyQualifiedEntityName,
      DocRevision.empty,
      user,
      ActivationId.generate(),
      loadBalancer.id,
      blocking = false,
      content = None,
      functionActivationId = Some(swap.functionActivationId),
      appActivationId = Some(swap.appActivationId),
      swapFrom = Some(swap.source)
    )
  }

  protected def scheduleSwap(obj: SwapObject, user: Identity)(implicit transid: TransactionId): Future[ActivationId] = {
    val z: Future[ActivationId] = SwapObject.swapAction(obj.mem) flatMap { exec =>
      val y = WhiskActionMetaData.serdes.read(WhiskAction.serdes.write(exec)).toExecutableWhiskAction flatMap { action =>
        val msg = activationMsgFromObj(obj, user)
        logging.debug(this, s"scheduling swap activation for ${obj.appActivationId}::${obj.functionActivationId} with activation ${msg.activationId}")
        val x: Future[ActivationId] = loadBalancer.publish(action, msg).flatten.flatMap({
          case Left(id) => Future.successful(id)
          case Right(act) => Future.successful(act.activationId)
        })
        Some(x)
      }
      y
    } getOrElse Future.failed(new Exception("Fails to get swap action"))
    z
  }

}

