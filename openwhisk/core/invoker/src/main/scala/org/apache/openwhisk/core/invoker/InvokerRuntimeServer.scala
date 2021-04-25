package org.apache.openwhisk.core.invoker

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity._
import spray.json._

import scala.concurrent.ExecutionContext

case class RuntimeDependencyInvocation(target: String,
                                       value: Option[JsObject],
                                       parallelism : Option[Seq[String]],
                                       dependency : Option[Seq[String]],
                                       functionActivationId: ActivationId,
                                       appActivationId: ActivationId,
                                      )
object RuntimeDependencyInvocation extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val serdes: RootJsonFormat[RuntimeDependencyInvocation] = jsonFormat6(RuntimeDependencyInvocation.apply)
}

class InvokerRuntimeServer() (
  implicit val actorSystem : ActorSystem,
  implicit val executionContext: ExecutionContext,
  implicit val logging: Logging
) {


  def route : Route =

    pathPrefix ("activation" / Segment ) { _activationId : String =>
      // TODO: get info from json object
      concat {

        path ("corunning") {
          complete((500, s"not implemented ${_activationId}"))
        }

        path ("dependency") {
          logging.debug(this, "Got dependency message!")
          post {
            entity(as[RuntimeDependencyInvocation]) { invoke =>
              logging.warn(this, s"Get Dependency Request from (unparsed) ${_activationId}: $invoke. No-op.")
              complete((200, "no-op"))
            }
          }
        }
      }
    }
}
