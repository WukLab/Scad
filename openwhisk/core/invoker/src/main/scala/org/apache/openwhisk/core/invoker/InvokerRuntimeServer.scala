package org.apache.openwhisk.core.invoker

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import spray.json._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

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

// TODO: currently this one cannot talk to other actors
class InvokerRuntimeServer(producer: MessageProducer,
                           topic: String
) (
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
              logging.info(this, s"Get Dependency Request from (unparsed) ${_activationId}: $invoke")
              ActivationId.parse(_activationId).toEither match {
                case Left(throwable) =>
                  logging.warn(this, s"aid parse failed: ${_activationId}: ${throwable}")
                  complete((500, throwable))
                case Right(aid) =>
                  invokeDependency(topic)(invoke, aid) match {
                    case Left(x)  =>
                      logging.warn(this, s"dependency request failed: ${x}")
                      complete((500, x))
                    case Right(x) =>
                      complete((200, x))
                  }
              }
            }
          }
        }
      }
    }

  def invokeDependency(topic : String)
                      (invoke: RuntimeDependencyInvocation, activationId: ActivationId): Either[String, String] = {
    val msg = DependencyInvocationMessage(
      action = invoke.target,
      activationId = activationId,
      content = invoke.value,
      dependency = invoke.dependency.getOrElse(Seq.empty),
      functionActivationId = invoke.functionActivationId,
      appActivationId = invoke.appActivationId,
    )
    // Send a message
    Await.result(producer.send(topic = topic, msg) flatMap { res =>
      logging.debug(this, s"dependency message sent successfully ${msg}") 
      Future.successful(Right(res.toString))
    } recoverWith {
      case exception: Throwable =>
        val err = s"failed to post action dependency ${msg.activationId}, ${msg.toString} , ${exception.toString}"
        logging.error(this, err)
        Future.successful(Left(err))
    }, 30.seconds)
  }


}
