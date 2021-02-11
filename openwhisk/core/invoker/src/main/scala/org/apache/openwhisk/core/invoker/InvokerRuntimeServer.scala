package org.apache.openwhisk.core.invoker

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import spray.json._

import scala.concurrent.{ExecutionContext}
import scala.util.{Failure, Success}


case class RuntimeDependencyInvocation(target: String,
                                       value: Option[JsObject],
                                       parallelism : Option[Seq[String]],
                                       dependency : Option[Seq[String]]
                                      )
object RuntimeJsonSupport extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val runtimeDependencyInvocationF = jsonFormat4(RuntimeDependencyInvocation.apply)
}

// TODO: currently this one cannot talk to other actors
class InvokerRuntimeServer(producer: MessageProducer,
                           topic: String
) (
  implicit val actorSystem : ActorSystem,
  implicit val executionContext: ExecutionContext,
  implicit val logging: Logging
) {

  import RuntimeJsonSupport._


  def route : Route =

    pathPrefix ("activation" / Segment ) { _activationId : String =>
      // TODO: get info from json object
      concat {

        path ("corunning") {
          complete((500, s"not implemented ${_activationId}"))
        }

        path ("dependency") {
          post {
            entity(as[RuntimeDependencyInvocation]) { invoke =>

              // TODO: change this get
              val activationId = ActivationId.parse(_activationId).get

              logging.info(this, s"Get Dependency Request from $activationId: $invoke")

              invokeDependency(topic)(invoke, activationId) match {
                case Left(x)  => complete((500, x))
                case Right(x) => complete((200, x))
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
      dependency = invoke.dependency.getOrElse(Seq.empty)
    )
    // Send a message
    val res = producer.send(topic = topic, msg).value
    res.getOrElse(Left("Fail")) match {
      case Failure(exception) =>
        val err = s"failed to post action dependency ${msg.activationId}, ${msg.toString} , ${exception.toString()}"
        logging.error(this, err)
        Left(err)

      case Success(value)     =>
        Right(value.toString())
    }

  }


}
