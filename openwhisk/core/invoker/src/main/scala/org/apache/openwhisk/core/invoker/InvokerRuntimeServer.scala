package org.apache.openwhisk.core.invoker

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import spray.json.{DefaultJsonProtocol, JsValue}

import scala.concurrent.{ExecutionContext}
import scala.util.{Failure, Success}


case class RuntimeDependencyInvocation(target: String,
                                       action: JsValue,
                                       value: Option[JsValue],
                                       parallelism : Option[Seq[String]],
                                       dependency : Option[Seq[String]]
                                      )
object RuntimeJsonSupport extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val runtimeDependencyInvocationF = jsonFormat5(RuntimeDependencyInvocation.apply)
}

// TODO: currently this one cannot talk to other actors
class InvokerRuntimeServer(producer: MessageProducer,
                           topic: String
) (
  implicit val acterSystem : ActorSystem,
  implicit val executionContext: ExecutionContext,
  implicit val logging: Logging
) {

  import RuntimeJsonSupport._


  def route : Route =

    pathPrefix ("activation" / Segment ) { _activationId : String =>
      // TODO: get info from json object
      concat {

        path ("corunning") {
          complete((500, "not implemented"))
        }

        path ("dependency") {
          post {
            entity(as[RuntimeDependencyInvocation]) { invoke : RuntimeDependencyInvocation  =>

              val activationId = ActivationId.parse(_activationId).get

              val msg = DependencyInvocationMessage(
                action = invoke.target,
                activationId = activationId,
                content = invoke.value,
                parallelism = invoke.parallelism.getOrElse(Seq.empty),
                dependency = invoke.dependency.getOrElse(Seq.empty)
              )

              invokeDependency(msg, topic) match {
                case Left(x)  => complete((500, x))
                case Right(x) => complete((200, x))
              }

            }
          }
        }

      }
    }

  def invokeDependency(msg: DependencyInvocationMessage, topic : String): Either[String, String] = {
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
