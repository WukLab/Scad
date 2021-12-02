package org.apache.openwhisk.core.invoker

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.core.connector.{ActivationMessage, MessagingProvider}
import org.apache.openwhisk.core.containerpool.{ActorProxyAddressBook, LibdTransportConfig, ProxyAddress, RuntimeResources, TransportAddress, TransportRequest}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.swap.SwapObject
import pureconfig.loadConfigOrThrow
import spray.json._

import scala.concurrent._

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

class InvokerRuntimeServer(config: WhiskConfig, msgProvider: MessagingProvider,
                           addressBook: Option[ActorProxyAddressBook],
                           containerPool: ActorRef
                          )(
  implicit val actorSystem : ActorSystem,
  implicit val executionContext: ExecutionContext,
  implicit val logging: Logging
) {
  private val rackInt: Int = loadConfigOrThrow[Int](ConfigKeys.invokerRack)
  private val schedId = new RackSchedInstanceId(rackInt, RuntimeResources.none())
  private val msgProducer = msgProvider.getProducer(config)

  // compute is not yet implemented, type parameter will eventually change
  // only mem is currently supported
  case class LaunchCommand(swap: Option[SwapObject], compute: Option[JsObject])

  object LaunchCommand extends DefaultJsonProtocol with SprayJsonSupport {
    implicit val serdes: RootJsonFormat[LaunchCommand] = jsonFormat2(LaunchCommand.apply)
  }

  def route : Route =
    pathPrefix ("activation" / Segment ) { _activationId : String =>
      val activationId = ActivationId.parse(_activationId).toOption
      concat {

        // Release a related memory segment
        path ("transport" / Segment) { _transport : String =>
          /**
           * delete, remove a existing transport
           */
          delete {
            val rep = for {
              book <- addressBook
              aid <- activationId
            } yield book.release(ProxyAddress(aid, _transport))

            complete((200, "ok"))
          }

          /**
           * Ppst, create a new transport
           * currently we use this path to create new memory objects
           */
          post {
            entity(as[LaunchCommand]) { cmd =>
              cmd.swap.map({ obj =>
                scheduleSwap(obj)
              }) match {
                case Some(value) =>
                  // TODO option to future
                  value.foreach { destAid =>
                    for {
                      book <- addressBook
                      aid <- activationId
                    } yield {
                      val src = ProxyAddress(aid, _transport)
                      val dst = ProxyAddress(destAid, "memory")
                      book.prepareReply(src, dst)

                      val request = TransportRequest.getMessage(aid)
                      val configMsg = LibdTransportConfig(aid, request, TransportAddress.empty)
                      containerPool ! configMsg
                    }
                  }
                  complete((200, "ok"))
                case None =>
                  complete((500, "no swap"))
              }
            }
          }
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

  protected def activationMsgFromObj(swap: SwapObject): ActivationMessage = {
    ActivationMessage(
      TransactionId(TransactionId.generateTid()),
      EntityPath(swap.originalAction).toFullyQualifiedEntityName,
      DocRevision.empty,
      swap.user,
      ActivationId.generate(),
      schedId,
      blocking = false,
      content = None,
      functionActivationId = Some(swap.functionActivationId),
      appActivationId = Some(swap.appActivationId),
      swapFrom = Some(swap.source)
    )
  }

  protected def scheduleSwap(obj: SwapObject): Future[ActivationId] = {
    val msg = activationMsgFromObj(obj)
    logging.debug(this, s"scheduling additional mem object activation for ${obj.appActivationId}::${obj.functionActivationId} with activation ${msg.activationId}")
    msgProducer.send(schedId.schedTopic, msg).map(_ => msg.activationId).map(_ => msg.activationId)
  }
}
