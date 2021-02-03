package org.apache.openwhisk.core.topbalancer

import akka.actor.{Actor, ActorSystem}
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector.{ActivationMessage, DependencyInvocationMessage, DependencyInvocationMessageContext, MessageConsumer, MessageFeed, MessagingProvider, RunningActivation}
import org.apache.openwhisk.core.database.{ActivationStoreProvider, CacheChangeNotification, UserContext}
import org.apache.openwhisk.core.entity.{ActivationId, ActivationLogs, ActivationResponse, FullyQualifiedEntityName, Identity, Parameters, SemVer, WhiskAction, WhiskActionMetaData, WhiskActivation, WhiskEntityReference, WhiskFunction}
import org.apache.openwhisk.core.entity.types.{AuthStore, EntityStore}
import org.apache.openwhisk.spi.SpiLoader

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

class DependencyForwarding(whiskConfig: WhiskConfig,
                           topBalancer: TopBalancer)(
  implicit val actorSystem: ActorSystem,
  val logging: Logging,
  val messagingProvider: MessagingProvider,
  val ec: ExecutionContext,
  val entityStore: EntityStore,
  val authStore: AuthStore) extends Actor {

  implicit val transid: TransactionId = TransactionId.depInvocation

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  private val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, materializer, logging)

  val depMsgConsumer: MessageConsumer = messagingProvider.getConsumer(
    whiskConfig,
    DependencyInvocationMessageContext.DEP_INVOCATION_TOPIC,
    DependencyInvocationMessageContext.DEP_INVOCATION_TOPIC, maxPeek = 128
  )

  val pollDuration: FiniteDuration = 1.second
  val depMsgFeed: MessageFeed = new MessageFeed(DependencyInvocationMessageContext.DEP_INVOCATION_TOPIC,
    logging,
    depMsgConsumer,
    depMsgConsumer.maxPeek,
    pollDuration,
    processDependencyInvocationMessageBytes)

  override def receive: Receive = {
    case e: DependencyInvocationMessage => scheduleDependencyInvocationMessage(e)
  }

  def processDependencyInvocationMessageBytes(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    DependencyInvocationMessage.parse(raw) match {
      case Success(p: DependencyInvocationMessage) =>
        self ! p
      case Failure(t) =>
        logging.error(this, s"failed processing message: $raw with $t")
    }
  }

  def scheduleDependencyInvocationMessage(msg: DependencyInvocationMessage): Unit = {
    // We're getting notice of a particular object finishing
    // need to schedule the next object in line
    // First, lookup the application and function this finished object is a part of.
    val f = WhiskAction.get(entityStore, msg.getFQEN().toDocId) flatMap { whiskObject =>
      Identity.get(authStore, whiskObject.fullyQualifiedName(false).path.root).flatMap(identity => {
        // if the object has any dependencies, schedule them all.
        // if the object has no dependencies, it is the end of the function
        // encapsulate the dependency invocation message to function invocation message handler.
        whiskObject.parentFunc map { pf =>
          whiskObject.relationships map { rel =>
            if (rel.dependents.isEmpty) {
              // TODO(zac) send function completion message
              processFunctionInvocationMessage(pf, msg, identity)
              // This will result in the next function in the chain being triggered.
            } else {
              // schedule the next set of dependencies
              // generate the new activationIds
              // Use RunningActivation type so invokers can update the DB with network address
              val siblingActivations: Set[ActivationId] = rel.dependents.map( _ => ActivationId.generate()).toSet
              // for all of the next objects to be activated, get the action metadata and publish
              // to the load balancer
              rel.dependents zip siblingActivations map {
                case (child, newActivationId) =>
                  WhiskActionMetaData.get(entityStore, child.getDocId()) flatMap { action =>
                    action.toExecutableWhiskAction map { obj =>
                      val sibs = (siblingActivations - newActivationId).map(x => RunningActivation(x))
                      val message = ActivationMessage(
                        transid,
                        FullyQualifiedEntityName(action.namespace, action.name, Some(action.version), action.binding),
                        action.rev,
                        identity,
                        newActivationId, // activation id created here
                        topBalancer.id,
                        blocking = false,
                        msg.content,
                        action.parameters.initParameters,
                        action.parameters.lockedParameters(msg.content.map(_.fields.keySet).getOrElse(Set.empty)),
                        cause = Some(msg.activationId),
                        WhiskTracerProvider.tracer.getTraceContext(transid),
                        siblings = Some(sibs.toSeq),
                        functionActivationId = msg.functionActivationId,
                        appActivationId = msg.appActivationId
                      )
                      topBalancer.publish(obj, message)
                    } get
                  }
              }
            }
          }
        }
        Future.successful(())

      })
    }
    ()
  }

  /**
   * Begin the activation of the next function in the application DAG.
   *
   * @param func the function whose activation just completed
   * @param invocationMessage the final invocation message from the object DAG
   */
  private def processFunctionInvocationMessage(func: WhiskEntityReference, invocationMessage: DependencyInvocationMessage, user: Identity): Unit = {
    WhiskFunction.get(entityStore, func.getDocId()) flatMap { wf =>
      val chillen = wf.children getOrElse Seq.empty
      if (chillen.isEmpty) {
        // post application response, there are no more functions in the DAG
        postActivationResponse(invocationMessage.appActivationId.get, invocationMessage, user)
      } else {
        // there are more functions to be invoked after this one
        chillen map { nextFunc =>
          WhiskFunction.get(entityStore, nextFunc.getDocId()) flatMap { func =>
            DagExecutor.executeFunction(func, entityStore, (obj, funcId, corunning) => {
                val message = ActivationMessage(
                  transid,
                  FullyQualifiedEntityName(obj.namespace, obj.name, Some(obj.version), obj.binding),
                  obj.rev,
                  user,
                  ActivationId.generate(),
                  topBalancer.id,
                  blocking = false,
                  invocationMessage.content,
                  obj.parameters.initParameters,
                  obj.parameters.lockedParameters(invocationMessage.content.map(_.fields.keySet).getOrElse(Set.empty)),
                  cause = Some(invocationMessage.activationId),
                  WhiskTracerProvider.tracer.getTraceContext(transid),
                  siblings = Some(corunning.toSeq.map(id => RunningActivation(id))),
                  functionActivationId = Some(funcId),
                  appActivationId = invocationMessage.appActivationId
                )
                topBalancer.publish(obj, message)
            })
          }
        }
      }
      Future.successful(())
    }
  }

  private def postActivationResponse(appId: ActivationId, msg: DependencyInvocationMessage, user: Identity)(implicit notifier: Option[CacheChangeNotification] = None): Unit = {
    val activation = WhiskActivation(
      msg.getFQEN().path,
      msg.getFQEN().name,
      user.subject,
      appId,
      Instant.now(),
      Instant.now(),
      cause = None,
      response = ActivationResponse.success(msg.content),
      logs = ActivationLogs(),
      version = SemVer(),
    annotations = Parameters(),
    duration = None,
    parent = None
    )
    activationStore.store(activation, UserContext(user))
  }
}

object DependencyForwarding {



}
