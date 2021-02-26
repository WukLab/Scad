package org.apache.openwhisk.core.topbalancer

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.core.connector.{ActivationMessage, DependencyInvocationMessage, DependencyInvocationMessageContext, MessageConsumer, MessageFeed, MessagingProvider, PartialPrewarmConfig, RunningActivation}
import org.apache.openwhisk.core.containerpool.RuntimeResources
import org.apache.openwhisk.core.database.{ActivationStoreProvider, CacheChangeNotification, UserContext}
import org.apache.openwhisk.core.entity.SizeUnits.MB
import org.apache.openwhisk.core.entity.{ActivationId, ActivationLogs, ActivationResponse, ByteSize, EntityName, EntityPath, ExecutableWhiskActionMetaData, FullyQualifiedEntityName, Identity, Parameters, SemVer, WhiskAction, WhiskActionMetaData, WhiskActivation, WhiskEntityReference, WhiskFunction}
import org.apache.openwhisk.core.entity.types.{AuthStore, EntityStore}
import org.apache.openwhisk.spi.SpiLoader
import pureconfig.loadConfigOrThrow

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
  val authStore: AuthStore,
  val appActivator: ActorRef) extends Actor {
  protected val controllerPrewarmConfig: Boolean =
    loadConfigOrThrow[Boolean](ConfigKeys.controllerDepPrewarm)

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

  val depMsgFeed: ActorRef = context.system.actorOf(Props {
    new MessageFeed(DependencyInvocationMessageContext.DEP_INVOCATION_TOPIC,
      logging,
      depMsgConsumer,
      depMsgConsumer.maxPeek,
      pollDuration,
      processDependencyInvocationMessageBytes)
  })

  override def receive: Receive = {
    case e: DependencyInvocationMessage => scheduleDependencyInvocationMessage(e)
  }

  def processDependencyInvocationMessageBytes(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    DependencyInvocationMessage.parse(raw) match {
      case Success(p: DependencyInvocationMessage) =>
        depMsgFeed ! MessageFeed.Processed
        self ! p
        case Failure(t) =>
        depMsgFeed ! MessageFeed.Processed
        logging.error(this, s"failed processing message: $raw with $t")
    }
  }

  def scheduleDependencyInvocationMessage(msg: DependencyInvocationMessage): Unit = {
    // We're getting notice of a particular object finishing
    // need to schedule the next object in line
    // First, lookup the application and function this finished object is a part of.
    logging.debug(this, s"scheduling dependency invocation message: ${msg}")
    val f = WhiskAction.get(entityStore, msg.getFQEN().toDocId) flatMap { whiskObject =>
      Identity.get(authStore, whiskObject.fullyQualifiedName(false).path.root).flatMap(identity => {
        // if the object has any dependencies, schedule them all.
        // if the object has no dependencies, it is the end of the function
        // encapsulate the dependency invocation message to function invocation message handler.
        logging.debug(this, s"dependency msg with identity: ${identity}")
        whiskObject.parentFunc map { pf =>
          logging.debug(this, s"dependency msg with whiskObjectParentFunc: ${pf}")
          whiskObject.relationships map { rel =>
            logging.debug(this, s"dependency msg with whisk object relationship: ${rel}")
            if (rel.dependents.isEmpty) {
              processFunctionInvocationMessage(pf, msg, identity, msg.getFQEN())
              // This will result in the next function in the chain being triggered.
            } else {
              // schedule the next set of dependencies
              // generate the new activationIds
              // Use RunningActivation type so invokers can update the DB with network address
              val siblingActivations: Set[ActivationId] = rel.dependents.map(_ => ActivationId.generate()).toSet
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
                        functionActivationId = Some(msg.functionActivationId),
                        appActivationId = Some(msg.appActivationId)
                      )
                      logging.debug(this, s"scheduling dependent object ${action.namespace}/${action.name}")
                      val publishedMsg = topBalancer.publish(obj, message)
                      // once published, prewarm next objects in the DAG...
                      publishedMsg.onComplete(_ => {
                        if (controllerPrewarmConfig) {
                          logging.debug(this, s"prewarming dependent object ${obj.namespace}, ${obj.name}}")
                          prewarmNextLevelDeps(message, obj)
                        }
                      })
                      publishedMsg
                    } get
                  }
              }
            }
          }
        }
        Future.successful(())
      })
    }
  }

  /**
   * Begin the activation of the next function in the application DAG.
   *
   * @param func the function whose activation just completed
   * @param invocationMessage the final invocation message from the object DAG
   */
  private def processFunctionInvocationMessage(func: WhiskEntityReference, invocationMessage: DependencyInvocationMessage, user: Identity, fqen: FullyQualifiedEntityName): Unit = {
    logging.debug(this, s"processFunctionInvocationMessage(${invocationMessage.appActivationId}): func: ${func}")
    WhiskFunction.get(entityStore, func.getDocId()) flatMap { wf =>
      val chillen = wf.children getOrElse Seq.empty
      logging.debug(this, s"processFunctionInvocationMessage(${invocationMessage.appActivationId}): chillen: ${chillen}")

      if (chillen.isEmpty) {
        // post application response, there are no more functions in the DAG
        logging.debug(this, s"processFunctionInvocationMessage(${invocationMessage.appActivationId}): postActivationResponse: ")
        postActivationResponse(invocationMessage.appActivationId, invocationMessage, user, fqen)
      } else {
        // there are more functions to be invoked after this one
        logging.debug(this, s"processFunctionInvocationMessage(${invocationMessage.appActivationId}): invoking next chillen")
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
                  appActivationId = Some(invocationMessage.appActivationId)
                )
                val publishedMsg = topBalancer.publish(obj, message)
                // once published, prewarm next objects in the DAG...
                publishedMsg.onComplete(_ => {
                  if (controllerPrewarmConfig) {
                    prewarmNextLevelDeps(message, obj)
                  }
                })
                publishedMsg
            })
          }
        }
      }
      Future.successful(())
    }
  }

  private def prewarmNextLevelDeps(activationMessage: ActivationMessage, obj: ExecutableWhiskActionMetaData): Future[Unit] = {
    Future.successful(obj.relationships.map(relationships => {
      relationships.dependents.map(ref => {
        WhiskActionMetaData.get(entityStore, ref.getDocId()) flatMap { nextObj =>
          Future.successful(nextObj.toExecutableWhiskAction map { nextAction: ExecutableWhiskActionMetaData =>
            val ppc = Some(PartialPrewarmConfig(1000, RuntimeResources(1, ByteSize(256, MB), ByteSize(0, MB))))
            val newMsg = activationMessage.copy(prewarmOnly = ppc)
            topBalancer.publish(nextAction, newMsg)
          })
        }
      })
    }))
  }

  private def postActivationResponse(appId: ActivationId, msg: DependencyInvocationMessage, user: Identity, fqen: FullyQualifiedEntityName)(implicit notifier: Option[CacheChangeNotification] = None): Unit = {
    val finishActivation = FinishActivation(appId, ActivationResponse.success(msg.content))
    appActivator ! finishActivation
  }
}

object DependencyForwarding {

}

case class IncompleteActivation(id: ActivationId, startTime: Instant, entityPath: EntityPath, actionName: EntityName, user: Identity)
case class FinishActivation(id: ActivationId, response: ActivationResponse,
                            logs: ActivationLogs = ActivationLogs(),
                            version: SemVer = SemVer(),
                            annotations: Parameters = Parameters())

/**
 * This actor receives messages which allow it to track and store activations for "applications"
 *
 *
 * an IncompleteActivation will store the activation in memory temporarily. a FinishActivation
 * message will record and store the final activation result in the database
 *
 * @param actorSystem
 * @param materializer
 * @param logging
 * @param ec
 * @param cacheChangeNotification
 */
case class AppActivator()(implicit val actorSystem: ActorSystem, materializer: ActorMaterializer, val logging: Logging, ec: ExecutionContext, cacheChangeNotification: Option[CacheChangeNotification] = None) extends Actor {

  implicit val transid: TransactionId = TransactionId.appActivator
  private val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, materializer, logging)

  val incompleteActivations: collection.mutable.Map[ActivationId, IncompleteActivation] = collection.mutable.Map()

   def receive: Receive = {
    case info: IncompleteActivation =>
      incompleteActivations.put(info.id, info)
    case fin: FinishActivation =>
      incompleteActivations.get(fin.id).map(incomplete => {
        val now = Instant.now()
        val act = WhiskActivation(
          incomplete.entityPath,
          incomplete.actionName,
          incomplete.user.subject,
          fin.id,
          incomplete.startTime,
          now,
          response = fin.response,
          duration = Some(now.toEpochMilli - incomplete.startTime.toEpochMilli)
        )
        activationStore.store(act, UserContext(incomplete.user)) flatMap { docInfo =>
          logging.debug(this, s"application activation ${fin.id} stored with $docInfo")
          Future.successful(())
        } recoverWith {
          case t: Throwable =>
            logging.warn(this, s"application activation store ${fin.id} FAILED with $t")
            Future.successful(())
        }
        incompleteActivations.remove(fin.id)
      })
  }
}
