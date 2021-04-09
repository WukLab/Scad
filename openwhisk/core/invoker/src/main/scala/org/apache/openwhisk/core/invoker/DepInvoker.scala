package org.apache.openwhisk.core.invoker

import akka.actor.{Actor, ActorRef, ActorSystem, Timers}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common.{Logging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.common.TransactionId.childOf
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.connector.{ActivationMessage, DependencyInvocationMessage, Message, MessageProducer, PartialPrewarmConfig, RunningActivation}
import org.apache.openwhisk.core.containerpool.{Interval, RuntimeResources}
import org.apache.openwhisk.core.database.CacheChangeNotification
import org.apache.openwhisk.core.entity.SizeUnits.MB
import org.apache.openwhisk.core.entity.{ActivationId, ActivationResponse, ByteSize, ExecutableWhiskActionMetaData, FullyQualifiedEntityName, Identity, InvokerInstanceId, TopSchedInstanceId, WhiskAction, WhiskActionMetaData, WhiskActivation, WhiskEntityReference, WhiskFunction}
import org.apache.openwhisk.core.entity.types.{AuthStore, EntityStore}
import org.apache.openwhisk.core.scheduler.{DagExecutor, FinishActivation}
import pureconfig.loadConfigOrThrow
import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import spray.json._

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class DepInvoker(invokerInstance: InvokerInstanceId, topSchedInstanceId: TopSchedInstanceId, msgProducer: MessageProducer)(
  implicit val ex: ExecutionContext,
  implicit val actorSystem: ActorSystem,
  implicit val entityStore: EntityStore,
  implicit val authStore: AuthStore,
  implicit val logging: Logging,
) extends Actor {
  private val topschedTopic = topSchedInstanceId.topic
  protected val controllerPrewarmConfig: Boolean =
    loadConfigOrThrow[Boolean](ConfigKeys.controllerDepPrewarm)

  override def receive: Receive = {
    case e: DependencyInvocationMessage =>
      scheduleDependencyInvocationMessage(e)
    case invoke: ActivationMessage =>
      invoke.appActivationId map { appActivationId =>
        invoke.functionActivationId map { functionActivationId =>
          self ! DependencyInvocationMessage(
            action = invoke.action.qualifiedNameWithLeadingSlash,
            activationId = invoke.activationId,
            content = None,
            dependency = Seq.empty,
            functionActivationId = functionActivationId,
            appActivationId = appActivationId,
            transactionId = invoke.transid
          )
        }
      }
  }

  def scheduleDependencyInvocationMessage(msg: DependencyInvocationMessage): Unit = {
    // We're getting notice of a particular object finishing
    // need to schedule the next object in line
    // First, lookup the application and function this finished object is a part of.
    implicit val transid: TransactionId = childOf(msg.transactionId)
    logging.debug(this, s"scheduling dependency invocation message: ${msg}")
    val f = WhiskAction.get(entityStore, msg.getFQEN().toDocId) flatMap { whiskObject =>
      Identity.get(authStore, whiskObject.fullyQualifiedName(false).path.root).flatMap(identity => {
        // if the object has any dependencies, schedule them all.
        // if the object has no dependencies, it is the end of the function
        // encapsulate the dependency invocation message to function invocation message handler.
        //        logging.debug(this, s"dependency msg with identity: ${identity}")
        whiskObject.parentFunc map { pf =>
          //          logging.debug(this, s"dependency msg with whiskObjectParentFunc: ${pf}")
          whiskObject.relationships map { rel =>
            if (rel.dependents.nonEmpty) {
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
                        topSchedInstanceId,
                        blocking = false,
                        msg.content,
                        action.parameters.initParameters,
                        action.parameters.lockedParameters(msg.content.map(_.fields.keySet).getOrElse(Set.empty)),
                        cause = Some(msg.activationId),
                        WhiskTracerProvider.tracer.getTraceContext(transid),
                        siblings = Some(sibs.toSeq),
                        functionActivationId = Some(msg.functionActivationId),
                        appActivationId = Some(msg.appActivationId),
                        sendResultToInvoker = Some((invokerInstance, msg.activationId)),
                        waitForContent = Some(rel.parents.size), // the number of input messages required to run this object
                      )
                      transid.mark(this, LoggingMarkers.INVOKER_DEP_SCHED)
                      publishToTopBalancer(message)
                        .onComplete(_ => {
                          // once published, prewarm next objects in the DAG...
                          if (controllerPrewarmConfig) {
                            prewarmNextLevelDeps(message, obj)
                          }
                        })
                      Future.successful(message)
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
  private def processFunctionInvocationMessage(func: WhiskEntityReference, invocationMessage: DependencyInvocationMessage, user: Identity, fqen: FullyQualifiedEntityName)(implicit transid: TransactionId): Unit = {
//    logging.debug(this, s"processFunctionInvocationMessage(${invocationMessage.appActivationId}): func: ${func}")
    WhiskFunction.get(entityStore, func.getDocId()) flatMap { wf =>
      val chillen = wf.children getOrElse Seq.empty
//      logging.debug(this, s"processFunctionInvocationMessage(${invocationMessage.appActivationId}): children: ${chillen}")

      if (chillen.isEmpty) {
        // post application response, there are no more functions in the DAG
        transid.mark(this, LoggingMarkers.INVOKER_DEP_FUNC_POST)
        postActivationResponse(invocationMessage.appActivationId, invocationMessage, user, fqen)
      } else {
        // there are more functions to be invoked after this one
//        logging.debug(this, s"processFunctionInvocationMessage(${invocationMessage.appActivationId}): invoking next children")
        chillen map { nextFunc =>
          WhiskFunction.get(entityStore, nextFunc.getDocId()) flatMap { func =>
            DagExecutor.executeFunction(func, entityStore, (obj, funcId, corunning) => {
              val message = ActivationMessage(
                transid,
                FullyQualifiedEntityName(obj.namespace, obj.name, Some(obj.version), obj.binding),
                obj.rev,
                user,
                ActivationId.generate(),
                topSchedInstanceId,
                blocking = false,
                None,
                obj.parameters.initParameters,
                obj.parameters.lockedParameters(invocationMessage.content.map(_.fields.keySet).getOrElse(Set.empty)),
                cause = Some(invocationMessage.activationId),
                WhiskTracerProvider.tracer.getTraceContext(transid),
                siblings = Some(corunning.toSeq.map(id => RunningActivation(id))),
                functionActivationId = Some(funcId),
                appActivationId = Some(invocationMessage.appActivationId),
                waitForContent = Some(1),
              )
              publishToTopBalancer(message)
                .onComplete(_ => {
                  // once published, prewarm next objects in the DAG...
                  if (controllerPrewarmConfig) {
                    logging.debug(this, s"prewarming dependent object ${obj.namespace}, ${obj.name} ||latency: ${Interval.currentLatency()}")
                    prewarmNextLevelDeps(message, obj)
                  }
                })
              Future.successful(message)
            })
          }
        }
      }
      Future.successful(())
    }
  }

  private def publishToTopBalancer(activationMessage: Message): Future[RecordMetadata] = {
    msgProducer.send(topschedTopic, activationMessage)
  }

  private def prewarmNextLevelDeps(activationMessage: ActivationMessage, obj: ExecutableWhiskActionMetaData)(implicit transid: TransactionId): Future[Unit] = {
    Future.successful(obj.relationships.map(relationships => {
      relationships.dependents.map(ref => {
        WhiskActionMetaData.get(entityStore, ref.getDocId()) flatMap { nextObj =>
          Future.successful(nextObj.toExecutableWhiskAction map { nextAction: ExecutableWhiskActionMetaData =>
            val ppc = Some(PartialPrewarmConfig(1000, RuntimeResources(1, ByteSize(256, MB), ByteSize(0, MB))))
            implicit val tid: TransactionId = childOf(activationMessage.transid)
            val newMsg = activationMessage.copy(transid = tid, prewarmOnly = ppc)
            publishToTopBalancer(newMsg)
          })
        }
      })
    }))
  }

  private def postActivationResponse(appId: ActivationId, msg: DependencyInvocationMessage, user: Identity, fqen: FullyQualifiedEntityName)(implicit notifier: Option[CacheChangeNotification] = None): Unit = {
    val finishActivation = FinishActivation(appId, ActivationResponse.success(msg.content))
    publishToTopBalancer(finishActivation)
  }
}

case class DepResultMessage(activationId: ActivationId, nextActivationId: ActivationId, whiskActivation: WhiskActivation) extends Message {
  /**
   * Serializes message to string. Must be idempotent.
   */
  override def serialize: String = DepResultMessage.serdes.write(this).compactPrint
}

object DepResultMessage extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[DepResultMessage] = jsonFormat(
    DepResultMessage.apply,
    "activationId",
    "nextActivationId",
    "whiskActivation")

  def parse(msg: String): Try[DepResultMessage] = Try(serdes.read(msg.parseJson))
}

case class SchedulingDecision(originalActivationId: ActivationId, nextActivationId: ActivationId, invoker: InvokerInstanceId, override val transid: TransactionId) extends Message {
  /**
   * Serializes message to string. Must be idempotent.
   */
  override def serialize: String = SchedulingDecision.serdes.write(this).compactPrint
}

object SchedulingDecision extends DefaultJsonProtocol {
  implicit val serdes: RootJsonFormat[SchedulingDecision] = jsonFormat(
    SchedulingDecision.apply,
    "originalActivationId",
    "nextActivationId",
    "invoker",
    "transid",
  )

  def parse(msg: String): Try[SchedulingDecision] = Try(serdes.read(msg.parseJson))
}

case class ExpirationTick(id: ActivationId)

class ResultWaiter(producer: MessageProducer)(implicit val logging: Logging) extends Actor with Timers {
  val activations: mutable.Map[ActivationId, (Instant, Either[WhiskActivation, SchedulingDecision])] = mutable.Map.empty

  override def receive: Receive = {
    case act: WhiskActivation =>
      activations.get(act.activationId) match {
        case Some((enter: Instant, value: Either[WhiskActivation, SchedulingDecision])) =>
          value match {
            case Left(_) =>
              logging.error(this, s"Got two WhiskActivation messages for same activation ID: ${act.activationId}")
            case Right(sched: SchedulingDecision) =>
              // send to topic
              sendResponse(act, sched, enter)
          }
        case None =>
          // store in map
          addEntry(act.activationId, Left(act))
      }
    case sched: SchedulingDecision =>
      activations.get(sched.originalActivationId) match {
        case Some((enter: Instant, value: Either[WhiskActivation, SchedulingDecision])) =>
          value match {
            case Left(act: WhiskActivation) =>
              // send to topic
              sendResponse(act, sched, enter)
            case Right(sched: SchedulingDecision) =>
              logging.error(this, s"Got two SchedulingDecision messages for same activation ID: ${sched.originalActivationId}")
          }
        case None =>
          // store in map
          addEntry(sched.originalActivationId, Right(sched))
      }
    case exp: ExpirationTick => activations.remove(exp.id)
  }

  def addEntry(id: ActivationId, value: Either[WhiskActivation, SchedulingDecision]): Unit = {
    timers.startSingleTimer(id, ExpirationTick(id), Duration(5, MINUTES))
    activations.put(id, (Instant.now(), value))
  }

  def sendResponse(act: WhiskActivation, sched: SchedulingDecision, enter: Instant): Unit = {
    implicit val transid: TransactionId = sched.transid;
    producer.send(sched.invoker.getDepInputTopic, DepResultMessage(act.activationId, sched.nextActivationId, act))
    val duration = java.time.Duration.between(enter, Instant.now).toMillis
    sched.transid.mark(this, LoggingMarkers.INVOKER_ACTIVATION_LEAVE_RESULT_WAITER)
    if (act.response.isSuccess) {
      logging.debug(this, s"[marker:invoker_resultwaiter_leave:${duration}]")
    } else {
      logging.warn(this, s"invoker activation failed for ${act.activationId}, removing from waiter")
    }
    activations.remove(act.activationId)
    timers.cancel(act.activationId)
  }
}

class ActivationWait()(implicit val logging: Logging) {
  var activationMsg: Option[ActivationMessage] = None
  var results: mutable.ListBuffer[DepResultMessage] = new mutable.ListBuffer[DepResultMessage]()

  def hasAllDeps: Boolean = activationMsg match {
    case Some(msg) =>
      msg.waitForContent match {
        case Some(value) =>
          results.size >= value
        case _ => false
      }
    case _ => false
  }

  def addActivationMsg(msg: ActivationMessage): Unit = {
    activationMsg match {
      case Some(act) =>
        logging.error(this, s"Got two WhiskActivation messages for same activation ID: ${act.activationId}")
      case None =>
        activationMsg = Some(msg)
    }
  }

  def getErrors: Option[String]  = {
    val res = results.filter(!_.whiskActivation.response.isSuccess)
    if (res.isEmpty) {
      None
    } else {
      Some(
        res.map(_.whiskActivation.response.statusCode)
          .map(_.toString)
          .reduce((x1, x2) => s"$x1|$x2")
      )
    }
  }

  def addDepResult(depResultMessage: DepResultMessage): Unit = {
    results += depResultMessage
  }

  def joinDeps(): JsObject = {
    // Always sort results so object gets the same ordering
    val x = results.filter(f => f.whiskActivation.response.result.isDefined)
      .sortBy(s => s.whiskActivation.fullyQualifiedName(false).asString)
      .map(f => (f.whiskActivation.name.toString, f.whiskActivation.response.result.get))
    JsObject(x: _*)
  }
}

class ActivationWaiter(runActor: ActorRef, msgProducer: MessageProducer)(implicit logging: Logging) extends Actor with Timers {
  val activations: mutable.Map[ActivationId, (Instant, ActivationWait)] = mutable.Map.empty

  override def receive: Receive = {
    case act: ActivationMessage =>
      activations.get(act.activationId) match {
        case Some((enter: Instant, value: ActivationWait)) =>
          addDepResult(value, activationMsg = Some(act))
        case None =>
          // store in map
          newContainer(act.activationId, activationMsg = Some(act))
      }
    case res: DepResultMessage =>
      activations.get(res.nextActivationId) match {
        case Some((enter: Instant, value: ActivationWait)) =>
          addDepResult(value, depResultMessage = Some(res))
        case None =>
          // store in map
          newContainer(res.nextActivationId, Some(res))
      }
  }

  def addDepResult(currentWait: ActivationWait, depResultMessage: Option[DepResultMessage] = None, activationMsg: Option[ActivationMessage] = None): Unit = {
    depResultMessage.foreach(dep => {
      currentWait.addDepResult(dep)
    })
    activationMsg.foreach(msg => {
      currentWait.addActivationMsg(msg)
    })

    if (currentWait.hasAllDeps){
      val act = currentWait.activationMsg.get
      val errMsg: Option[String] = currentWait.getErrors
      logging.debug(this, s"activationWait err messages is ${errMsg}")
      if (errMsg.isEmpty) {
        runActor ! act.copy(content = Some(currentWait.joinDeps()), waitForContent = None)
        act.transid.mark(this, LoggingMarkers.INVOKER_ACTIVATION_LEAVE_ACTIVATION_WAITER)
      } else {
        logging.debug(this, "activation failed because not all dependencies were success")
        act.appActivationId.map(appId => {
          msgProducer.send("topsched", FinishActivation(appId, ActivationResponse.applicationError(errMsg.get)))
        })
      }
      activations.remove(act.activationId)
      timers.cancel(act.activationId)
    }
  }

  def newContainer(activationId: ActivationId, res: Option[DepResultMessage] = None, activationMsg: Option[ActivationMessage] = None): Unit = {
    logging.debug(this, s"Adding new activationWait container; ${activationId}")
    val container = new ActivationWait();
    res foreach { dep =>
      container.addDepResult(dep)
    }
    activationMsg foreach { msg =>
      container.addActivationMsg(msg)
    }
    activations.put(activationId, (Instant.now, container))
    timers.startSingleTimer(activationId, ExpirationTick(activationId), Duration(5, MINUTES))
  }
}
