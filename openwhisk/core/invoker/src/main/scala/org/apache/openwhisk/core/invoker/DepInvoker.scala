package org.apache.openwhisk.core.invoker

import akka.actor.{Actor, ActorRef, ActorSystem, Timers}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common.{Logging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.common.TransactionId.childOf
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.connector.{ActivationMessage, DependencyInvocationMessage, Message, MessageProducer, ParallelismInfo, PartialPrewarmConfig, RunningActivation}
import org.apache.openwhisk.core.containerpool.RuntimeResources
import org.apache.openwhisk.core.database.CacheChangeNotification
import org.apache.openwhisk.core.entity.SizeUnits.MB
import org.apache.openwhisk.core.entity.{ActivationId, ActivationResponse, ByteSize, ExecutableWhiskActionMetaData, FullyQualifiedEntityName, Identity, InvokerInstanceId, TopSchedInstanceId, WhiskAction, WhiskActionMetaData, WhiskActionRelationship, WhiskActivation}
import org.apache.openwhisk.core.entity.types.{AuthStore, EntityStore}
import org.apache.openwhisk.core.scheduler.FinishActivation
import pureconfig.loadConfigOrThrow
import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import spray.json._

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}
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
      // only invoke dependencies if the object is the original (0th) index. All others are just copies which would
      // duplicate work
      if (invoke.parallelismIdx.index == 0) {
        invoke.appActivationId map { appActivationId =>
          invoke.functionActivationId map { functionActivationId =>
            self ! DependencyInvocationMessage(
              action = invoke.action.qualifiedNameWithLeadingSlash,
              activationId = invoke.activationId,
              content = None,
              dependency = Seq.empty,
              functionActivationId = functionActivationId,
              appActivationId = appActivationId,
              transactionId = invoke.transid,
              corunning = invoke.siblings
            )
          }
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

              val actions = rel.dependents.map(dep => WhiskActionMetaData.get(entityStore, dep.getDocId()))
              // Each action in the dependents map is expected to have the same parallelism argument. We just take
              // the value from the first dependent and apply it to all of the objects.
              actions.head.flatMap(f => Future.successful(f.parallelism.getOrElse(1))) map { parallelism =>
                for (i <- 0 until parallelism) {
                  implicit val id: Identity = identity
                  // it's implicitly expected the ordering of `rel.dependents` and `actions` is kept the same (since
                  // actions is derived from rel.dependents)
                  doActivationGroup(msg, rel, actions, ParallelismInfo(i, parallelism))
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
   * Executes a set of actions from the dependency invocation message.
   *
   * @param msgthe message triggering the invocations
   * @param rel relationships of the action which triggered the invocations
   * @param actions the dependent actions (derived from rel.dependents)
   * @param parallelismIndex 0-based index indicating the group in which the actions are launched
   * @param transid the parent
   * @param identity identity of user who invoked the DAG.
   */
  private def doActivationGroup(msg: DependencyInvocationMessage, rel: WhiskActionRelationship,
                                actions: Seq[Future[WhiskActionMetaData]], parallelismIndex: ParallelismInfo)(implicit transid: TransactionId, identity: Identity): Unit = {
    // schedule the next set of dependencies
    // generate the new activationIds
    // Use RunningActivation type so invokers can update the DB with network address
    val siblingActivations: Seq[RunningActivation] = rel.dependents.map(x => RunningActivation(x.toFQEN().toString, ActivationId.generate(), parallelismIndex))
    val siblingSet: Set[RunningActivation] = siblingActivations.toSet
    // for all of the next objects to be activated, get the action metadata and publish
    // to the load balancer

    actions zip siblingActivations map {
      case (childFuture, newActivation) =>
        childFuture flatMap { action =>
          action.toExecutableWhiskAction map { obj =>

            var sibs = siblingSet - newActivation

            obj.relationships map { objRel =>
              msg.corunning map { prevObjCorunning =>
                val corunNames = objRel.corunning.map(_.toString()).toSet
                // corunning objects which are part of the corunning set for this object *and* came from the
                // activation message of the object which is requesting our scheduling
                val corunsToInclude = prevObjCorunning.filter(p => corunNames.contains(p.objName))
                // now we need to add them to the sibling set. Currently the sibling set only contains the
                // siblings which are directly scheduled by the parent object, but may not include siblings
                // which were started from or before the parent object.
                sibs = sibs ++ corunsToInclude
              }
            }
            val msgsToWait = obj.relationships.map(x => x.parents.size)

            val message = ActivationMessage(
              transid,
              FullyQualifiedEntityName(action.namespace, action.name, Some(action.version), action.binding),
              action.rev,
              identity,
              newActivation.objActivation, // activation id created here
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
              waitForContent = msgsToWait, // the number of input messages required to run this object
              parallelismIdx = parallelismIndex,
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

object DepInvoker {
  val ACTION_TIMEOUT: FiniteDuration = Duration(30, SECONDS)
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

case class SchedulingDecision(originalActivationId: ActivationId, nextActivationId: ActivationId, invoker: InvokerInstanceId, override val transid: TransactionId, parallelismInfo: ParallelismInfo) extends Message {
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
    "maxParallelism"
  )

  def parse(msg: String): Try[SchedulingDecision] = Try(serdes.read(msg.parseJson))
}

case class ExpirationTick(id: ActivationId)

case class ResultWait()(implicit val logging: Logging, implicit val producer: MessageProducer) {
  var enterTime: Instant = Instant.now()
  var sentMsgs: Int = 0
  var activation: Option[WhiskActivation] = None
  var decisions: mutable.Queue[SchedulingDecision] = mutable.Queue()
  var maxParallelism: Int = 1

  def sentAllMessages(): Boolean = {
    logging.debug(this, s"checking if sentAll: $sentMsgs >= $maxParallelism")
    sentMsgs >= maxParallelism
  }

  def addDecision(dec: SchedulingDecision): Unit = {
    decisions += dec
    maxParallelism = Math.max(dec.parallelismInfo.max, maxParallelism)
    drainQueue()
  }

  def addActivation(act: WhiskActivation): Unit = {
    if (activation.isEmpty) {
      activation = Some(act)
    } else {
      logging.warn(this, s"got two activation messages for same id: ${act.activationId}")
    }
    drainQueue()
  }

  /**
   * Drains queue if possible of all scheduling decisions
   * @return the number of decisions sent
   */
  def drainQueue(): Unit = {
    activation foreach { act =>
      decisions.foreach(d => sendResponse(act, d, enterTime))
    }
  }

  def sendResponse(act: WhiskActivation, sched: SchedulingDecision, enter: Instant): Unit = {
    implicit val transid: TransactionId = sched.transid;
    producer.send(sched.invoker.getDepInputTopic, DepResultMessage(act.activationId, sched.nextActivationId, act))
    sentMsgs += 1
    val duration = java.time.Duration.between(enter, Instant.now).toMillis
    sched.transid.mark(this, LoggingMarkers.INVOKER_ACTIVATION_LEAVE_RESULT_WAITER)
    if (act.response.isSuccess) {
      logging.debug(this, s"[marker:invoker_resultwaiter_leave:${duration}]")
    } else {
      logging.warn(this, s"invoker activation failed for ${act.activationId}, removing from waiter")
    }
  }
}

object ResultWait {

}

class ResultWaiter(producer: MessageProducer)(implicit val logging: Logging) extends Actor with Timers {
  val activations: mutable.Map[ActivationId, ResultWait] = mutable.Map.empty

  override def receive: Receive = {
    case act: WhiskActivation =>
      activations.get(act.activationId) match {
        case Some(resultWait) =>
          addEntry(act.activationId, resultWait, Left(act))
        case None =>
          addNewEntry(act.activationId, Left(act))
      }
    case sched: SchedulingDecision =>
      activations.get(sched.originalActivationId) match {
        case Some(resultWait) =>
          addEntry(sched.originalActivationId, resultWait, Right(sched))
        case None =>
          // store in map
          addNewEntry(sched.originalActivationId, Right(sched))
      }
    case exp: ExpirationTick =>
      activations.remove(exp.id) match {
        case Some(value) =>
          logging.warn(this, s"timed out for activation id ${exp.id}")
        case None =>
      }
  }

  def addNewEntry(id: ActivationId, value: Either[WhiskActivation, SchedulingDecision]): Unit = {
    timers.startSingleTimer(id, ExpirationTick(id), DepInvoker.ACTION_TIMEOUT)
    val wait = ResultWait()(logging, producer)
    value match {
      case Left(value) => wait.addActivation(value)
      case Right(value) => wait.addDecision(value)
    }
    activations.put(id, wait)
  }

  def addEntry(id: ActivationId, wait: ResultWait, value: Either[WhiskActivation, SchedulingDecision]): Unit = {
    value match {
      case Left(value) => wait.addActivation(value)
      case Right(value) => wait.addDecision(value)
    }
    if (wait.sentAllMessages()) {
      activations.remove(id)
      timers.cancel(id)
    }
  }
}

/**
 * Stores two important pieces of information:
 *  - the original activation message for this object
 *  - the dependency results which are required to activate this
 * @param logging
 */
class ActivationWait()(implicit val logging: Logging) {
  val enter: Instant = Instant.now
  var activationMsg: Option[ActivationMessage] = None
  var results: mutable.ListBuffer[DepResultMessage] = new mutable.ListBuffer[DepResultMessage]()

  def hasAllDeps: Boolean = {
    activationMsg match {
      case Some(msg) =>
        msg.waitForContent match {
          case Some(value) =>
            results.size >= value
          case _ => false
        }
      case _ => false
    }
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
  val activations: mutable.Map[ActivationId, ActivationWait] = mutable.Map.empty

  override def receive: Receive = {
    case act: ActivationMessage =>
      activations.get(act.activationId) match {
        case Some(value) =>
          addDepResult(value, activationMsg = Some(act))
        case None =>
          // store in map
          newContainer(act.activationId, activationMsg = Some(act))
      }
    case res: DepResultMessage =>
      activations.get(res.nextActivationId) match {
        case Some(value) =>
          addDepResult(value, depResultMessage = Some(res))
        case None =>
          // store in map
          newContainer(res.nextActivationId, Some(res))
      }
    case tick: ExpirationTick =>
      logging.warn(this, s"expired activation wait ${tick.id}")
      activations.remove(tick.id)
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
    activations.put(activationId, container)
    timers.startSingleTimer(activationId, ExpirationTick(activationId), DepInvoker.ACTION_TIMEOUT)
  }
}
