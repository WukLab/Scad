package org.apache.openwhisk.core.topbalancer

import akka.actor.{Actor, ActorRef, ActorRefFactory, FSM, Props}
import akka.actor.FSM.CurrentState
import akka.actor.FSM.SubscribeTransitionCallBack
import akka.actor.FSM.Transition
import akka.pattern.pipe
import akka.util.Timeout

import scala.concurrent.duration._
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common.AkkaLogging
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.LoggingMarkers
import org.apache.openwhisk.common.RingBuffer
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.connector.MessageConsumer
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.connector.MessageProducer
import org.apache.openwhisk.core.connector.MessagingProvider
import org.apache.openwhisk.core.connector.PingRackMessage
import org.apache.openwhisk.core.database.NoDocumentException
import org.apache.openwhisk.core.entity.ActionLimits
import org.apache.openwhisk.core.entity.ActivationId.ActivationIdGenerator
import org.apache.openwhisk.core.entity.BasicAuthenticationAuthKey
import org.apache.openwhisk.core.entity.CodeExecAsString
import org.apache.openwhisk.core.entity.ControllerInstanceId
import org.apache.openwhisk.core.entity.DocRevision
import org.apache.openwhisk.core.entity.EntityName
import org.apache.openwhisk.core.entity.ExecManifest
import org.apache.openwhisk.core.entity.Identity
import org.apache.openwhisk.core.entity.Namespace
import org.apache.openwhisk.core.entity.RackSchedInstanceId
import org.apache.openwhisk.core.entity.ResourceLimit
import org.apache.openwhisk.core.entity.Secret
import org.apache.openwhisk.core.entity.Subject
import org.apache.openwhisk.core.entity.TopSchedInstanceId
import org.apache.openwhisk.core.entity.UUID
import org.apache.openwhisk.core.entity.WhiskAction
import org.apache.openwhisk.core.entity.types.EntityStore
import org.apache.openwhisk.core.loadBalancer.GetStatus
import org.apache.openwhisk.core.loadBalancer.InvocationFinishedMessage
import org.apache.openwhisk.core.loadBalancer.InvocationFinishedResult
import org.apache.openwhisk.core.loadBalancer.InvokerInfo
import org.apache.openwhisk.core.loadBalancer.RackActivationRequest
import org.apache.openwhisk.core.loadBalancer.Tick
import org.apache.openwhisk.core.topbalancer.RackState.Healthy
import org.apache.openwhisk.core.topbalancer.RackState.Offline
import org.apache.openwhisk.core.topbalancer.RackState.Unhealthy
import org.apache.openwhisk.core.topbalancer.RackState.Unresponsive

import java.nio.charset.StandardCharsets
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success

trait RackPoolFactory {
  def createRackPool(
                         actorRefFactory: ActorRefFactory,
                         messagingProvider: MessagingProvider,
                         messagingProducer: MessageProducer,
                         sendActivationToRack: (MessageProducer, ActivationMessage, RackSchedInstanceId) => Future[RecordMetadata],
                         monitor: Option[ActorRef]): ActorRef
}

/**
 * Actor representing a pool of available racks
 *
 * The RackPool manages racks through subactors. when a new rack
 * is registered lazily by sending it a Ping event with the name of the
 * Rack. Ping events are furthermore forwarded to the respective
 * Racks for their respective State handling.
 *
 * Note: A Rack that never sends an initial Ping will not be considered
 * by the RackPool and thus might not be caught by monitoring.
 */
class RackPool(childFactory: (ActorRefFactory, RackSchedInstanceId) => ActorRef,
               sendActivationToRack: (ActivationMessage, RackSchedInstanceId) => Future[RecordMetadata],
               pingConsumer: MessageConsumer,
               monitor: Option[ActorRef])
  extends Actor {

  import RackState._

  implicit val transid: TransactionId = TransactionId.rackHealth
  implicit val logging: Logging = new AkkaLogging(context.system.log)
  implicit val timeout: Timeout = Timeout(60.seconds)
  implicit val ec: ExecutionContext = context.dispatcher

  // State of the actor. Mutable vars with immutable collections prevents closures or messages
  // from leaking the state for external mutation
  var instanceToRef = immutable.Map.empty[Int, ActorRef]
  var refToInstance = immutable.Map.empty[ActorRef, RackSchedInstanceId]
  var status = IndexedSeq[RackHealth]()

  def receive: Receive = {
    case p: PingRackMessage =>
      val rack = instanceToRef.getOrElse(p.instance.toInt, registerRack(p.instance))
      instanceToRef = instanceToRef.updated(p.instance.toInt, rack)

      // For the case when the rack scheduler was restarted and got a new displayed name
      val oldHealth = status(p.instance.toInt)
      if (oldHealth.id != p.instance) {
        status = status.updated(p.instance.toInt, new RackHealth(p.instance, oldHealth.status))
        refToInstance = refToInstance.updated(rack, p.instance)
      }

      rack.forward(p)

    case GetStatus => sender() ! status

    case msg: InvocationFinishedMessage =>
      // Forward message to rack, if RackActor exists
      instanceToRef.get(msg.invokerInstance.toInt).foreach(_.forward(msg))

    case CurrentState(rack, currentState: RackState) =>
      refToInstance.get(rack).foreach { instance =>
        status = status.updated(instance.toInt, new RackHealth(instance, currentState))
      }
      logStatus()

    case Transition(rack, oldState: RackState, newState: RackState) =>
      refToInstance.get(rack).foreach { instance =>
        status = status.updated(instance.toInt, new RackHealth(instance, newState))
      }
      logStatus()

    // this is only used for the internal test action which enables a rack to become healthy again
    case msg: RackActivationRequest => sendActivationToRack(msg.msg, msg.rack).pipeTo(sender)
  }

  def logStatus(): Unit = {
    monitor.foreach(_ ! CurrentRackPoolState(status))
    val pretty = status.map(i => s"${i.id.toInt} -> ${i.status}")
    logging.info(this, s"rack status changed to ${pretty.mkString(", ")}")
  }

  /** Receive Ping messages from racks. */
  val pingPollDuration: FiniteDuration = 1.second
  val rackPingFeed: ActorRef = context.system.actorOf(Props {
    new MessageFeed(
      "rackPing",
      logging,
      pingConsumer,
      pingConsumer.maxPeek,
      pingPollDuration,
      processRackPing,
      logHandoff = false)
  })

  def processRackPing(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    PingRackMessage.parse(raw) match {
      case Success(p: PingRackMessage) =>
        self ! p
        rackPingFeed ! MessageFeed.Processed

      case Failure(t) =>
        rackPingFeed ! MessageFeed.Processed
        logging.error(this, s"failed processing message: $raw with $t")
    }
  }

  /** Pads a list to a given length using the given function to compute entries */
  def padToIndexed[A](list: IndexedSeq[A], n: Int, f: Int => A): IndexedSeq[A] = list ++ (list.size until n).map(f)

  // Register a new rack
  def registerRack(instanceId: RackSchedInstanceId): ActorRef = {
    logging.info(this, s"registered a new rack: rack${instanceId.toInt}")(TransactionId.rackHealth)

    // Grow the underlying status sequence to the size needed to contain the incoming ping. Dummy values are created
    // to represent racks, where ping messages haven't arrived yet
    status = padToIndexed(
      status,
      instanceId.toInt + 1,
      i => new RackHealth(new RackSchedInstanceId(i, resources = instanceId.resources), Offline))
    status = status.updated(instanceId.toInt, new RackHealth(instanceId, Offline))

    val ref = childFactory(context, instanceId)
    ref ! SubscribeTransitionCallBack(self) // register for state change events
    refToInstance = refToInstance.updated(ref, instanceId)

    ref
  }

}

object RackPool {
  private def createTestActionForrackHealth(db: EntityStore, action: WhiskAction): Future[Unit] = {
    implicit val tid: TransactionId = TransactionId.loadbalancer
    implicit val ec: ExecutionContext = db.executionContext
    implicit val logging: Logging = db.logging

    WhiskAction
      .get(db, action.docid)
      .flatMap { oldAction =>
        WhiskAction.put(db, action.revision(oldAction.rev), Some(oldAction))(tid, notifier = None)
      }
      .recover {
        case _: NoDocumentException => WhiskAction.put(db, action, old = None)(tid, notifier = None)
      }
      .map(_ => {})
      .andThen {
        case Success(_) => logging.info(this, "test action for rack health now exists")
        case Failure(e) => logging.error(this, s"error creating test action for rack health: $e")
      }
  }

  /**
   * Prepares everything for the health protocol to work (i.e. creates a testaction)
   *
   * @param controllerInstance instance of the controller we run in
   * @param entityStore store to write the action to
   * @return throws an exception on failure to prepare
   */
  def prepare(controllerInstance: ControllerInstanceId, entityStore: EntityStore): Unit = {
    RackPool
      .healthAction(controllerInstance)
      .map {
        // Await the creation of the test action; on failure, this will abort the constructor which should
        // in turn abort the startup of the controller.
        a =>
          Await.result(createTestActionForrackHealth(entityStore, a), 1.minute)
      }
      .orElse {
        throw new IllegalStateException(
          "cannot create test action for rack health because runtime manifest is not valid")
      }
  }

  def props(f: (ActorRefFactory, RackSchedInstanceId) => ActorRef,
            p: (ActivationMessage, RackSchedInstanceId) => Future[RecordMetadata],
            pc: MessageConsumer,
            m: Option[ActorRef] = None): Props = {
    Props(new RackPool(f, p, pc, m))
  }

  /** A stub identity for invoking the test action. This does not need to be a valid identity. */
  val healthActionIdentity: Identity = {
    val whiskSystem = "whisk.system"
    val uuid = UUID()
    Identity(Subject(whiskSystem), Namespace(EntityName(whiskSystem), uuid), BasicAuthenticationAuthKey(uuid, Secret()))
  }

  /** An action to use for monitoring rack health. */
  def healthAction(i: ControllerInstanceId): Option[WhiskAction] =
    ExecManifest.runtimesManifest.resolveDefaultRuntime("nodejs:default").map { manifest =>
      new WhiskAction(
        namespace = healthActionIdentity.namespace.name.toPath,
        name = EntityName(s"rackHealthTestAction${i.asString}"),
        exec = CodeExecAsString(manifest, """function main(params) { return params; }""", None),
        limits = ActionLimits(resources = ResourceLimit(ResourceLimit.MIN_RESOURCES)))
    }
}

/**
 * Actor representing an rack
 *
 * This finite state-machine represents an rack in its possible
 * states "Healthy" and "Offline".
 */
class RackActor(rackInstance: RackSchedInstanceId, topsched: TopSchedInstanceId)
  extends FSM[RackState, InvokerInfo] {

  implicit val transid: TransactionId = TransactionId.rackHealth
  implicit val logging: Logging = new AkkaLogging(context.system.log)
  val name = s"rack${rackInstance.toInt}"

  val healthyTimeout: FiniteDuration = 10.seconds

  // This is done at this point to not intermingle with the state-machine
  // especially their timeouts.
  def customReceive: Receive = {
    case _: RecordMetadata => // The response of putting testactions to the MessageProducer. We don't have to do anything with them.
  }
  override def receive: Receive = customReceive.orElse(super.receive)

  /** Always start UnHealthy. Then the rack receives some test activations and becomes Healthy. */
  startWith(RackState.Unhealthy, InvokerInfo(new RingBuffer[InvocationFinishedResult](RackActor.bufferSize)))

  /** An Offline rack represents an existing but broken rack. This means, that it does not send pings anymore. */
  when(RackState.Offline) {
    case Event(_: PingRackMessage, _) => goto(RackState.Unhealthy)
  }

  // To be used for all states that should send test actions to reverify the rack
  val healthPingingState: StateFunction = {
    case Event(_: PingRackMessage, _) => goto(RackState.Healthy)
    case Event(StateTimeout, _)   => goto(RackState.Offline)
    case Event(Tick, _) =>
      stay
  }

  /** An Unhealthy rack represents an rack that was not able to handle actions successfully. */
  when(RackState.Unhealthy, stateTimeout = healthyTimeout)(healthPingingState)

  /** An Unresponsive rack represents an rack that is not responding with active acks in a timely manner */
  when(RackState.Unresponsive, stateTimeout = healthyTimeout)(healthPingingState)

  /**
   * A Healthy rack is characterized by continuously getting pings. It will go offline if that state is not confirmed
   * for 20 seconds.
   */
  when(RackState.Healthy, stateTimeout = healthyTimeout) {
    case Event(_: PingRackMessage, _) => stay
    case Event(StateTimeout, _)   => goto(RackState.Offline)
  }

  /** Handle the completion of an Activation in every state. */
  whenUnhandled {
    case Event(cm: InvocationFinishedMessage, info) => handleCompletionMessage(cm.result, info.buffer)
  }

  /** Logging on Transition change */
  onTransition {
    case _ -> newState if !newState.isUsable =>
      transid.mark(
        this,
        LoggingMarkers.LOADBALANCER_RACK_STATUS_CHANGE(newState.asString),
        s"$name is ${newState.asString}",
        akka.event.Logging.WarningLevel)
    case _ -> newState if newState.isUsable => logging.info(this, s"$name is ${newState.asString}")
  }

  // To be used for all states that should send test actions to reverify the rack
  def healthPingingTransitionHandler(state: RackState): TransitionHandler = {
    case _ -> `state` =>
      setTimer(RackActor.timerName, Tick, 1.minute, repeat = true)
    case `state` -> _ => cancelTimer(RackActor.timerName)
  }

  onTransition(healthPingingTransitionHandler(RackState.Unhealthy))
  onTransition(healthPingingTransitionHandler(RackState.Unresponsive))

  initialize()

  /**
   * Handling for active acks. This method saves the result (successful or unsuccessful)
   * into an RingBuffer and checks, if the rackActor has to be changed to UnHealthy.
   *
   * @param result: result of Activation
   * @param buffer to be used
   */
  private def handleCompletionMessage(result: InvocationFinishedResult,
                                      buffer: RingBuffer[InvocationFinishedResult]) = {
    buffer.add(result)

    // If the action is successful it seems like the rack is Healthy again. So we execute immediately
    // a new test action to remove the errors out of the RingBuffer as fast as possible.
    // The actions that arrive while the rack is unhealthy are most likely health actions.
    // It is possible they are normal user actions as well. This can happen if such actions were in the
    // rack queue or in progress while the rack's status flipped to Unhealthy.
    if (result == InvocationFinishedResult.Success && stateName == Unhealthy) {
      invokeTestAction()
    }

    // Stay in online if the activations was successful.
    // Stay in offline, if an activeAck reaches the controller.
    if ((stateName == Healthy && result == InvocationFinishedResult.Success) || stateName == Offline) {
      stay
    } else {
      val entries = buffer.toList
      // Goto Unhealthy or Unresponsive respectively if there are more errors than accepted in buffer, else goto Healthy
      if (entries.count(_ == InvocationFinishedResult.SystemError) > RackActor.bufferErrorTolerance) {
        gotoIfNotThere(Unhealthy)
      } else if (entries.count(_ == InvocationFinishedResult.Timeout) > RackActor.bufferErrorTolerance) {
        gotoIfNotThere(Unresponsive)
      } else {
        gotoIfNotThere(Healthy)
      }
    }
  }

  /**
   * Creates an activation request with the given action and sends it to the rackPool.
   * The rackPool redirects it to the rack which is represented by this rackActor.
   */
  private def invokeTestAction() = {
    RackPool.healthAction(topsched).map { action =>
      val activationMessage = ActivationMessage(
        // Use the sid of the rackSupervisor as tid
        transid = transid,
        action = action.fullyQualifiedName(true),
        // Use empty DocRevision to force the rack to pull the action from db all the time
        revision = DocRevision.empty,
        user = RackPool.healthActionIdentity,
        // Create a new Activation ID for this activation
        activationId = new ActivationIdGenerator {}.make(),
        rootControllerIndex = topsched,
        blocking = false,
        content = None,
        initArgs = Set.empty,
        lockedArgs = Map.empty)

      context.parent ! RackActivationRequest(activationMessage, rackInstance)
    }
  }

  /**
   * Only change the state if the currentState is not the newState.
   *
   * @param newState of the rackActor
   */
  private def gotoIfNotThere(newState: RackState) = {
    if (stateName == newState) stay() else goto(newState)
  }
}

object RackActor {
  def props(rackInstance: RackSchedInstanceId, topsched: TopSchedInstanceId) =
    Props(new RackActor(rackInstance, topsched))

  val bufferSize = 10
  val bufferErrorTolerance = 3

  val timerName = "testActionTimer"
}

