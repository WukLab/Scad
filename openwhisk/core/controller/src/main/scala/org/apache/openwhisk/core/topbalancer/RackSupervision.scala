package org.apache.openwhisk.core.topbalancer

//import akka.actor.Actor
//import akka.actor.ActorRef
//import akka.actor.ActorRefFactory
//import akka.actor.FSM.CurrentState
//import akka.actor.FSM.SubscribeTransitionCallBack
//import akka.actor.FSM.Transition
//import akka.actor.Props
//import akka.pattern.pipe
//import akka.util.Timeout
//import org.apache.kafka.clients.producer.RecordMetadata
//import org.apache.openwhisk.common.AkkaLogging
//import org.apache.openwhisk.common.Logging
//import org.apache.openwhisk.common.TransactionId
//import org.apache.openwhisk.core.connector.ActivationMessage
//import org.apache.openwhisk.core.connector.MessageConsumer
//import org.apache.openwhisk.core.connector.MessageFeed
//import org.apache.openwhisk.core.connector.PingMessage
//import org.apache.openwhisk.core.connector.PingRackMessage
//import org.apache.openwhisk.core.database.NoDocumentException
//import org.apache.openwhisk.core.entity.ActionLimits
//import org.apache.openwhisk.core.entity.BasicAuthenticationAuthKey
//import org.apache.openwhisk.core.entity.CodeExecAsString
//import org.apache.openwhisk.core.entity.EntityName
//import org.apache.openwhisk.core.entity.ExecManifest
//import org.apache.openwhisk.core.entity.Identity
//import org.apache.openwhisk.core.entity.InvokerInstanceId
//import org.apache.openwhisk.core.entity.MemoryLimit
//import org.apache.openwhisk.core.entity.Namespace
//import org.apache.openwhisk.core.entity.RackSchedInstanceId
//import org.apache.openwhisk.core.entity.Secret
//import org.apache.openwhisk.core.entity.Subject
//import org.apache.openwhisk.core.entity.TopSchedInstanceId
//import org.apache.openwhisk.core.entity.UUID
//import org.apache.openwhisk.core.entity.WhiskAction
//import org.apache.openwhisk.core.entity.types.EntityStore
//import org.apache.openwhisk.core.loadBalancer.GetStatus
//import org.apache.openwhisk.core.loadBalancer.InvocationFinishedMessage
//import org.apache.openwhisk.core.loadBalancer.InvokerPool
//import org.apache.openwhisk.core.loadBalancer.RackActivationRequest
//
//import java.nio.charset.StandardCharsets
//import scala.collection.immutable
//import scala.concurrent.Await
//import scala.concurrent.ExecutionContext
//import scala.concurrent.duration._
//import scala.concurrent.Future
//import scala.concurrent.duration.FiniteDuration
//import scala.util.Failure
//import scala.util.Success

//class RackPool(childFactory: (ActorRefFactory, RackSchedInstanceId) => ActorRef,
//               sendActivationToRack: (ActivationMessage, RackSchedInstanceId) => Future[RecordMetadata],
//               pingConsumer: MessageConsumer,
//               monitor: Option[ActorRef])
//  extends Actor {
//
//  implicit val transid: TransactionId = TransactionId.invokerHealth
//  implicit val logging: Logging = new AkkaLogging(context.system.log)
//  implicit val timeout: Timeout = Timeout(5.seconds)
//  implicit val ec: ExecutionContext = context.dispatcher
//
//  // State of the actor. Mutable vars with immutable collections prevents closures or messages
//  // from leaking the state for external mutation
//  var instanceToRef = immutable.Map.empty[Int, ActorRef]
//  var refToInstance = immutable.Map.empty[ActorRef, RackSchedInstanceId]
//  var status = IndexedSeq[RackHealth]()
//
//  def receive: Receive = {
//    case p: PingRackMessage =>
//      val rack = instanceToRef.getOrElse(p.instance.toInt, registerRack(p.instance))
//      instanceToRef = instanceToRef.updated(p.instance.toInt, rack)
//
//      // For the case when the invoker was restarted and got a new displayed name
//      val oldHealth = status(p.instance.toInt)
//      if (oldHealth.id != p.instance) {
//        status = status.updated(p.instance.toInt, new RackHealth(p.instance, oldHealth.status))
//        refToInstance = refToInstance.updated(rack, p.instance)
//      }
//
//      rack.forward(p)
//
//    case GetStatus => sender() ! status
//
//    case msg: InvocationFinishedMessage =>
//      // Forward message to invoker, if InvokerActor exists
//      instanceToRef.get(msg.invokerInstance.toInt).foreach(_.forward(msg))
//
//    case CurrentState(invoker, currentState: RackState) =>
//      refToInstance.get(invoker).foreach { instance =>
//        status = status.updated(instance.toInt, new RackHealth(instance, currentState))
//      }
//      logStatus()
//
//    case Transition(invoker, oldState: RackState, newState: RackState) =>
//      refToInstance.get(invoker).foreach { instance =>
//        status = status.updated(instance.toInt, new RackHealth(instance, newState))
//      }
//      logStatus()
//
//    // this is only used for the internal test action which enabled an invoker to become healthy again
//    case msg: RackActivationRequest => sendActivationToRack(msg.msg, msg.invoker).pipeTo(sender)
//  }
//
//  def logStatus(): Unit = {
//    monitor.foreach(_ ! CurrentRackPoolState(status))
//    val pretty = status.map(i => s"${i.id.toInt} -> ${i.status}")
//    logging.info(this, s"invoker status changed to ${pretty.mkString(", ")}")
//  }
//
//  /** Receive Ping messages from invokers. */
//  val pingPollDuration: FiniteDuration = 1.second
//  val invokerPingFeed: ActorRef = context.system.actorOf(Props {
//    new MessageFeed(
//      "ping",
//      logging,
//      pingConsumer,
//      pingConsumer.maxPeek,
//      pingPollDuration,
//      processInvokerPing,
//      logHandoff = false)
//  })
//
//  def processInvokerPing(bytes: Array[Byte]): Future[Unit] = Future {
//    val raw = new String(bytes, StandardCharsets.UTF_8)
//    PingMessage.parse(raw) match {
//      case Success(p: PingMessage) =>
//        self ! p
//        invokerPingFeed ! MessageFeed.Processed
//
//      case Failure(t) =>
//        invokerPingFeed ! MessageFeed.Processed
//        logging.error(this, s"failed processing message: $raw with $t")
//    }
//  }
//
//  /** Pads a list to a given length using the given function to compute entries */
//  def padToIndexed[A](list: IndexedSeq[A], n: Int, f: (Int) => A): IndexedSeq[A] = list ++ (list.size until n).map(f)
//
//  // Register a new invoker
//  def registerRack(instanceId: RackSchedInstanceId): ActorRef = {
//    logging.info(this, s"registered a new invoker: invoker${instanceId.toInt}")(TransactionId.invokerHealth)
//
//    // Grow the underlying status sequence to the size needed to contain the incoming ping. Dummy values are created
//    // to represent invokers, where ping messages haven't arrived yet
//    status = padToIndexed(
//      status,
//      instanceId.toInt + 1,
//      i => new RackHealth(new RackSchedInstanceId(i, resources = instanceId.resources), RackState.Offline))
//    status = status.updated(instanceId.toInt, new RackHealth(instanceId, RackState.Offline))
//
//    val ref = childFactory(context, instanceId)
//    ref ! SubscribeTransitionCallBack(self) // register for state change events
//    refToInstance = refToInstance.updated(ref, instanceId)
//
//    ref
//  }
//}
//
//object RackPool {
//  private def createTestActionForRackHealth(db: EntityStore, action: WhiskAction): Future[Unit] = {
//    implicit val tid: TransactionId = TransactionId.loadbalancer
//    implicit val ec: ExecutionContext = db.executionContext
//    implicit val logging: Logging = db.logging
//
//    WhiskAction
//      .get(db, action.docid)
//      .flatMap { oldAction =>
//        WhiskAction.put(db, action.revision(oldAction.rev), Some(oldAction))(tid, notifier = None)
//      }
//      .recover {
//        case _: NoDocumentException => WhiskAction.put(db, action, old = None)(tid, notifier = None)
//      }
//      .map(_ => {})
//      .andThen {
//        case Success(_) => logging.info(this, "test action for rack health now exists")
//        case Failure(e) => logging.error(this, s"error creating test action for rack health: $e")
//      }
//  }
//
//  /** A stub identity for invoking the test action. This does not need to be a valid identity. */
//  val healthActionIdentity: Identity = {
//    val whiskSystem = "whisk.system"
//    val uuid = UUID()
//    Identity(Subject(whiskSystem), Namespace(EntityName(whiskSystem), uuid), BasicAuthenticationAuthKey(uuid, Secret()))
//  }
//
//  /** An action to use for monitoring rack health. */
//  def healthAction(i: TopSchedInstanceId): Option[WhiskAction] =
//    ExecManifest.runtimesManifest.resolveDefaultRuntime("nodejs:default").map { manifest =>
//      new WhiskAction(
//        namespace = healthActionIdentity.namespace.name.toPath,
//        name = EntityName(s"rackHealthTestAction${i.asString}"),
//        exec = CodeExecAsString(manifest, """function main(params) { return params; }""", None),
//        limits = ActionLimits(memory = MemoryLimit(MemoryLimit.MIN_MEMORY)))
//    }
//
//  /**
//   * Prepares everything for the health protocol to work (i.e. creates a testaction)
//   *
//   * @param topSchedInstance instance of the controller we run in
//   * @param entityStore        store to write the action to
//   * @return throws an exception on failure to prepare
//   */
//  def prepare(topSchedInstance: TopSchedInstanceId, entityStore: EntityStore): Unit = {
//    RackPool
//      .healthAction(topSchedInstance)
//      .map {
//        // Await the creation of the test action; on failure, this will abort the constructor which should
//        // in turn abort the startup of the controller.
//        a =>
//          Await.result(createTestActionForRackHealth(entityStore, a), 1.minute)
//      }
//      .orElse {
//        throw new IllegalStateException(
//          "cannot create test action for invoker health because runtime manifest is not valid")
//      }
//  }
//
//  def props(f: (ActorRefFactory, InvokerInstanceId) => ActorRef,
//            p: (ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
//            pc: MessageConsumer,
//            m: Option[ActorRef] = None): Props = {
//    Props(new InvokerPool(f, p, pc, m))
//  }
//}

