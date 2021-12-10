/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.invoker

import java.nio.charset.StandardCharsets
import java.time.Instant
import akka.Done
import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, CoordinatedShutdown, Props}
import akka.event.Logging.InfoLevel
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common._
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.core.ack.{MessagingActiveAck, UserEventSender}
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.logging.LogStoreProvider
import org.apache.openwhisk.core.database.{UserContext, _}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.http.Messages
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._
import pureconfig.generic.auto._
import spray.json._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object InvokerReactive extends InvokerProvider {

  override def instance(
    config: WhiskConfig,
    instance: InvokerInstanceId,
    producer: MessageProducer,
    poolConfig: ContainerPoolConfig,
    limitsConfig: ConcurrencyLimitConfig)(implicit actorSystem: ActorSystem, logging: Logging): InvokerCore =
    new InvokerReactive(config, instance, producer, poolConfig, limitsConfig)

}

class InvokerReactive(
  config: WhiskConfig,
  instance: InvokerInstanceId,
  producer: MessageProducer,
  poolConfig: ContainerPoolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool),
  limitsConfig: ConcurrencyLimitConfig = loadConfigOrThrow[ConcurrencyLimitConfig](ConfigKeys.concurrencyLimit))(
  implicit actorSystem: ActorSystem,
  logging: Logging)
    extends InvokerCore {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val cfg: WhiskConfig = config

  private val rackId: RackSchedInstanceId = new RackSchedInstanceId(loadConfigOrThrow[Int](ConfigKeys.invokerRack), RuntimeResources.none())
  private val proxyPort: Int = loadConfigOrThrow[Int](ConfigKeys.invokerProxyNetworkPort)
  private val proxyRoutes: Map[String, (String, Int)] = config.proxyNetworkRouting.split(",").filter(_.contains(":")).map({value =>
      val split: Array[String] = value.split(":")
      split(0) -> (split(0), split(1).toInt)
  }).toMap

  private val rackHealthTopic = RackSchedInstanceId.rackSchedHealthTopic(rackId.toInt)
  logging.debug(this, s"rack health topic is ${rackHealthTopic}")

  private val logsProvider = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  logging.info(this, s"LogStoreProvider: ${logsProvider.getClass}")

  /**
   * Factory used by the ContainerProxy to physically create a new container.
   *
   * Create and initialize the container factory before kicking off any other
   * task or actor because further operation does not make sense if something
   * goes wrong here. Initialization will throw an exception upon failure.
   */
  private val containerFactory =
    SpiLoader
      .get[ContainerFactoryProvider]
      .instance(
        actorSystem,
        logging,
        config,
        instance,
        Map(
          "--cap-drop" -> Set("NET_RAW", "NET_ADMIN"),
          "--ulimit" -> Set("nofile=1024:1024"),
          "--pids-limit" -> Set("1024")) ++ logsProvider.containerParameters)
  containerFactory.init()

  CoordinatedShutdown(actorSystem)
    .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "cleanup runtime containers") { () =>
      containerFactory.cleanup()
      Future.successful(Done)
    }

  /** Initialize needed databases */
  private implicit val entityStore = WhiskEntityStore.datastore()
  private val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, materializer, logging)

  private implicit val authStore = WhiskAuthStore.datastore()

  private val namespaceBlacklist = new NamespaceBlacklist(authStore)

  Scheduler.scheduleWaitAtMost(loadConfigOrThrow[NamespaceBlacklistConfig](ConfigKeys.blacklist).pollInterval) { () =>
    logging.debug(this, "running background job to update blacklist")
    namespaceBlacklist.refreshBlacklist()(ec, TransactionId.invoker).andThen {
      case Success(set) => logging.info(this, s"updated blacklist to ${set.size} entries")
      case Failure(t)   => logging.error(this, s"error on updating the blacklist: ${t.getMessage}")
    }
  }

  private val msgProvider = SpiLoader.get[MessagingProvider]

  /** Initialize message consumers */
  private val topic = instance.getMainTopic
  private val depInputTopic = instance.getDepInputTopic
  private val schedResultTopic = instance.getSchedResultTopic

  // The maximum number of containers is limited by the memory or storage
  private val maximumContainers: Int = {
    val configured = poolConfig.resources
    val min = ResourceLimit.MIN_RESOURCES
    // memory or storage minimums could be 0
    val minStorage = configured.storage.toBytes / Math.max(1, min.storage.toBytes)
    val minMem = configured.mem.toBytes / Math.max(1, min.mem.toBytes)
    Math.min(minMem, minStorage).toInt
  }

  // This waiter holds onto results until a scheduling decision arrives with the topic to send the result to
  private val resultWaiter: ActorRef = actorSystem.actorOf(Props {
    new ResultWaiter(msgProvider.getProducer(config))
  })

  // This actor processes new activation messages from the scheduler or from the activationWaiter
  private val activationProcessor = actorSystem.actorOf(Props {
    new Actor {
      override def receive: Receive = {
        case msg: ActivationMessage => processActivationMessage(msg)
      }
    }
  })

  private val msgProducer: MessageProducer = msgProvider.getProducer(config)

  // This waiter holds onto new activation messages which don't contain all of the necessary content (parameters/arguments)
  // until those empty argument arrive.
  // empty arguments arrive from the depInputConsumer
  private val activationWaiter: ActorRef = actorSystem.actorOf(Props {
    new ActivationWaiter(activationProcessor, msgProducer)
  })

  private val prewarmDeadlineCache: PrewarmDeadlineCache = PrewarmDeadlineCache()
  private val prewarmDeadlineCacheActor: ActorRef = actorSystem.actorOf(Props(PrewarmDeadlineCacheActor(prewarmDeadlineCache)))

  private val dependencyScheduler: ActorRef = actorSystem.actorOf(Props {
    // instance ID doesn't matter as current impl only supports one topscheduler. The topic is always the same.
    new DepInvoker(instance, rackId, msgProducer, prewarmDeadlineCache)
  })

  //number of peeked messages - increasing the concurrentPeekFactor improves concurrent usage, but adds risk for message loss in case of crash
  private val maxPeek =
    math.max(maximumContainers, (maximumContainers * limitsConfig.max * poolConfig.concurrentPeekFactor).toInt)

  private val consumer =
    msgProvider.getConsumer(config, topic, topic, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)

  private val depInputConsumer =
    msgProvider.getConsumer(config, depInputTopic, depInputTopic, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)

  private val schedResultConsumer =
    msgProvider.getConsumer(config, schedResultTopic, schedResultTopic, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)

  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed("activation", logging, consumer, maxPeek, 1.second, parseActivationMessage)
  })

  private val depInputFeed = actorSystem.actorOf(Props {
    new MessageFeed("dependencyInputs", logging, depInputConsumer, maxPeek, 1.second, parseDependencyInputMessage)
  })

  private val schedResultFeed = actorSystem.actorOf(Props {
    new MessageFeed("schedulingResults", logging, schedResultConsumer, maxPeek, 1.second, parseSchedResult)
  })

  private val ack: MessagingActiveAck = {
    val sender = if (UserEvents.enabled) Some(new UserEventSender(producer)) else None
    new MessagingActiveAck(producer, instance, sender)
  }

  private val collectLogs = new LogStoreCollector(logsProvider)

  /** Stores an activation in the database. */
  private val store = (tid: TransactionId, activation: WhiskActivation, isBlocking: Boolean, context: UserContext) => {
    implicit val transid: TransactionId = tid
    activationStore.storeAfterCheck(activation, isBlocking, None, context)(tid, notifier = None, logging)
  }

  val memType: String = loadConfigOrThrow[String](ConfigKeys.invokerMemoryPoolType)
  // TODO: change this to config
  val proxyNode = new ProxyNode(proxyPort, proxyRoutes, memType)
  val addressBook = poolConfig.useProxy match {
    case true  => Some(new ActorProxyAddressBook(proxyNode))
    case false => None
  }


  /** Creates a ContainerProxy Actor when being called. */
  private val childFactory = (f: ActorRefFactory) =>
    f.actorOf(
      ContainerProxy
        .props(containerFactory.createContainer, ack, store, collectLogs, instance, poolConfig,
          msgProducer = msgProducer,
          resultWaiter = Some(resultWaiter),
          addressBook = addressBook,
          prewarmDeadlineCache = prewarmDeadlineCacheActor
        ))

  val prewarmingConfigs: List[PrewarmingConfig] = {
    ExecManifest.runtimesManifest.stemcells.flatMap {
      case (mf, cells) =>
        cells.map { cell =>
          PrewarmingConfig(cell.initialCount, new CodeExecAsString(mf, "", None), cell.resources, cell.reactive)
        }
    }.toList
  }

  val pool =
    actorSystem.actorOf(ContainerPool.props(childFactory, poolConfig, activationFeed, prewarmingConfigs, addressBook))

  val sock: String = loadConfigOrThrow[String](ConfigKeys.invokerMemoryPoolSock)
  var memoryPool: Option[MemoryPoolEndPoint] = None
  if (poolConfig.useProxy) {
    logging.debug(this, s"[MPT] initing MP with file ${sock}")
    memoryPool = Some(new MemoryPoolEndPoint(sock, proxyNode, ack))
  }

   def handlePrewarmMessage(msg: ActivationMessage, partialConfig: PartialPrewarmConfig)(implicit transid: TransactionId): Unit = {
     // wait time before actually handling message. 10ms for kafka latency, 400 for cold start time, 5 as "epsilon", for
     // some additional wiggle room
     val time: Long = partialConfig.prevElemRuntimeMs - 10 - 400 - 5
     val waitTime = if (time > 0) time else 0
     logging.debug(this, s"delaying prewarming of container ${msg.action} by ${waitTime}ms")
     actorSystem.getScheduler.scheduleOnce(FiniteDuration(waitTime, TimeUnit.MILLISECONDS))(() => {
       val namespace = msg.action.path
       val name = msg.action.name
       val actionid = FullyQualifiedEntityName(namespace, name).toDocId
       val subject = msg.user.subject
       logging.debug(this, s"handling prewarm message: ${actionid.id} $subject ${msg.activationId}")
       WhiskAction.get(entityStore, actionid)
         .flatMap(action => {
           action.toExecutableWhiskAction match {
             case Some(executable) =>
               pool ! PrewarmContainer(executable, partialConfig)
               Future.successful(())
             case None =>
               logging.error(this, s"non-executable action attempted to prewarm at invoker ${action.fullyQualifiedName(false)}")
               Future.successful(())
           }
         })
         .recoverWith({
           case t =>
             logging.warn(this, s"Failed to handle prewarming request: $actionid; $t")
             Future.successful(())
         })
     })
   }

  def handleActivationMessage(msg: ActivationMessage)(implicit transid: TransactionId): Future[Unit] = {
    val namespace = msg.action.path
    val name = msg.action.name
    val actionid = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
    val subject = msg.user.subject
    msg.transid.mark(this, LoggingMarkers.INVOKER_ACTIVATION_HANDLE)

    // send message to dependency invoker to invoke next-in-line dependencies
    dependencyScheduler ! msg

    // caching is enabled since actions have revision id and an updated
    // action will not hit in the cache due to change in the revision id;
    // if the doc revision is missing, then bypass cache
    if (actionid.rev == DocRevision.empty) logging.warn(this, s"revision was not provided for ${actionid.id}")
    WhiskAction
      .get(entityStore, actionid.id, actionid.rev, fromCache = actionid.rev != DocRevision.empty)
      .flatMap(action => {
        action.toExecutableWhiskAction match {
          case Some(executable) =>
            // If is memory, dispatch the element to memory pool; else, launch container
            val run = Run(executable, msg)
            val isMemory = (action.porusParams.runtimeType.getOrElse(ElementType.Compute) == ElementType.Memory) || msg.swapFrom.isDefined
            if (poolConfig.useProxy && isMemory)
              memoryPool.get.initRun(run)
            else
              pool ! run

            Future.successful(())
          case None =>
            logging.error(this, s"non-executable action reached the invoker ${action.fullyQualifiedName(false)}")
            Future.failed(new IllegalStateException("non-executable action reached the invoker"))
        }
      })
      .recoverWith {
        case DocumentRevisionMismatchException(_) =>
          // if revision is mismatched, the action may have been updated,
          // so try again with the latest code
          handleActivationMessage(msg.copy(revision = DocRevision.empty))
        case t =>
          val response = t match {
            case _: NoDocumentException =>
              ActivationResponse.applicationError(Messages.actionRemovedWhileInvoking)
            case _: DocumentTypeMismatchException | _: DocumentUnreadable =>
              ActivationResponse.whiskError(Messages.actionMismatchWhileInvoking)
            case _ =>
              ActivationResponse.whiskError(Messages.actionFetchErrorWhileInvoking)
          }
          activationFeed ! MessageFeed.Processed

          val activation = generateFallbackActivation(msg, response)
          ack(
            msg.transid,
            activation,
            msg.blocking,
            msg.rootControllerIndex,
            msg.user.namespace.uuid,
            CombinedCompletionAndResultMessage(transid, activation, instance))

          store(msg.transid, activation, msg.blocking, UserContext(msg.user))
          Future.successful(())
      }
  }

  def parseActivationMessage(bytes: Array[Byte]): Future[Unit] = {
    Future(ActivationMessage.parse(new String(bytes, StandardCharsets.UTF_8)))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        activationProcessor ! msg
        activationFeed ! MessageFeed.Processed
        Future.successful(())
      }.recoverWith {
      case t =>
        // Iff everything above failed, we have a terminal error at hand. Either the message failed
        // to deserialize, or something threw an error where it is not expected to throw.
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"terminal failure while processing message: $t")
        Future.successful(())
    }
  }

  /** Is called when an ActivationMessage is read from Kafka */
  def processActivationMessage(msg: ActivationMessage): Unit = {
    // The message has been parsed correctly, thus the following code needs to *always* produce at least an
    // active-ack.
    implicit val transid: TransactionId = msg.transid
    if (msg.waitForContent.isDefined) {
      if (msg.content.isDefined) {
        logging.warn(this, s"content for activation ${msg.activationId} is defined, but waitForContent is ${msg.waitForContent.get}")
      }
    // no content provided during scheduling, need to send to the waiting map until its content arrives
    activationWaiter ! msg
    return
    }
    //set trace context to continue tracing
    WhiskTracerProvider.tracer.setTraceContext(transid, msg.traceContext)

    if (!namespaceBlacklist.isBlacklisted(msg.user)) {
      transid.started(this, LoggingMarkers.INVOKER_ACTIVATION, logLevel = InfoLevel)
      val future = if (msg.prewarmOnly.isDefined) {
        handlePrewarmMessage(msg, msg.prewarmOnly.get)
      } else {
        handleActivationMessage(msg)
      }
    } else {
      // Iff the current namespace is blacklisted, an active-ack is only produced to keep the loadbalancer protocol
      // Due to the protective nature of the blacklist, a database entry is not written.
      val activation =
        generateFallbackActivation(msg, ActivationResponse.applicationError(Messages.namespacesBlacklisted))
      ack(
        msg.transid,
        activation,
        blockingInvoke = false,
        msg.rootControllerIndex,
        msg.user.namespace.uuid,
        CombinedCompletionAndResultMessage(transid, activation, instance))
      logging.warn(this, s"namespace ${msg.user.namespace.name} was blocked in invoker.")
    }
  }

  /**
   * Generates an activation with zero runtime. Usually used for error cases.
   *
   * Set the kind annotation to `Exec.UNKNOWN` since it is not known to the invoker because the action fetch failed.
   */
  private def generateFallbackActivation(msg: ActivationMessage, response: ActivationResponse): WhiskActivation = {
    val now = Instant.now
    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = msg.action.name,
      version = msg.action.version.getOrElse(SemVer()),
      start = now,
      end = now,
      duration = Some(0),
      response = response,
      annotations = {
        Parameters(WhiskActivation.pathAnnotation, JsString(msg.action.asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(Exec.UNKNOWN)) ++ causedBy
      })
  }

  private val healthProducer = msgProvider.getProducer(config)
  logging.debug(this, s"scheduling health pings to topic '${rackHealthTopic}'")
  Scheduler.scheduleWaitAtMost(1.seconds)(() => {
    healthProducer.send(rackHealthTopic, PingMessage(instance)).andThen {
      case Failure(t) => logging.error(this, s"failed to ping the rack: $t")
    }
  })

  def parseDependencyInputMessage(bytes: Array[Byte]): Future[Unit] = {
    Future(DepResultMessage.parse(new String(bytes, StandardCharsets.UTF_8)))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        activationWaiter ! msg
        depInputFeed ! MessageFeed.Processed
        Future.successful(())
      }.recoverWith {
      case t =>
        // Iff everything above failed, we have a terminal error at hand. Either the message failed
        // to deserialize, or something threw an error where it is not expected to throw.
        depInputFeed ! MessageFeed.Processed
        logging.error(this, s"terminal failure while processing message: $t")
        Future.successful(())
    }
  }

  def parseSchedResult(bytes: Array[Byte]): Future[Unit] = {
    Future(SchedulingDecision.parse(new String(bytes, StandardCharsets.UTF_8)))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        resultWaiter ! msg
        schedResultFeed ! MessageFeed.Processed
        Future.successful(())
      }.recoverWith {
      case t =>
        // Iff everything above failed, we have a terminal error at hand. Either the message failed
        // to deserialize, or something threw an error where it is not expected to throw.
        schedResultFeed ! MessageFeed.Processed
        logging.error(this, s"terminal failure while processing message: $t")
        Future.successful(())
    }
  }
}
