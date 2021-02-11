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

package org.apache.openwhisk.core.sched

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.event.Logging.InfoLevel
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import kamon.Kamon
import pureconfig._
import pureconfig.generic.auto._
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.openwhisk.common.Https.HttpsConfig
import org.apache.openwhisk.common.{AkkaLogging, ConfigMXBean, Logging, LoggingMarkers, Scheduler, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig.kafkaHosts
import org.apache.openwhisk.core.connector.{MessagingProvider, PingRackMessage}
import org.apache.openwhisk.core.containerpool.RuntimeResources
import org.apache.openwhisk.core.containerpool.logging.LogStoreProvider
import org.apache.openwhisk.core.controller.RestApiCommons
import org.apache.openwhisk.core.database.{ActivationStoreProvider, CacheChangeNotification, RemoteCacheInvalidation}
import org.apache.openwhisk.core.entitlement._
import org.apache.openwhisk.core.entity.ActivationId.ActivationIdGenerator
import org.apache.openwhisk.core.entity.ExecManifest.Runtimes
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.loadBalancer.InvokerState
import org.apache.openwhisk.http.{BasicHttpService, BasicRasService}
import org.apache.openwhisk.spi.SpiLoader
import pureconfig.ConfigReader.Result

import scala.concurrent.ExecutionContext.Implicits
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}
import scala.util.Try

case class CmdLineArgs(uniqueName: Option[String] = None, id: Option[Int] = None, displayedName: Option[String] = None)

/**
 * The RackSched is the service that provides coarse-grained scheduling and Rest APIs
 **/
class
RackSched(val instance: RackSchedInstanceId,
                runtimes: Runtimes,
                implicit val whiskConfig: WhiskConfig,
                implicit val actorSystem: ActorSystem,
                implicit val materializer: ActorMaterializer,
                implicit val logging: Logging)
    extends BasicRasService {

  TransactionId.racksched.mark(
    this,
    LoggingMarkers.RACKSCHED_STARTUP(instance.toString),
    s"starting racksched instance ${instance.toString}",
    logLevel = InfoLevel)

  /**
   * A Route in Akka is technically a function taking a RequestContext as a parameter.
   *
   * The "~" Akka DSL operator composes two independent Routes, building a routing tree structure.
   * @see http://doc.akka.io/docs/akka-http/current/scala/http/routing-dsl/routes.html#composing-routes
   */
  override def routes(implicit transid: TransactionId): Route = {
    super.routes ~ {
      (pathEndOrSingleSlash & get) {
        complete(info)
      }
    } ~ internalInvokerHealth
  }

  // initialize datastores
  private implicit val entityStore = WhiskEntityStore.datastore()
  private implicit val cacheChangeNotification = Some(new CacheChangeNotification {
    val remoteCacheInvalidaton = new RemoteCacheInvalidation(whiskConfig, "racksched", instance)
    override def apply(k: CacheKey) = {
      remoteCacheInvalidaton.invalidateWhiskActionMetaData(k)
      remoteCacheInvalidaton.notifyOtherInstancesAboutInvalidation(k)
    }
  })

  // initialize backend services
  private implicit val loadBalancer =
    SpiLoader.get[RackLoadBalancerProvider].instance(whiskConfig, instance)
  logging.info(this, s"rackbalancer initialized: ${loadBalancer.getClass.getSimpleName}")(TransactionId.racksched)



  private implicit val activationIdFactory = new ActivationIdGenerator {}
  private implicit val logStore = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  private implicit val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, materializer, logging)

  // register collections
  Collection.initialize(entityStore)

  /** The REST APIs. */
  implicit val rackschedInstance: RackSchedInstanceId = instance
//  private val apiV1 = new RestAPIVersion(whiskConfig, "api", "v1")

  /**
   * Handles GET /invokers - list of invokers
   *             /invokers/healthy/count - nr of healthy invokers
   *             /invokers/ready - 200 in case # of healthy invokers are above the expected value
   *                             - 500 in case # of healthy invokers are bellow the expected value
   *
   * @return JSON with details of invoker health or count of healthy invokers respectively.
   */
  protected[sched] val internalInvokerHealth: Route = {
    implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher
    (pathPrefix("invokers") & get) {
      pathEndOrSingleSlash {
        complete {
          loadBalancer
            .invokerHealth()
            .map(_.map(i => i.id.toString -> i.status.asString).toMap.toJson.asJsObject)
        }
      } ~ path("healthy" / "count") {
        complete {
          loadBalancer
            .invokerHealth()
            .map(_.count(_.status == InvokerState.Healthy).toJson)
        }
      } ~ path("ready") {
        onSuccess(loadBalancer.invokerHealth()) { invokersHealth =>
          val all = invokersHealth.size
          val healthy = invokersHealth.count(_.status == InvokerState.Healthy)
          val ready = RackSched.readyState(all, healthy, RackSched.readinessThreshold.getOrElse(1))
          if (ready)
            complete(JsObject("healthy" -> s"$healthy/$all".toJson))
          else
            complete(InternalServerError -> JsObject("unhealthy" -> s"${all - healthy}/$all".toJson))
        }
      }
    }
  }

  // racksched top level info
  private val info =
    RackSched.info(whiskConfig, TimeLimit.config, MemoryLimit.config, LogLimit.config, runtimes, List("/fillerAPIPath"))
}

/**
 * Singleton object provides a factory to create and start an instance of the racksched service.
 */
object RackSched {

  protected val protocol: String = loadConfigOrThrow[String]("whisk.rackloadbalancer.protocol")
  protected val interface: String = loadConfigOrThrow[String]("whisk.rackloadbalancer.interface")
  protected val readinessThreshold: Result[Double] = loadConfig[Double]("whisk.rackloadbalancer.readiness-fraction")

  // requiredProperties is a Map whose keys define properties that must be bound to
  // a value, and whose values are default values.   A null value in the Map means there is
  // no default value specified, so it must appear in the properties file
  def requiredProperties =
    ExecManifest.requiredProperties ++
      RestApiCommons.requiredProperties ++
      SpiLoader.get[RackLoadBalancerProvider].requiredProperties ++
      kafkaHosts

  def info(config: WhiskConfig,
           timeLimit: TimeLimitConfig,
           memLimit: MemoryLimitConfig,
           logLimit: MemoryLimitConfig,
           runtimes: Runtimes,
           apis: List[String]) =
    JsObject(
      "description" -> "OpenWhisk".toJson,
      "support" -> JsObject(
        "github" -> "https://github.com/apache/openwhisk/issues".toJson,
        "slack" -> "http://slack.openwhisk.org".toJson),
      "api_paths" -> apis.toJson,
      "limits" -> JsObject(
        "actions_per_minute" -> config.actionInvokePerMinuteLimit.toInt.toJson,
        "triggers_per_minute" -> config.triggerFirePerMinuteLimit.toInt.toJson,
        "concurrent_actions" -> config.actionInvokeConcurrentLimit.toInt.toJson,
        "sequence_length" -> config.actionSequenceLimit.toInt.toJson,
        "min_action_duration" -> timeLimit.min.toMillis.toJson,
        "max_action_duration" -> timeLimit.max.toMillis.toJson,
        "min_action_memory" -> memLimit.min.toBytes.toJson,
        "max_action_memory" -> memLimit.max.toBytes.toJson,
        "min_action_logs" -> logLimit.min.toBytes.toJson,
        "max_action_logs" -> logLimit.max.toBytes.toJson),
      "runtimes" -> runtimes.toJson)

  def readyState(allInvokers: Int, healthyInvokers: Int, readinessThreshold: Double): Boolean = {
    if (allInvokers > 0) (healthyInvokers / allInvokers) >= readinessThreshold else false
  }

  def main(args: Array[String]): Unit = {
    implicit val actorSystem = ActorSystem("racksched-actor-system")
    implicit val logger = new AkkaLogging(akka.event.Logging.getLogger(actorSystem, this))
    start(args)
  }

  def start(args: Array[String])(implicit actorSystem: ActorSystem, logger: Logging): Unit = {
    try {
      ConfigMXBean.register()
    } catch { case e : Throwable =>
      logger.error(this, s"Config MXBean Error, $e")
    }
    Kamon.init()

    // Prepare Kamon shutdown
    CoordinatedShutdown(actorSystem).addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "shutdownKamon") { () =>
      logger.info(this, s"Shutting down Kamon with coordinated shutdown")
      Kamon.stopModules().map(_ => Done)(Implicits.global)
    }

    // extract configuration data from the environment
    val config = new WhiskConfig(requiredProperties)
    val port = config.servicePort.toInt

    // if deploying multiple instances (scale out), must pass the instance number as the
    require(args.length >= 1, "racksched instance required")

    /** Returns Some(s) if the string is not empty with trimmed whitespace, None otherwise. */
    def nonEmptyString(s: String): Option[String] = {
      val trimmed = s.trim
      if (trimmed.nonEmpty) Some(trimmed) else None
    }

    def abort(message: String) = {
      logger.error(this, message)
      actorSystem.terminate()
      Await.result(actorSystem.whenTerminated, 30.seconds)
      sys.exit(1)
    }

    def parse(ls: List[String], c: CmdLineArgs): CmdLineArgs = {
      ls match {
        case "--uniqueName" :: uniqueName :: tail =>
          parse(tail, c.copy(uniqueName = nonEmptyString(uniqueName)))
        case "--displayedName" :: displayedName :: tail =>
          parse(tail, c.copy(displayedName = nonEmptyString(displayedName)))
        case "--id" :: id :: tail if Try(id.toInt).isSuccess =>
          parse(tail, c.copy(id = Some(id.toInt)))
        case Nil => c
        case _   => abort(s"Error processing command line arguments $ls")
      }
    }
    
    val cmdLineArgs = parse(args.toList, CmdLineArgs())
    logger.info(this, "Command line arguments parsed to yield " + cmdLineArgs)
    
    val id = cmdLineArgs match {
      // --id is defined with a valid value, use this id directly.
      case CmdLineArgs(_, Some(id), _) =>
        logger.info(this, s"rackschedReg: using proposedRackschedId $id")
        id

      case _ => abort(s"--id must be configured with correct values")
    }
    
    if (id < 0) {
      throw new RuntimeException(s"Rack scheduler instance ID must be > 0. Got id number ${id}")
    }
    val instance = new RackSchedInstanceId(id,
      new RuntimeResources(0, ByteSize.fromString("0B"), ByteSize.fromString("0B")),
      None, None)

    if (!config.isValid) {
      abort("Bad configuration, cannot start.")
    }

    val msgProvider = SpiLoader.get[MessagingProvider]

    Seq(
      ("completed" + instance.toString, "completed", Some(ActivationEntityLimit.MAX_ACTIVATION_LIMIT)),
      ("health", "health", None),
      ("rackHealth", "rackHealth", None),
      (instance.toString, instance.toString, None),
      ("cacheInvalidation", "cache-invalidation", None),
      ("events", "events", None)).foreach {
      case (topic, topicConfigurationKey, maxMessageBytes) =>
        if (msgProvider.ensureTopic(config, topic, topicConfigurationKey, maxMessageBytes).isFailure) {
          abort(s"failure during msgProvider.ensureTopic for topic $topic")
        }
    }

    val healthProducer = msgProvider.getProducer(config)
    Scheduler.scheduleWaitAtMost(1.seconds)(() => {
      healthProducer.send("rackHealth", PingRackMessage(instance))
    })

    ExecManifest.initialize(config) match {
      case Success(_) =>
        val racksched = new RackSched(
          instance,
          ExecManifest.runtimesManifest,
          config,
          actorSystem,
          ActorMaterializer.create(actorSystem),
          logger)

       val httpsConfig =
         if (RackSched.protocol == "https") Some(loadConfigOrThrow[HttpsConfig]("whisk.rackloadbalancer.https")) else None

       BasicHttpService.startHttpService(racksched.route, port, httpsConfig, interface)(
         actorSystem,
         racksched.materializer)

      case Failure(t) =>
        abort(s"Invalid runtimes manifest: $t")
    }
  }
}
