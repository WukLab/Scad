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
import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown
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
import org.apache.openwhisk.common.{AkkaLogging, ConfigMXBean, Logging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector.MessagingProvider
import org.apache.openwhisk.core.containerpool.logging.LogStoreProvider
import org.apache.openwhisk.core.controller.{RestAPIVersion, RestApiCommons}
import org.apache.openwhisk.core.database.{ActivationStoreProvider, CacheChangeNotification, RemoteCacheInvalidation}
import org.apache.openwhisk.core.entitlement._
import org.apache.openwhisk.core.entity.ActivationId.ActivationIdGenerator
import org.apache.openwhisk.core.entity.ExecManifest.Runtimes
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.loadBalancer.{InvokerState, LoadBalancerProvider}
import org.apache.openwhisk.http.{BasicHttpService, BasicRasService}
import org.apache.openwhisk.spi.SpiLoader

import scala.concurrent.ExecutionContext.Implicits
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import scala.util.{Failure, Success}

/**
 * The RackSched is the service that provides coarse-grained scheduling and Rest APIs
 **/
class RackSched(val instance: RackSchedInstanceId,
                runtimes: Runtimes,
                implicit val whiskConfig: WhiskConfig,
                implicit val actorSystem: ActorSystem,
                implicit val materializer: ActorMaterializer,
                implicit val logging: Logging)
    extends BasicRasService {

  TransactionId.racksched.mark(
    this,
    LoggingMarkers.CONTROLLER_STARTUP(instance.asString),
    s"starting racksched instance ${instance.asString}",
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
    } ~ apiV1.routes ~ internalInvokerHealth
  }

  // initialize datastores
  private implicit val authStore = WhiskAuthStore.datastore()
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
  logging.info(this, s"loadbalancer initialized: ${loadBalancer.getClass.getSimpleName}")(TransactionId.controller)

  private implicit val entitlementProvider =
    SpiLoader.get[EntitlementSpiProvider].instance(whiskConfig, loadBalancer, instance)
  private implicit val activationIdFactory = new ActivationIdGenerator {}
  private implicit val logStore = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  private implicit val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, materializer, logging)

  // register collections
  Collection.initialize(entityStore)

  /** The REST APIs. */
  implicit val rackschedInstance = instance
  private val apiV1 = new RestAPIVersion(whiskConfig, "api", "v1")

  /**
   * Handles GET /invokers - list of invokers
   *             /invokers/healthy/count - nr of healthy invokers
   *             /invokers/ready - 200 in case # of healthy invokers are above the expected value
   *                             - 500 in case # of healthy invokers are bellow the expected value
   *
   * @return JSON with details of invoker health or count of healthy invokers respectively.
   */
  protected[RackSched] val internalInvokerHealth = {
    implicit val executionContext = actorSystem.dispatcher
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

  // controller top level info
  private val info =
    RackSched.info(whiskConfig, TimeLimit.config, MemoryLimit.config, LogLimit.config, runtimes, List(apiV1.basepath()))
}

/**
 * Singleton object provides a factory to create and start an instance of the racksched service.
 */
object RackSched {

  protected val protocol = loadConfigOrThrow[String]("whisk.racksched.protocol")
  protected val interface = loadConfigOrThrow[String]("whisk.racksched.interface")
  protected val readinessThreshold = loadConfig[Double]("whisk.racksched.readiness-fraction")

  // requiredProperties is a Map whose keys define properties that must be bound to
  // a value, and whose values are default values.   A null value in the Map means there is
  // no default value specified, so it must appear in the properties file
  def requiredProperties =
    ExecManifest.requiredProperties ++
      RestApiCommons.requiredProperties ++
      SpiLoader.get[LoadBalancerProvider].requiredProperties ++
      EntitlementProvider.requiredProperties

  private def info(config: WhiskConfig,
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
    implicit val actorSystem = ActorSystem("controller-actor-system")
    implicit val logger = new AkkaLogging(akka.event.Logging.getLogger(actorSystem, this))
    start(args)
  }

  def start(args: Array[String])(implicit actorSystem: ActorSystem, logger: Logging): Unit = {
    ConfigMXBean.register()
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
    require(args.length >= 1, "controller instance required")
    val instance = new RackSchedInstanceId(args(0))

    def abort(message: String) = {
      logger.error(this, message)
      actorSystem.terminate()
      Await.result(actorSystem.whenTerminated, 30.seconds)
      sys.exit(1)
    }

    if (!config.isValid) {
      abort("Bad configuration, cannot start.")
    }

    val msgProvider = SpiLoader.get[MessagingProvider]

    Seq(
      ("completed" + instance.asString, "completed", Some(ActivationEntityLimit.MAX_ACTIVATION_LIMIT)),
      ("health", "health", None),
      ("cacheInvalidation", "cache-invalidation", None),
      ("events", "events", None)).foreach {
      case (topic, topicConfigurationKey, maxMessageBytes) =>
        if (msgProvider.ensureTopic(config, topic, topicConfigurationKey, maxMessageBytes).isFailure) {
          abort(s"failure during msgProvider.ensureTopic for topic $topic")
        }
    }

    ExecManifest.initialize(config) match {
      case Success(_) =>
        val controller = new RackSched(
          instance,
          ExecManifest.runtimesManifest,
          config,
          actorSystem,
          ActorMaterializer.create(actorSystem),
          logger)

        val httpsConfig =
          if (RackSched.protocol == "https") Some(loadConfigOrThrow[HttpsConfig]("whisk.controller.https")) else None

        BasicHttpService.startHttpService(controller.route, port, httpsConfig, interface)(
          actorSystem,
          controller.materializer)

      case Failure(t) =>
        abort(s"Invalid runtimes manifest: $t")
    }
  }
}
