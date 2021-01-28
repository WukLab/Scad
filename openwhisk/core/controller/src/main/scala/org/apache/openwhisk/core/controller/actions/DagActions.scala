package org.apache.openwhisk.core.controller.actions

import akka.actor.ActorSystem
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.connector.MessagingProvider
import org.apache.openwhisk.core.controller.WhiskServices
import org.apache.openwhisk.core.database.ActivationStore
import org.apache.openwhisk.core.entity.ControllerInstanceId
import org.apache.openwhisk.core.entity.types.EntityStore
import org.apache.openwhisk.spi.SpiLoader

import scala.concurrent.ExecutionContext

protected[actions] trait DagActions {
    /** The core collections require backend services to be injected in this trait. */
    services: WhiskServices =>

    /** An actor system for timed based futures. */
    protected implicit val actorSystem: ActorSystem

    /** An execution context for futures. */
    protected implicit val executionContext: ExecutionContext

    protected implicit val logging: Logging

    /**
     *  The index of the active ack topic, this controller is listening for.
     *  Typically this is also the instance number of the controller
     */
    protected val activeAckTopicIndex: ControllerInstanceId

    /** Database service to CRUD actions. */
    protected val entityStore: EntityStore

    /** Database service to get activations. */
    protected val activationStore: ActivationStore

  /** Message producer. This is needed to write user-metrics. */
  private val messagingProvider = SpiLoader.get[MessagingProvider]
  private val producer = messagingProvider.getProducer(services.whiskConfig)

}
