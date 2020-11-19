package org.apache.openwhisk.core.topbalancer

// States an Invoker can be in
sealed trait RackState {
  val asString: String
  val isUsable: Boolean
}

object InvokerState {
  // Invokers in this state can be used to schedule workload to
  sealed trait Usable extends RackState { val isUsable = true }
  // No workload should be scheduled to invokers in this state
  sealed trait Unusable extends RackState { val isUsable = false }

  // A completely healthy invoker, pings arriving fine, no system errors
  case object Healthy extends Usable { val asString = "up" }
  // Pings are arriving fine, the invoker returns system errors though
  case object Unhealthy extends Unusable { val asString = "unhealthy" }
  // Pings are arriving fine, the invoker does not respond with active-acks in the expected time though
  case object Unresponsive extends Unusable { val asString = "unresponsive" }
  // Pings are not arriving for this invoker
  case object Offline extends Unusable { val asString = "down" }
}
