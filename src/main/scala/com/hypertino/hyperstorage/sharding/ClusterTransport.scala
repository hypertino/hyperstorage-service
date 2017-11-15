package com.hypertino.hyperstorage.sharding

trait TransportNode {
  def nodeId: String
}

sealed trait TransportNodeEvent
case class TransportNodeUp(node: TransportNode) extends TransportNodeEvent
case class TransportNodeDown(nodeId: String) extends TransportNodeEvent

case class TransportMessage(node: TransportNode, message: Any)
case class TransportStarted(nodeId: String)

case object SubscribeToEvents
