/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

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
