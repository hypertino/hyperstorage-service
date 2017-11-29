/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.sharding

import akka.actor.ActorRef
import com.hypertino.hyperbus.model.{RequestBase, RequestMeta}

case class TransportNodeUp(nodeId: String) extends AnyVal
case class TransportNodeDown(nodeId: String) extends AnyVal
case class TransportStarted(nodeId: String)

trait ClusterTransport {
  def subscribe(actor: ActorRef): Unit
  def unsubscribe(actor: ActorRef): Unit
  def fireMessage[T <: RequestBase](nodeId: String, message: T)(implicit requestMeta: RequestMeta[T]): Unit
}
