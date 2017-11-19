/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.sharding

import akka.routing.{ConsistentHash, MurmurHash}
import com.hypertino.hyperstorage.internal.api.NodeStatus

case class ShardedClusterData(nodes: Map[String, ShardNode], selfId: String, selfStatus: String) {
  lazy val clusterHash: Integer =  MurmurHash.stringHash(nodeStatuses.keys.toSeq.sorted.mkString("|"))

  private final lazy val consistentHash = ConsistentHash(activeNodes, VirtualNodesSize)
  private final lazy val consistentHashPrevious = ConsistentHash(previouslyActiveNodes, VirtualNodesSize)
  private final lazy val nodeStatuses: Map[String, NodeStatus.StringEnum] = {
    nodes.map {
      case (nodeId, rvm) ⇒ nodeId → rvm.status
    } + (selfId → selfStatus)
  }

  def +(elem: (String, ShardNode)) = ShardedClusterData(nodes + elem, selfId, selfStatus)

  def -(key: String) = ShardedClusterData(nodes - key, selfId, selfStatus)

  def taskIsFor(task: ShardTask): String = consistentHash.nodeFor(task.key)

  def taskWasFor(task: ShardTask): String = consistentHashPrevious.nodeFor(task.key)

  private def VirtualNodesSize = 128 // todo: find a better value, configurable? http://www.tom-e-white.com/2007/11/consistent-hashing.html

  private def activeNodes: Iterable[String] = nodeStatuses.flatMap {
    case (nodeId, NodeStatus.ACTIVE) ⇒ Some(nodeId)
    case (nodeId, NodeStatus.ACTIVATING) ⇒ Some(nodeId)
    case _ ⇒ None
  }

  private def previouslyActiveNodes: Iterable[String] = nodeStatuses.flatMap {
    case (nodeId, NodeStatus.ACTIVE) ⇒ Some(nodeId)
    case (nodeId, NodeStatus.ACTIVATING) ⇒ Some(nodeId)
    case (nodeId, NodeStatus.DEACTIVATING) ⇒ Some(nodeId)
    case _ ⇒ None
  }
}
