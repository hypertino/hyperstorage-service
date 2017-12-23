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

case class ShardedClusterData(nodesExceptSelf: Map[String, ShardNode], selfId: String, selfStatus: String) {
  private final val allNodesStatuses: Map[String, NodeStatus.StringEnum] = {
    nodesExceptSelf.map {
      case (nodeId, rvm) ⇒ nodeId → rvm.status
    } + (selfId → selfStatus)
  }
  val clusterHash: Integer =  MurmurHash.stringHash(allNodesStatuses.keys.toSeq.sorted.mkString("|"))
  private final val consistentHash = ConsistentHash(activeNodes, VirtualNodesSize)
  private final val consistentHashPrevious = ConsistentHash(previouslyActiveNodes, VirtualNodesSize)

  def +(elem: (String, ShardNode)) = ShardedClusterData(nodesExceptSelf + elem, selfId, selfStatus)

  def -(key: String) = ShardedClusterData(nodesExceptSelf - key, selfId, selfStatus)

  def keyIsFor(key: String): String = consistentHash.nodeFor(key)

  def taskIsFor(task: ShardTask): String = keyIsFor(task.key)

  def keyWasFor(key: String): String = consistentHashPrevious.nodeFor(key)

  def taskWasFor(task: ShardTask): String = keyWasFor(task.key)

  private def VirtualNodesSize = 128 // todo: find a better value, configurable? http://www.tom-e-white.com/2007/11/consistent-hashing.html

  private def activeNodes: Iterable[String] = allNodesStatuses.flatMap {
    case (nodeId, NodeStatus.ACTIVE) ⇒ Some(nodeId)
    case (nodeId, NodeStatus.ACTIVATING) ⇒ Some(nodeId)
    case _ ⇒ None
  }

  private def previouslyActiveNodes: Iterable[String] = allNodesStatuses.flatMap {
    case (nodeId, NodeStatus.ACTIVE) ⇒ Some(nodeId)
    case (nodeId, NodeStatus.ACTIVATING) ⇒ Some(nodeId)
    case (nodeId, NodeStatus.DEACTIVATING) ⇒ Some(nodeId)
    case _ ⇒ None
  }
}
