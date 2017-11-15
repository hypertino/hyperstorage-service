/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.sharding.akkacluster

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, ClusterEvent, Member}
import com.hypertino.hyperstorage.internal.api.NodeStatus
import com.hypertino.hyperstorage.sharding._

private[akkacluster] case class AKNode(nodeId: String,
                                       actorRef: ActorSelection) extends TransportNode

class AkkaClusterShardingTransport(roleName: String) extends Actor with ActorLogging{
  private val cluster = Cluster(context.system)
  if (!cluster.selfRoles.contains(roleName)) {
    log.error(s"Cluster doesn't contains '$roleName' role. Please configure.")
  }

  override def receive = {
    case SubscribeToEvents ⇒
      cluster.subscribe(self, initialStateMode = ClusterEvent.InitialStateAsEvents, classOf[MemberEvent])
      sender() ! TransportStarted(cluster.selfAddress.toString)
      context.become(subscribed(sender))
  }

  def subscribed(subscriber: ActorRef): Receive = {
    case MemberUp(member) if member.hasRole(roleName) ⇒ subscriber ! TransportNodeUp(akn(member))
    case MemberExited(member) if member.hasRole(roleName) => subscriber ! TransportNodeDown(member.address.toString)
    case MemberRemoved(member, _) if member.hasRole(roleName) ⇒ subscriber ! TransportNodeDown(member.address.toString)
    // case MemberLeft(member) ⇒ subscriber ! TransportNodeDown(member.address.toString) todo: zmq do we need this?

    case TransportMessage(AKNode(_, actor), message) ⇒ actor forward message
    //case DeliverTask(AKNode(_, actor), task) ⇒ actor forward task
  }

  private def akn(member: Member): AKNode = {
    val actor = context.actorSelection(RootActorPath(member.address) / "user" / roleName)
    AKNode(member.address.toString, actor)
  }
}

object AkkaClusterShardingTransport {
  def props(
             roleName: String
           ) = Props(classOf[AkkaClusterShardingTransport],
    roleName
  )
}
