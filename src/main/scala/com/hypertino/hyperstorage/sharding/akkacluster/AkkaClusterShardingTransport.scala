/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.sharding.akkacluster

import akka.actor.{Actor, ActorContext, ActorRef, ActorSelection, AddressFromURIString, Props, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, ClusterEvent}
import com.hypertino.hyperbus.model.{RequestBase, RequestMeta}
import com.hypertino.hyperstorage.sharding._
import com.typesafe.scalalogging.StrictLogging

private[akkacluster] case object SubscribeToEvents
private[akkacluster] case object UnsubscribeFromEvents
private[akkacluster] case class AKMessage(nodeId: String, message: RequestBase)

class AkkaClusterShardingTransportActor(roleName: String) extends Actor with StrictLogging {
  private val cluster = Cluster(context.system)
  if (!cluster.selfRoles.contains(roleName)) {
    logger.error(s"Cluster doesn't contains '$roleName' role. Please configure.")
  }

  override def receive = {
    case SubscribeToEvents ⇒
      cluster.subscribe(self, initialStateMode = ClusterEvent.InitialStateAsEvents, classOf[MemberEvent])
      sender() ! TransportStarted(cluster.selfAddress.toString)
      context.become(subscribed(sender))
  }

  def subscribed(subscriber: ActorRef): Receive = {
    case MemberUp(member) if member.hasRole(roleName) ⇒ subscriber ! TransportNodeUp(member.address.toString)
    case MemberExited(member) if member.hasRole(roleName) => subscriber ! TransportNodeDown(member.address.toString)
    case MemberRemoved(member, _) if member.hasRole(roleName) ⇒ subscriber ! TransportNodeDown(member.address.toString)
    case AKMessage(nodeId, message) ⇒ try {
      val actor = context.actorSelection(RootActorPath(AddressFromURIString(nodeId)) / "user" / roleName)
      actor ! message
    } catch  {
      case t: Throwable ⇒
        logger.error(s"Can't send transport message: $message", t)
    }
    case UnsubscribeFromEvents ⇒ {
      context.unbecome
      cluster.unsubscribe(self)
      context.stop(self)
    }
  }
}

object AkkaClusterShardingTransportActor {
  def props(
             roleName: String
           ) = Props(new AkkaClusterShardingTransportActor(roleName))
}

class AkkaClusterShardingTransport(transportActor: ActorRef) extends ClusterTransport {
  override def subscribe(actor: ActorRef): Unit = {
    transportActor.!(SubscribeToEvents)(actor)
  }
  override def unsubscribe(actor: ActorRef): Unit = {
    transportActor.!(UnsubscribeFromEvents)(actor)
  }
  override def fireMessage[T <: RequestBase](nodeId: String, message: T)(implicit requestMeta: RequestMeta[T]): Unit = {
    transportActor ! AKMessage(nodeId, message)
  }
}
