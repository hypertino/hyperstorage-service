/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.sharding.consulzmq

import akka.actor.ActorRef
import com.hypertino.binders.value.Null
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{Accepted, EmptyBody, Headers, MessagingContext, RequestBase, RequestMeta, ResponseBase}
import com.hypertino.hyperbus.subscribe.Subscribable
import com.hypertino.hyperbus.transport.ZMQHeader
import com.hypertino.hyperbus.transport.api.{ServiceEndpoint, ServiceResolver}
import com.hypertino.hyperstorage.internal.api.{RemoteTask, TaskResultsPost, TasksPost}
import com.hypertino.hyperstorage.sharding.{ClusterTransport, TransportNodeDown, TransportNodeUp, TransportStarted}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.{Ack, Scheduler}
import monix.execution.Ack.Continue
import scaldi.{Injectable, Injector}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

private [consulzmq] case class ZMQConsulShardingTransportConfig(advertisedAddress: String, advertisedPort: Int)
private [consulzmq] case class ZCNode(address: String, port: Int) {
  def nodeId = s"$address-$port"
}

class ZMQConsulShardingTransport(
                                config: Config
                                )(implicit inj: Injector) extends ClusterTransport
  with StrictLogging with Injectable with AutoCloseable with Subscribable {

  import com.hypertino.binders.config.ConfigBinders._
  private val shardingConfig = config.read[ZMQConsulShardingTransportConfig]("node")
  private val selfNodeId = shardingConfig.advertisedAddress + "-" + shardingConfig.advertisedPort
  private val internalHyperbus = new Hyperbus(config.getConfig("internal-hyperbus"))
  private val resolver = inject[ServiceResolver] (identified by "hyperstorage-shard-resolver")
  private val dummyTask = TasksPost(RemoteTask(selfNodeId,1,"","",0,false,Null,"",Null))(MessagingContext.empty)
  private implicit val scheduler = inject[Scheduler]
  private val hyperbusSubscriptions = internalHyperbus.subscribe(this)
  private val stateLock = new Object
  @volatile private var subscriber: ActorRef = null
  @volatile private var nodes = Set.empty[ZCNode]
  private val resolverSubscription = resolver.serviceObservable(dummyTask).subscribe(onNodesUpdate _)

  protected def onNodesUpdate(update: Seq[ServiceEndpoint]): Future[Ack] = {
    stateLock.synchronized {
      val updatedNodes = update
        .map(se ⇒ ZCNode(se.hostname, se.port.getOrElse(shardingConfig.advertisedPort)))
        .toSet

      val newNodes = updatedNodes.diff(nodes)
      val removedNodes = nodes.diff(updatedNodes)

      nodes = updatedNodes
      if (subscriber != null) {
        newNodes.foreach{ n ⇒
          subscriber ! TransportNodeUp(n.nodeId)
        }
        removedNodes.foreach{ n ⇒
          subscriber ! TransportNodeDown(n.nodeId)
        }
      }
    }
    Continue
  }

  def onTasksPost(implicit r: TasksPost): Task[ResponseBase] = {
    if (subscriber != null) {
      subscriber ! r
    }
    Task.now(Accepted(EmptyBody))
  }

  def onTasksPost(implicit r: TaskResultsPost): Task[ResponseBase] = {
    if (subscriber != null) {
      subscriber ! r
    }
    Task.now(Accepted(EmptyBody))
  }

  override def fireMessage[T <: RequestBase](nodeId: String, message: T)(implicit requestMeta: RequestMeta[T]): Unit = {
    val directMessage = message.copyWithHeaders(Headers(ZMQHeader.ZMQ_SEND_TO → nodeId))
      .asInstanceOf[T]
    internalHyperbus
      .ask(directMessage)
      .onErrorRecover {
        case t: Throwable ⇒
          logger.warn("Shard message failed", t)
      }
      .runAsync // todo: intelligent wait or make separate pool for this?
  }

  override def subscribe(actor: ActorRef): Unit = {
    stateLock.synchronized {
      subscriber = actor
      subscriber ! TransportStarted(selfNodeId)
      nodes.foreach{ n ⇒
        subscriber ! TransportNodeUp(n.nodeId)
      }
    }
  }

  override def unsubscribe(actor: ActorRef): Unit = {
    stateLock.synchronized {
      subscriber = null
      close()
    }
  }

  override def close(): Unit = {
    try {
      resolverSubscription.cancel
      hyperbusSubscriptions.foreach(_.cancel)
      Await.result(internalHyperbus.shutdown(30.seconds).runAsync, 30.seconds)
    }
    catch {
      case t: Throwable ⇒
        logger.error("Can't stop internal hyperbus", t)
    }
  }
}
