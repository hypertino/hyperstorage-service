/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.sharding

import java.io.StringReader

import akka.actor._
import akka.cluster.ClusterEvent._
import com.hypertino.binders.value.{Obj, Value}
import com.hypertino.hyperbus.model.{Headers, MessagingContext, Ok, RequestBase, RequestHeaders, RequestMeta, RequestMetaCompanion, ResponseBase}
import com.hypertino.hyperstorage.internal.api
import com.hypertino.hyperstorage.internal.api._
import com.hypertino.hyperstorage.metrics.Metrics
import com.hypertino.hyperstorage.utils.AkkaNaming
import com.hypertino.metrics.MetricsTracker
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal

@SerialVersionUID(1L) trait ShardTask {
  def key: String

  def group: String

  def ttl: Long

  def expectsResult: Boolean

  def isExpired: Boolean = ttl < System.currentTimeMillis()
}

case class LocalTask(key: String, group: String, ttl: Long, expectsResult: Boolean, request: RequestBase, extra: Value) extends ShardTask

//case class RemoteRTask(sourceNodeId: String, taskId: Long, key: String, group: String, ttl: Long, expectsResult: Boolean, task: ShardTask) extends ShardTask with Serializable

//case class RemoteRTaskResult(taskId: Long, key: String, result: Any)

@SerialVersionUID(1L) case class NoSuchGroupWorkerException(groupName: String) extends RuntimeException(s"No such worker group: $groupName")

case class ShardNode(transportNode: TransportNode,
                     status: String,
                     confirmedStatus: String)

// todo: is this used?
case class SubscribeToShardStatus(subscriber: ActorRef)

// todo: is this used?
case class UpdateShardStatus(self: ActorRef, status: String, stateData: ShardedClusterData)

private[sharding] case object ShardSyncTimer

case object ShutdownProcessor

case class WorkerTaskResult(key: String, group: String, result: Option[ResponseBase], extra: Value) // r: Try[Option[ResponseBase]]

case class ExpectingRemoteResult(client: ActorRef, ttl: Long, key: String, requestMeta: RequestMeta[_ <: RequestBase]) {
  def isExpired: Boolean = ttl < System.currentTimeMillis()
}

case class WorkerGroupSettings(props: Props, maxCount: Int, prefix: String, metaCompanions: Seq[RequestMetaCompanion[_ <: RequestBase]]) {
  private val m : Map[(String,String), RequestMetaCompanion[_ <: RequestBase]] = metaCompanions.map { mc ⇒
    (mc.location, mc.method) → mc
  }.toMap

  def lookupRequestMeta(headers: RequestHeaders): RequestMeta[_ <: RequestBase] = {
    val l = (headers.hrl.location,headers.method)
    m.get(l) match {
      case Some(mc) ⇒ mc
      case None ⇒ throw new IllegalArgumentException(s"Didn't found RequestMeta for $l")
    }
  }
}

private [sharding] case class ActiveWorkerRemoteData(nodeId: String, taskId: Long/*, meta: RequestMeta[_ <: RequestBase]*/)

private [sharding] case class ActiveWorker(taskId: Long,
                                           workerActor: ActorRef,
                                           client: Option[ActorRef],
                                           remoteData: Option[ActiveWorkerRemoteData]
                                          )

class ShardProcessor(clusterTransport: ActorRef,
                     workersSettings: Map[String, WorkerGroupSettings],
                     tracker: MetricsTracker,
                     syncTimeout: FiniteDuration = 1000.millisecond)
  extends FSMEx[String, ShardedClusterData] with Stash with StrictLogging {

  private val activeWorkers = workersSettings.keys.map { // todo: make this Map of Map!
    _ → mutable.Map[String, ActiveWorker]()
  }.toMap
  private val shardStatusSubscribers = mutable.MutableList[ActorRef]()
  private val remoteTasks = mutable.Map[Long, ExpectingRemoteResult]()

  // trackers
  private val trackStashMeter = tracker.meter(Metrics.SHARD_PROCESSOR_STASH_METER)
  private val trackTaskMeter = tracker.meter(Metrics.SHARD_PROCESSOR_TASK_METER)
  private val trackForwardMeter = tracker.meter(Metrics.SHARD_PROCESSOR_FORWARD_METER)
  private var lastTaskId: Long = 0

  clusterTransport ! SubscribeToEvents

  startWith(NodeStatus.ACTIVATING, ShardedClusterData(Map.empty, "", NodeStatus.ACTIVATING))

  when(NodeStatus.ACTIVATING) {
    case Event(TransportStarted(nodeId), data) ⇒
      stay using data.copy(selfId=nodeId)

    case Event(TransportNodeUp(node), data) ⇒
      updateAndStay(introduceSelfTo(node, data))

    case Event(ShardSyncTimer, data) ⇒
      if (isActivationAllowed(data)) {
        goto(NodeStatus.ACTIVE)
      }
      else {
        stay
      }

    case Event(task: ShardTask, data) ⇒
      holdTask(task, data)
      stay
  }

  when(NodeStatus.ACTIVE) {
    case Event(ShardSyncTimer, data) ⇒
      confirmStatus(data, NodeStatus.ACTIVE, isFirst = false)
      stay

    case Event(ShutdownProcessor, data) ⇒
      confirmStatus(data, NodeStatus.DEACTIVATING, isFirst = true)
      setSyncTimer()
      goto(NodeStatus.DEACTIVATING) using data.copy(selfStatus = NodeStatus.DEACTIVATING)

    case Event(task: ShardTask, data) ⇒
      processTask(task, data)
      stay
  }

  when(NodeStatus.DEACTIVATING) {
    case Event(ShardSyncTimer, data) ⇒
      if (confirmStatus(data, NodeStatus.DEACTIVATING, isFirst = false)) {
        confirmStatus(data, NodeStatus.PASSIVE, isFirst = false) // not reliable
        stop()
      }
      else {
        stay
      }

    // ignore those when Deactivating
    case Event(MemberLeft(member), _) ⇒ stay()
    case Event(ShutdownProcessor, _) ⇒ stay()
  }

  whenUnhandled {
    case Event(get: NodeGet, data) ⇒
      implicit val mcx = get
      sender() ! Ok(api.Node(data.selfId, data.selfStatus, data.clusterHash))
      stay()

    case Event(sync: NodesPost, data) ⇒
      updateAndStay(incomingSync(sync, data))

    case Event(syncReply: Ok[NodeUpdated] @unchecked, data) ⇒
      updateAndStay(processReply(syncReply, data))

    case Event(TransportNodeUp(node), data) ⇒
      updateAndStay(addNode(node, data))

      case Event(TransportNodeDown(nodeId), data) ⇒
        updateAndStay(removeNode(nodeId, data))

    case Event(wt: WorkerTaskResult, data) ⇒
      workerIsReadyForNextTask(wt, data)
      stay()

    case Event(rr: RemoteTaskResult, data) ⇒
      handleRemoteTaskResult(rr)
      stay()

    case Event(task: ShardTask, data) ⇒
      forwardTask(task, data)
      stay()

    case Event(SubscribeToShardStatus(actorRef), data) ⇒
      shardStatusSubscribers += actorRef
      actorRef ! UpdateShardStatus(self, stateName, data)
      stay()

    case Event(e, s) =>
      logger.warn("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  onTransition {
    case a -> b ⇒
      if (a != b) {
        logger.info(s"Changing state from $a to $b")
        safeUnstashAll()
      }
  }

  initialize()

  override def processEventEx(event: Event, source: AnyRef): Unit = {
    val oldState = stateName
    val oldData = stateData
    super.processEventEx(event, source)
    if (oldData != stateData || oldState != stateName) {
      shardStatusSubscribers.foreach(_ ! UpdateShardStatus(sender, stateName, stateData))
    }
  }

  private def introduceSelfTo(node: TransportNode, data: ShardedClusterData): Option[ShardedClusterData] = {
    if (node.nodeId != data.selfId) {
      // todo: zmq
      val newData: ShardedClusterData = data + (node.nodeId → ShardNode(node, NodeStatus.PASSIVE, NodeStatus.PASSIVE))
      val sync = NodesPost(Node(data.selfId, NodeStatus.ACTIVATING, newData.clusterHash))(MessagingContext.empty)
      clusterTransport ! TransportMessage(node, sync)
      setSyncTimer()
      logger.info(s"New member of shard cluster: $node. $sync was sent.")

      Some(newData)
    }
    else {
      logger.info(s"Self is up: ${data.selfId}")
      setSyncTimer()
      None
    }
  }

  private def setSyncTimer(): Unit = {
    setTimer("syncing", ShardSyncTimer, syncTimeout)
  }

  private def processReply(syncReply: Ok[NodeUpdated], data: ShardedClusterData): Option[ShardedClusterData] = {
    logger.debug(s"$syncReply received from $sender")
    if (data.clusterHash != syncReply.body.clusterHash) {
      logger.info(s"ClusterHash for ${data.selfId} (${data.clusterHash}) is not matched for $syncReply. SyncReply is ignored")
      None
    } else {
      data.nodes.get(syncReply.body.sourceNodeId) map { node ⇒
        data + (syncReply.body.sourceNodeId →
          node.copy(status = syncReply.body.sourceStatus, confirmedStatus = syncReply.body.acceptedStatus))
      } orElse {
        logger.warn(s"Got $syncReply from unknown node. Current nodes: ${data.nodes}")
        None
      }
    }
  }

  private def isActivationAllowed(data: ShardedClusterData): Boolean = {
    if (confirmStatus(data, NodeStatus.ACTIVATING, isFirst = false)) {
      logger.info(s"Synced with all members: ${data.nodes}. Activating")
      confirmStatus(data, NodeStatus.ACTIVE, isFirst = true)
      true
    }
    else {
      false
    }
  }

  private def confirmStatus(data: ShardedClusterData, status: String, isFirst: Boolean): Boolean = {
    var syncedWithAllMembers = true
    data.nodes.foreach { case (address, node) ⇒
      if (node.confirmedStatus != status) {
        syncedWithAllMembers = false
        val sync = NodesPost(Node(data.selfId, status, data.clusterHash))(MessagingContext.empty)
        clusterTransport ! TransportMessage(node.transportNode, sync)
        setSyncTimer()
        if (!isFirst) {
          logger.debug(s"Didn't received reply from: $node. $sync was sent to.")
        }
      }
    }
    syncedWithAllMembers
  }

  private def incomingSync(sync: NodesPost, data: ShardedClusterData): Option[ShardedClusterData] = {
    logger.debug(s"$sync received from $sender")
    if (data.clusterHash != sync.body.clusterHash) { // todo: zmq, remove ClusterHash
      logger.info(s"ClusterHash for ${data.selfId} (${data.clusterHash}) is not matched for $sync")
      None
    } else {
      data.nodes.get(sync.body.nodeId) map { member ⇒
        val newData: ShardedClusterData = data + (sync.body.nodeId → member.copy(status = sync.body.status))
        val allowSync = if (sync.body.status == NodeStatus.ACTIVATING) {
          activeWorkers.values.flatten.forall { case (key, aw) ⇒
            if (newData.keyIsFor(key) == sync.body.nodeId) {
              logger.info(s"Ignoring sync request $sync while processing task #${aw.taskId}/$key by worker ${aw.workerActor}")
              false
            } else {
              true
            }
          }
        } else {
          true
        }

        if (allowSync) { // todo: zmq, new name for nodeStatuses? vs clusterNodes
          val syncReply = Ok(NodeUpdated(data.selfId, stateName, sync.body.status, data.clusterHash))(MessagingContext.empty)
          logger.debug(s"Replying with $syncReply to $sender")
          sender() ! syncReply
        }
        newData
      } orElse {
        logger.error(s"Got $sync from unknown member. Current members: ${data.nodes}")
        sender() ! Ok(NodeUpdated(data.selfId, stateName, NodeStatus.PASSIVE, data.clusterHash))(MessagingContext.empty)
        None
      }
    }
  }

  private def addNode(node: TransportNode, data: ShardedClusterData): Option[ShardedClusterData] = {
    if (node.nodeId != data.selfId) {
      val newData = data + (node.nodeId → ShardNode(
        node, NodeStatus.PASSIVE, NodeStatus.PASSIVE
      ))
      logger.info(s"New node $node. State is unknown yet")
      logger.debug(s"Node ${node.nodeId} is added")
      Some(newData)
    }
    else {
      None
    }
  }

  private def removeNode(nodeId: String, data: ShardedClusterData): Option[ShardedClusterData] = {
    logger.debug(s"Node $nodeId is removed")
    Some(data - nodeId)
  }

  private def processTask(task: ShardTask, data: ShardedClusterData): Unit = {
    trackTaskMeter.mark()
    logger.debug(s"Got task to process: $task")
    if (task.isExpired) {
      logger.warn(s"Task is expired, dropping: $task")
    } else {
      if (data.taskIsFor(task) == data.selfId) {
        if (data.taskWasFor(task) != data.selfId) {
          logger.debug(s"Stashing task received for deactivating node: ${data.taskWasFor(task)}: $task")
          safeStash(task)
        } else {
          activeWorkers.get(task.group) match {
            case Some(activeGroupWorkers) ⇒
              activeGroupWorkers.get(task.key) map { aw ⇒
                logger.debug(s"Stashing task for the 'locked' URL: ${task.key} while working on ${aw.taskId} @ ${aw.workerActor}")
                safeStash(task)
                true
              } getOrElse {
                val ws = workersSettings(task.group)
                if (activeGroupWorkers.size >= ws.maxCount) {
                  logger.debug(s"Worker limit for group '${task.group}' is reached (${ws.maxCount}), stashing task: $task")
                  safeStash(task)
                } else {
                  try {
                    val worker = context.system.actorOf(ws.props, AkkaNaming.next(ws.prefix))
                    logger.debug(s"Starting worker for task $task sent from ${sender()}")
                    val localTaskId = nextTaskId()
                    val (localTask, remoteData) = task match {
                      case r: RemoteTask ⇒
                        val req = deserializeRequest(r)
                        val l = LocalTask(r.key, r.group, r.ttl, r.expectsResult, req, r.extra)
                        (l, Some(ActiveWorkerRemoteData(r.sourceNodeId,r.taskId)))

                      case l: LocalTask ⇒ (l, None)
                    }
                    worker ! localTask
                    val client = if (task.expectsResult) Some(sender()) else None
                    activeGroupWorkers += task.key → ActiveWorker(localTaskId, worker, client, remoteData)
                  } catch {
                    case e: Throwable ⇒
                      logger.error(s"Can't create worker from props ${ws.props}", e)
                      sender() ! e
                  }
                }
              }
            case None ⇒
              logger.error(s"No such worker group: ${task.group}. Task is dismissed: $task")
              sender() ! NoSuchGroupWorkerException(task.group)
          }
        }
      }
      else {
        forwardTask(task, data)
      }
    }
  }

  private def holdTask(task: ShardTask, data: ShardedClusterData): Unit = {
    trackTaskMeter.mark()
    logger.debug(s"Got task to process while activating: $task")
    if (task.isExpired) {
      logger.warn(s"Task is expired, dropping: $task")
    } else {
      if (data.taskIsFor(task) == data.selfId) {
        logger.debug(s"Stashing task while activating: $task")
        safeStash(task)
      } else {
        forwardTask(task, data)
      }
    }
  }

  private def safeStash(task: ShardTask): Unit = try {
    trackStashMeter.mark()
    stash()
  } catch {
    case e: Throwable ⇒
      logger.error(s"Can't stash task: $task. It's lost now", e)
  }

  private def forwardTask(task: ShardTask, data: ShardedClusterData): Unit = {
    trackForwardMeter.mark()
    val address = data.taskIsFor(task)
    data.nodes.get(address) map { rvm ⇒
      logger.debug(s"Task is forwarded to $address: $task")
      clearExpiredRemoteTasks()
      val remoteTask = task match {
        case r: RemoteTask ⇒ r
        case l: LocalTask ⇒
          val taskId = nextTaskId()
          val bodyString = l.request.serializeToString
          val r = RemoteTask(data.selfId,taskId,l.key,l.group,l.ttl,l.expectsResult,Obj(l.request.headers.underlying.v),bodyString,l.extra)
          val requestMeta = workersSettings.get(l.group).map(_.lookupRequestMeta(l.request.headers)).getOrElse {
            throw new IllegalArgumentException(s"No settings are defined for a group ${l.group}")
          }
          remoteTasks += taskId → ExpectingRemoteResult(sender(), task.ttl, task.key, requestMeta)
          r
      }
      clusterTransport ! TransportMessage(rvm.transportNode, remoteTask)
      true
    } getOrElse {
      logger.error(s"Task actor is not found: $address, dropping: $task")
    }
  }

  private def handleRemoteTaskResult(r: RemoteTaskResult): Unit = {
    remoteTasks.get(r.taskId) match {
      case Some(rt) ⇒
        logger.debug(s"Forwarding result $r from task: #${r.taskId} to ${rt.client}")
        remoteTasks.remove(r.taskId)
        rt.client ! deserializeResponse(r, rt)

      case None ⇒
        logger.warn(s"Dropping result $r from task: #${r.taskId}, timed out?")
    }
  }

  private def workerIsReadyForNextTask(workerTaskResult: WorkerTaskResult, data: ShardedClusterData): Unit = {
    activeWorkers.get(workerTaskResult.group) match {
      case Some(activeGroupWorkers) ⇒
        activeGroupWorkers.get(workerTaskResult.key) match {
          case Some(aw) ⇒
            workerTaskResult.result.foreach { result ⇒
              aw.remoteData match {
                case None ⇒
                  aw.client.foreach { client ⇒
                    logger.debug(s"Sending result $result to $client")
                    client ! result
                  }

                case Some(remoteData) ⇒
                  data.nodes.get(remoteData.nodeId) match {
                    case Some(rvm) ⇒
                      logger.debug(s"Forwarding result $result to source node ${remoteData.nodeId}")
                      val r = RemoteTaskResult(remoteData.taskId, Obj(result.headers.v), result.serializeToString, workerTaskResult.extra)
                      clusterTransport ! TransportMessage(rvm.transportNode, r)

                    case None ⇒
                      logger.error(s"Dropping result $workerTaskResult, didn't found source node ${remoteData.nodeId}")
                  }
              }
            }

            logger.debug(s"Worker ${aw.workerActor} is ready for next task. Completed task: #${aw.taskId}/${workerTaskResult.key}, r: ${workerTaskResult.result}")
            activeGroupWorkers.remove(workerTaskResult.key)
            aw.workerActor ! PoisonPill
            safeUnstashAll()

          case None ⇒
            logger.error(s"workerIsReadyForNextTask: unknown key $workerTaskResult actor: $sender")
        }

      case None ⇒
        logger.error(s"No such worker group: ${workerTaskResult.group}. Task r from $sender with key ${workerTaskResult.key} is ignored: ${workerTaskResult.result}")
    }
  }

  private def safeUnstashAll(): Unit = try {
    logger.debug("Unstashing tasks")
    unstashAll()
  } catch {
    case e: Throwable ⇒
      logger.error(s"Can't unstash tasks. Some are lost now", e)
  }

  // todo: call this only when we need it, now it's called for each remote task
  private def clearExpiredRemoteTasks(): Unit = try {
    val expired = remoteTasks.filter(_._2.isExpired)
    expired.foreach { case (k,v) ⇒
      logger.debug(s"Removing expired remote task: #$k/${v.key}")
      remoteTasks -= k
    }
  } catch {
    case e: Throwable ⇒
      logger.error(s"Unexpected", e)
  }

  private def nextTaskId(): Long = {
    lastTaskId+=1
    lastTaskId
  }

  private def deserializeRequest(remoteTask: RemoteTask): RequestBase = {
    workersSettings.get(remoteTask.group).map { s ⇒
      val requestHeaders = RequestHeaders(Headers(remoteTask.taskHeaders.toMap.toSeq: _*))
      val requestMeta = s.lookupRequestMeta(requestHeaders)
      val stringReader = new StringReader(remoteTask.taskBody)
      requestMeta(stringReader, requestHeaders.underlying)
    } getOrElse {
      throw new IllegalArgumentException(s"No settings are defined for a group ${remoteTask.group}")
    }
  }

  private def deserializeResponse(r: RemoteTaskResult, e: ExpectingRemoteResult): ResponseBase = {
    val stringReader = new StringReader(r.resultBody)
    val headers = Headers(r.resultHeaders.toMap.toSeq: _*)
    e.requestMeta.responseDeserializer(stringReader, headers)
  }

  private def updateAndStay(data: Option[ShardedClusterData]): State = {
    if (data.isDefined) {
      safeUnstashAll()
      stay using data.get
    }
    else {
      stay
    }
  }
}

object ShardProcessor {
  def props(
             clusterTransport: ActorRef,
             workersSettings: Map[String, WorkerGroupSettings],
             tracker: MetricsTracker,
             syncTimeout: FiniteDuration = 1000.millisecond // todo: move to config!
           ) = Props(new ShardProcessor(clusterTransport, workersSettings, tracker, syncTimeout))
}
