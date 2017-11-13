package com.hypertino.hyperstorage.sharding

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, ClusterEvent, Member}
import akka.routing.{ConsistentHash, MurmurHash}
import com.hypertino.hyperbus.model.{MessagingContext, Ok}
import com.hypertino.hyperstorage.internal.api
import com.hypertino.hyperstorage.internal.api._
import com.hypertino.hyperstorage.metrics.Metrics
import com.hypertino.hyperstorage.utils.AkkaNaming
import com.hypertino.metrics.MetricsTracker

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal

@SerialVersionUID(1L) trait ShardTask {
  def key: String

  def group: String

  def isExpired: Boolean

  def expectsResult: Boolean
}

@SerialVersionUID(1L) case class NoSuchGroupWorkerException(groupName: String) extends RuntimeException(s"No such worker group: $groupName")

case class ShardNode(actorRef: ActorSelection,
                     status: String,
                     confirmedStatus: String,
                     id: String)

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

// todo: is this used?
case class SubscribeToShardStatus(subscriber: ActorRef)

// todo: is this used?
case class UpdateShardStatus(self: ActorRef, status: String, stateData: ShardedClusterData)

private[sharding] case object ShardSyncTimer

case object ShutdownProcessor

case class ShardTaskComplete(task: ShardTask, result: Any)

class ShardProcessor(workersSettings: Map[String, (Props, Int, String)],
                     roleName: String,
                     tracker: MetricsTracker,
                     syncTimeout: FiniteDuration = 1000.millisecond)
  extends FSMEx[String, ShardedClusterData] with Stash {

  private val cluster = Cluster(context.system)
  if (!cluster.selfRoles.contains(roleName)) {
    log.error(s"Cluster doesn't contains '$roleName' role. Please configure.")
  }
  private val selfId = cluster.selfAddress.toString
  val activeWorkers = workersSettings.map { case (groupName, _) ⇒
    groupName → mutable.ArrayBuffer[(ShardTask, ActorRef, ActorRef)]()
  }
  private val shardStatusSubscribers = mutable.MutableList[ActorRef]()
  cluster.subscribe(self, initialStateMode = ClusterEvent.InitialStateAsEvents, classOf[MemberEvent])

  // trackers
  val trackStashMeter = tracker.meter(Metrics.SHARD_PROCESSOR_STASH_METER)
  val trackTaskMeter = tracker.meter(Metrics.SHARD_PROCESSOR_TASK_METER)
  val trackForwardMeter = tracker.meter(Metrics.SHARD_PROCESSOR_FORWARD_METER)

  startWith(NodeStatus.ACTIVATING, ShardedClusterData(Map.empty, selfId, NodeStatus.ACTIVATING))

  when(NodeStatus.ACTIVATING) {
    case Event(MemberUp(member), data) ⇒
      introduceSelfTo(member, data) andUpdate

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
      sender() ! Ok(api.Node(this.selfId, data.selfStatus, data.clusterHash))
      stay()

    case Event(sync: NodesPost, data) ⇒
      incomingSync(sync, data) andUpdate

    case Event(syncReply: Ok[NodeUpdated], data) ⇒
      processReply(syncReply, data) andUpdate

    case Event(MemberUp(member), data) ⇒
      addNewMember(member, data) andUpdate

    case Event(MemberRemoved(member, previousState), data) ⇒
      removeMember(member, data) andUpdate

    case Event(MemberExited(member), data) ⇒
      removeMember(member, data) andUpdate

    case Event(ShardTaskComplete(task, result), data) ⇒
      workerIsReadyForNextTask(task, result)
      stay()

    case Event(task: ShardTask, data) ⇒
      forwardTask(task, data)
      stay()

    case Event(SubscribeToShardStatus(actorRef), data) ⇒
      shardStatusSubscribers += actorRef
      actorRef ! UpdateShardStatus(self, stateName, data)
      stay()

    case Event(e, s) =>
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  onTransition {
    case a -> b ⇒
      if (a != b) {
        log.info(s"Changing state from $a to $b")
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

  def introduceSelfTo(member: Member, data: ShardedClusterData): Option[ShardedClusterData] = {
    if (member.hasRole(roleName) && member.address != cluster.selfAddress) {
      val actor = context.actorSelection(RootActorPath(member.address) / "user" / roleName)

      // todo: zmq
      val newData: ShardedClusterData = data + (member.address.toString → ShardNode(
        actor, NodeStatus.PASSIVE, NodeStatus.PASSIVE, member.address.toString
      ))
      val sync = NodesPost(Node(selfId, NodeStatus.ACTIVATING, newData.clusterHash))(MessagingContext.empty)
      actor ! sync
      setSyncTimer()
      log.info(s"New member of shard cluster $roleName: $member. $sync was sent to $actor")

      Some(newData)
    }
    else if (member.hasRole(roleName) && member.address == cluster.selfAddress) {
      log.info(s"Self is up: $member on role $roleName")
      setSyncTimer()
      None
    }
    else {
      log.debug(s"Non shard member in $roleName is ignored: $member")
      None
    }
  }

  def setSyncTimer(): Unit = {
    setTimer("syncing", ShardSyncTimer, syncTimeout)
  }

  def processReply(syncReply: Ok[NodeUpdated], data: ShardedClusterData): Option[ShardedClusterData] = {
    if (log.isDebugEnabled) {
      log.debug(s"$syncReply received from $sender")
    }
    if (data.clusterHash != syncReply.body.clusterHash) {
      log.info(s"ClusterHash for $selfId (${data.clusterHash}) is not matched for $syncReply. SyncReply is ignored")
      None
    } else {
      data.nodes.get(syncReply.body.sourceNodeId) map { member ⇒
        data + syncReply.body.sourceNodeId →
          member.copy(status = syncReply.body.sourceStatus, confirmedStatus = syncReply.body.acceptedStatus)
      } orElse {
        log.warning(s"Got $syncReply from unknown member of $roleName. Current members: ${data.nodes}")
        None
      }
    }
  }

  def isActivationAllowed(data: ShardedClusterData): Boolean = {
    if (confirmStatus(data, NodeStatus.ACTIVATING, isFirst = false)) {
      log.info(s"Synced with all members: ${data.nodes}. Activating")
      confirmStatus(data, NodeStatus.ACTIVE, isFirst = true)
      true
    }
    else {
      false
    }
  }

  def confirmStatus(data: ShardedClusterData, status: String, isFirst: Boolean): Boolean = {
    var syncedWithAllMembers = true
    data.nodes.foreach { case (address, member) ⇒
      if (member.confirmedStatus != status) {
        syncedWithAllMembers = false
        val sync = NodesPost(Node(selfId, status, data.clusterHash))(MessagingContext.empty)
        member.actorRef ! sync
        setSyncTimer()
        if (log.isDebugEnabled && !isFirst) {
          log.debug(s"Didn't received reply from: $member. $sync was sent to ${member.actorRef}")
        }
      }
    }
    syncedWithAllMembers
  }

  def incomingSync(sync: NodesPost, data: ShardedClusterData): Option[ShardedClusterData] = {
    if (log.isDebugEnabled) {
      log.debug(s"$sync received from $sender")
    }
    if (data.clusterHash != sync.body.clusterHash) { // todo: zmq, remove ClusterHash
      log.info(s"ClusterHash for $selfId (${data.clusterHash}) is not matched for $sync")
      None
    } else {
      data.nodes.get(sync.body.nodeId) map { member ⇒
        val newData: ShardedClusterData = data + (sync.body.nodeId → member.copy(status = sync.body.status))
        val allowSync = if (sync.body.status == NodeStatus.ACTIVATING) {
          activeWorkers.values.flatten.forall { case (task, workerActor, _) ⇒
            if (newData.taskIsFor(task) == sync.body.nodeId) {
              log.info(s"Ignoring sync request $sync while processing task $task by worker $workerActor")
              false
            } else {
              true
            }
          }
        } else {
          true
        }

        if (allowSync) { // todo: zmq, new name for nodeStatuses? vs clusterNodes
          val syncReply = Ok(NodeUpdated(selfId, stateName, sync.body.status, data.clusterHash))(MessagingContext.empty)
          if (log.isDebugEnabled) {
            log.debug(s"Replying with $syncReply to $sender")
          }
          sender() ! syncReply
        }
        newData
      } orElse {
        log.error(s"Got $sync from unknown member. Current members: ${data.nodes}")
        sender() ! Ok(NodeUpdated(selfId, stateName, NodeStatus.PASSIVE, data.clusterHash))(MessagingContext.empty)
        None
      }
    }
  }

  def addNewMember(member: Member, data: ShardedClusterData): Option[ShardedClusterData] = {
    if (member.hasRole(roleName) && member.address != cluster.selfAddress) {
      val actor = context.actorSelection(RootActorPath(member.address) / "user" / roleName)
      val newData = data + (member.address.toString → ShardNode(
        actor, NodeStatus.PASSIVE, NodeStatus.PASSIVE, member.address.toString
      ))
      if (log.isDebugEnabled) {
        log.info(s"New member in $roleName $member. State is unknown yet")
      }
      Some(newData)
    }
    else {
      None
    }
  }

  def removeMember(member: Member, data: ShardedClusterData): Option[ShardedClusterData] = {
    if (member.hasRole(roleName)) {
      if (log.isDebugEnabled) {
        log.info(s"Member removed from $roleName: $member.")
      }
      Some(data - member.address.toString)
    } else {
      None
    }
  }

  def processTask(task: ShardTask, data: ShardedClusterData): Unit = {
    trackTaskMeter.mark()
    if (log.isDebugEnabled) {
      log.debug(s"Got task to process: $task")
    }
    if (task.isExpired) {
      log.warning(s"Task is expired, dropping: $task")
    } else {
      if (data.taskIsFor(task) == data.selfId) {
        if (data.taskWasFor(task) != data.selfId) {
          if (log.isDebugEnabled) {
            log.debug(s"Stashing task received for deactivating node: ${data.taskWasFor(task)}: $task")
          }
          safeStash(task)
        } else {
          activeWorkers.get(task.group) match {
            case Some(activeGroupWorkers) ⇒
              activeGroupWorkers.find(_._1.key == task.key) map { case (_, activeWorker, _) ⇒
                if (log.isDebugEnabled) {
                  log.debug(s"Stashing task for the 'locked' URL: $task worker: $activeWorker")
                }
                safeStash(task)
                true
              } getOrElse {
                val maxCount = workersSettings(task.group)._2
                if (activeGroupWorkers.size >= maxCount) {
                  if (log.isDebugEnabled) {
                    log.debug(s"Worker limit for group '${task.group}' is reached ($maxCount), stashing task: $task")
                  }
                  safeStash(task)
                } else {
                  val workerProps = workersSettings(task.group)._1
                  val prefix = workersSettings(task.group)._3
                  try {
                    val worker = context.system.actorOf(workerProps, AkkaNaming.next(prefix))
                    if (log.isDebugEnabled) {
                      log.debug(s"Forwarding task from ${sender()} to worker $worker: $task")
                    }
                    worker ! task
                    activeGroupWorkers.append((task, worker, sender()))
                  } catch {
                    case NonFatal(e) ⇒
                      log.error(e, s"Can't create worker from props $workerProps")
                      sender() ! e
                  }
                }
              }
            case None ⇒
              log.error(s"No such worker group: ${task.group}. Task is dismissed: $task")
              sender() ! NoSuchGroupWorkerException(task.group)
          }
        }
      }
      else {
        forwardTask(task, data)
      }
    }
  }

  def holdTask(task: ShardTask, data: ShardedClusterData): Unit = {
    trackTaskMeter.mark()
    if (log.isDebugEnabled) {
      log.debug(s"Got task to process while activating: $task")
    }
    if (task.isExpired) {
      log.warning(s"Task is expired, dropping: $task")
    } else {
      if (data.taskIsFor(task) == data.selfId) {
        if (log.isDebugEnabled) {
          log.debug(s"Stashing task while activating: $task")
        }
        safeStash(task)
      } else {
        forwardTask(task, data)
      }
    }
  }

  def safeStash(task: ShardTask) = try {
    trackStashMeter.mark()
    stash()
  } catch {
    case NonFatal(e) ⇒
      log.error(e, s"Can't stash task: $task. It's lost now")
  }

  def forwardTask(task: ShardTask, data: ShardedClusterData): Unit = {
    trackForwardMeter.mark()
    val address = data.taskIsFor(task)
    data.nodes.get(address) map { rvm ⇒
      if (log.isDebugEnabled) {
        log.debug(s"Task is forwarded to $address: $task")
      }
      rvm.actorRef forward task
      true
    } getOrElse {
      log.error(s"Task actor is not found: $address, dropping: $task")
    }
  }

  def workerIsReadyForNextTask(task: ShardTask, result: Any): Unit = {
    activeWorkers.get(task.group) match {
      case Some(activeGroupWorkers) ⇒
        val idx = activeGroupWorkers.indexWhere(_._2 == sender())
        if (idx >= 0) {
          val (task, worker, client) = activeGroupWorkers(idx)
          if (task.expectsResult) {
            result match {
              case None ⇒ // no result
              case other ⇒ client ! other
            }
          }
          if (log.isDebugEnabled) {
            log.debug(s"Worker $worker is ready for next task. Completed task: $task, result: $result")
          }
          activeGroupWorkers.remove(idx)
          worker ! PoisonPill
          safeUnstashAll()
        } else {
          log.error(s"workerIsReadyForNextTask: unknown worker actor: $sender")
        }
      case None ⇒
        log.error(s"No such worker group: ${task.group}. Task result from $sender is ignored: $result. Task: $task")
    }
  }

  def safeUnstashAll() = try {
    log.debug("Unstashing tasks")
    unstashAll()
  } catch {
    case NonFatal(e) ⇒
      log.error(e, s"Can't unstash tasks. Some are lost now")
  }

  protected[this] implicit class ImplicitExtender(data: Option[ShardedClusterData]) {
    def andUpdate: State = {
      if (data.isDefined) {
        safeUnstashAll()
        stay using data.get
      }
      else {
        stay
      }
    }
  }

}

object ShardProcessor {
  def props(
             workersSettings: Map[String, (Props, Int, String)],
             roleName: String,
             tracker: MetricsTracker,
             syncTimeout: FiniteDuration = 1000.millisecond // todo: move to config!
           ) = Props(classOf[ShardProcessor],
    workersSettings, roleName, tracker, syncTimeout
  )
}
