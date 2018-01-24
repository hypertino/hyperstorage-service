/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.indexing

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperstorage.TransactionLogic
import com.hypertino.hyperstorage.db.Db
import com.hypertino.hyperstorage.internal.api.{IndexDefTransaction, NodeStatus}
import com.hypertino.hyperstorage.sharding.{ShardedClusterData, UpdateShardStatus}
import com.hypertino.hyperstorage.utils.AkkaNaming
import com.hypertino.metrics.MetricsTracker
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

//case class IndexDefTransaction(documentUri: String, indexId: String, defTransactionId: UUID) {
//  def partition: Int = TransactionLogic.partitionFromUri(documentUri)
//}

// todo: handle child termination without IndexingComplete
class IndexManager(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, maxIndexWorkers: Int, private implicit val scheduler: Scheduler)
  extends Actor with StrictLogging {
  import IndexManager._

  val indexWorkers = mutable.Map[IndexDefTransaction, ActorRef]()
  val pendingPartitions = mutable.Map[Int, mutable.ListBuffer[IndexDefTransaction]]()
  var rev: Long = 0
  var currentProcessId: Long = 0

  override def receive: Receive = {
    case UpdateShardStatus(_, NodeStatus.ACTIVE, stateData) ⇒
      clusterActivated(stateData, Seq.empty)

    case ShutdownIndexManager ⇒
      context.stop(self)
  }

  def stopping: Receive = {
    case ShutdownIndexManager ⇒
      context.stop(self)
  }

  def running(clusterActor: ActorRef, stateData: ShardedClusterData): Receive = {
    case UpdateShardStatus(_, NodeStatus.ACTIVE, newStateData) ⇒
      if (newStateData != stateData) {
        // restart with new partition list
        clusterActivated(newStateData, TransactionLogic.getPartitions(stateData))
      }

    case UpdateShardStatus(_, NodeStatus.DEACTIVATING, _) ⇒
      indexWorkers.values.foreach(context.stop)
      context.become(stopping)

    case IndexingComplete(key) ⇒
      indexWorkers -= key
      processPendingIndexes(clusterActor)

    case ProcessNextPartitions(processId) if processId == currentProcessId ⇒
      currentProcessId = currentProcessId + 1
      processPendingIndexes(clusterActor)

    case ProcessPartitionPendingIndexes(partition, msgRev, indexes) if rev == msgRev ⇒
      if (indexes.isEmpty) {
        pendingPartitions -= partition
        processPendingIndexes(clusterActor)
      }
      else {
        val updated = indexes.foldLeft(false) { (updated, index) ⇒
          addPendingIndex(index) || updated
        }
        if (updated) {
          processPendingIndexes(clusterActor)
        }
      }

    case PartitionPendingFailed(msgRev) if rev == msgRev ⇒
      processPendingIndexes(clusterActor)

    case IndexCreatedOrDeleted(key) ⇒
      val partitionSet = TransactionLogic.getPartitions(stateData).toSet
      if (partitionSet.contains(TransactionLogic.partitionFromUri(key.documentUri))) {
        if (addPendingIndex(key)) {
          processPendingIndexes(clusterActor)
        }
      }
      else {
        logger.info(s"Received $key update but partition is handled by other node, ignored")
      }
      sender() ! IndexCommandAccepted
  }

  def addPendingIndex(key: IndexDefTransaction): Boolean = {
    val alreadyPending = pendingPartitions.getOrElseUpdate(TransactionLogic.partitionFromUri(key.documentUri), mutable.ListBuffer.empty)
    if (!alreadyPending.contains(key) && !indexWorkers.contains(key) && alreadyPending.size < maxIndexWorkers) {
      alreadyPending += key
      true
    }
    else {
      false
    }
  }

  def clusterActivated(stateData: ShardedClusterData,
                       previousPartitions: Seq[Int]): Unit = {
    rev = rev + 1
    logger.info(s"Cluster is active $getClass is running. Current data: $stateData. rev=$rev")

    val newPartitions = TransactionLogic.getPartitions(stateData)
    val newPartitionSet = newPartitions.toSet
    val previousPartitionSet = previousPartitions.toSet
    val detachedPartitions = previousPartitionSet diff newPartitionSet

    // stop workers for detached partitions
    val toRemove = indexWorkers.flatMap {
      case (v: IndexDefTransaction, actorRef) if detachedPartitions.contains(TransactionLogic.partitionFromUri(v.documentUri)) ⇒
        context.stop(actorRef)
        Some(v)
      case _ ⇒
        None
    }
    // remove fromIndexWorkers before actor is stopped
    toRemove.foreach(indexWorkers -= _)

    val attachedPartitions = newPartitionSet diff previousPartitionSet
    pendingPartitions ++= attachedPartitions.map(_ → mutable.ListBuffer.empty[IndexDefTransaction])

    context.become(running(sender(), stateData))
    processPendingIndexes(sender())
  }

  def processPendingIndexes(clusterActor: ActorRef): Unit = {
    val availableWorkers = maxIndexWorkers - indexWorkers.size
    if (availableWorkers > 0 && pendingPartitions.nonEmpty) {
      createWorkerActors(clusterActor, availableWorkers)
      fetchPendingIndexes(availableWorkers)
    }
  }

  def createWorkerActors(clusterActor: ActorRef, availableWorkers: Int): Unit = {
    val nextPending = nextPendingPartitions.flatMap(_._2)
    val nextToWork = nextPending.take(availableWorkers)
    nextToWork.foreach { key ⇒
      // createWorkingActor here
      val actorRef = context.actorOf(PendingIndexWorker.props(
        clusterActor, key, hyperbus, db, tracker, scheduler
      ), AkkaNaming.next("idxw-"))
      indexWorkers += key → actorRef
      pendingPartitions(TransactionLogic.partitionFromUri(key.documentUri)) -= key
    }
  }

  def fetchPendingIndexes(availableWorkers: Int): Unit = {
    Task.sequence(nextPendingPartitions.flatMap {
      case (k, v) if v.isEmpty ⇒ Some(k)
      case _ ⇒ None
    }.take(availableWorkers).map { nextPartitionToFetch ⇒
      // async fetch and send as a message next portion of indexes along with `rev`
      IndexManagerImpl.fetchPendingIndexesFromDb(self, nextPartitionToFetch, rev, maxIndexWorkers, db)
    }).runAsync
  }

  def nextPendingPartitions: Vector[(Int, Seq[IndexDefTransaction])] = {
    import scala.collection.breakOut
    // move consequently (but not strictly) over pending partitions
    // because currentProcessId is always incremented
    val v = pendingPartitions.map(identity)(breakOut).toVector
    val startFrom = currentProcessId % v.size
    val vn = if (startFrom == 0) {
      v
    } else {
      val vnp = v.splitAt(startFrom.toInt)
      vnp._2 ++ vnp._1
    }
    vn
  }
}

object IndexManager {
  case object ShutdownIndexManager
  case class ProcessNextPartitions(processId: Long)
  case class ProcessPartitionPendingIndexes(partition: Int, rev: Long, indexes: Seq[IndexDefTransaction])
  case class PartitionPendingFailed(rev: Long)
  case class IndexCreatedOrDeleted(key: IndexDefTransaction)
  case class IndexingComplete(key: IndexDefTransaction)
  case object IndexCommandAccepted

  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, maxIndexWorkers: Int, scheduler: Scheduler) = Props(
    new IndexManager(hyperbus, db, tracker, maxIndexWorkers, scheduler)
  )
}

private[indexing] object IndexManagerImpl extends StrictLogging {
  import IndexManager._
  def fetchPendingIndexesFromDb(notifyActor: ActorRef, partition: Int, rev: Long, maxIndexWorkers: Int, db: Db)
                               (implicit scheduler: Scheduler): Task[Any] = {
    db.selectPendingIndexes(partition, maxIndexWorkers) map { indexesIterator ⇒
      val pendingIndexes = indexesIterator.toList
      ProcessPartitionPendingIndexes(partition, rev,
        pendingIndexes.map(ii ⇒ IndexDefTransaction(ii.documentUri, ii.indexId, ii.defTransactionId.toString))
      )
    } onErrorRecover {
      case e: Throwable ⇒
        logger.error(s"Can't fetch pending indexes", e)
        PartitionPendingFailed(rev)
    } map { msg =>
      //logger.trace(s"Sending $msg")
      notifyActor ! msg
    }
  }
}

