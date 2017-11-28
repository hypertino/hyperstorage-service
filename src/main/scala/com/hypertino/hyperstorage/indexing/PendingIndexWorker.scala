/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.indexing

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.hypertino.binders.value.Null
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{MessagingContext, Ok}
import com.hypertino.hyperstorage.TransactionLogic
import com.hypertino.hyperstorage.db.{Db, IndexDef, PendingIndex}
import com.hypertino.hyperstorage.internal.api._
import com.hypertino.hyperstorage.sharding.LocalTask
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.metrics.MetricsTracker
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}

case object StartPendingIndexWorker

case object CompletePendingIndex

case class BeginIndexing(indexDef: IndexDef, lastItemId: Option[String])

case class WaitForIndexDef(pendingIndex: PendingIndex)

case class IndexNextBatchTimeout(processId: Long)

// todo: add indexing progress log
class PendingIndexWorker(shardProcessor: ActorRef, indexKey: IndexDefTransaction, hyperbus: Hyperbus, db: Db, tracker: MetricsTracker)
  extends Actor with StrictLogging {

  override def preStart(): Unit = {
    super.preStart()
    self ! StartPendingIndexWorker
  }

  override def receive = starOrStop orElse {
    case WaitForIndexDef ⇒
      import context._
      become(waitingForIndexDef)
      IndexWorkerImpl.selectPendingIndex(context.self, indexKey, db)
  }

  def starOrStop: Receive = {
    case StartPendingIndexWorker ⇒
      import context._
      IndexWorkerImpl.selectPendingIndex(context.self, indexKey, db)

    case CompletePendingIndex ⇒
      context.parent ! IndexManager.IndexingComplete(indexKey)
      context.stop(self)

    case BeginIndexing(indexDef, lastItemId) ⇒
      indexNextBatch(0, indexDef, lastItemId)
  }

  def waitingForIndexDef: Receive = starOrStop orElse {
    case WaitForIndexDef(pendingIndex) ⇒
      import context._
      IndexWorkerImpl.deletePendingIndex(context.self, pendingIndex, db)
  }

  def indexing(processId: Long, indexDef: IndexDef, lastItemId: Option[String]): Receive = {
    case IndexNextBatchTimeout(p) if p == processId ⇒
      indexNextBatch(processId + 1, indexDef, lastItemId)

    case Ok(IndexContentTaskResult(Some(latestItemId), p, false, _), _) if p == processId ⇒
      indexNextBatch(processId + 1, indexDef, Some(latestItemId))

    case Ok(IndexContentTaskResult(None, p, false, _), _) if p == processId ⇒
      logger.info(s"Indexing of: $indexKey is complete")
      context.parent ! IndexManager.IndexingComplete(indexKey)
      context.stop(self)

    case Ok(IndexContentTaskResult(_, p, true, failReason), _) if p == processId ⇒
      logger.error(s"Restarting index worker $self. Failed because of: $failReason")
      import context._
      become(waitingForIndexDef)
      IndexWorkerImpl.selectPendingIndex(context.self, indexKey, db)
  }

  def indexNextBatch(processId: Long, indexDef: IndexDef, lastItemId: Option[String]): Unit = {
    import context.dispatcher
    context.become(indexing(processId, indexDef, lastItemId))
    implicit val mcx = MessagingContext.empty
    val indexTask = LocalTask(
      key = indexDef.documentUri,
      group = HyperstorageWorkerSettings.SECONDARY,
      ttl = System.currentTimeMillis() + IndexWorkerImpl.RETRY_PERIOD.toMillis,
      expectsResult = true, // todo: do we need to expecting result here?
      IndexContentTasksPost(
        IndexContentTask(
          IndexDefTransaction(indexDef.documentUri, indexDef.indexId, indexDef.defTransactionId.toString),
          lastItemId, processId
        )),
      extra = Null
    )

    shardProcessor ! indexTask
    context.system.scheduler.scheduleOnce(IndexWorkerImpl.RETRY_PERIOD * 2, self, IndexNextBatchTimeout(processId))
  }
}

object PendingIndexWorker {
  def props(cluster: ActorRef, indexKey: IndexDefTransaction, hyperbus: Hyperbus, db: Db, tracker: MetricsTracker) = Props(
    new PendingIndexWorker(cluster: ActorRef, indexKey, hyperbus, db, tracker)
  )
}

private[indexing] object IndexWorkerImpl extends StrictLogging {
  import scala.concurrent.duration._

  val RETRY_PERIOD = 60.seconds // todo: move to config

  def selectPendingIndex(notifyActor: ActorRef, indexKey: IndexDefTransaction, db: Db)
                        (implicit ec: ExecutionContext, actorSystem: ActorSystem) = {
    db.selectPendingIndex(TransactionLogic.partitionFromUri(indexKey.documentUri), indexKey.documentUri, indexKey.indexId, UUID.fromString(indexKey.defTransactionId)) flatMap {
      case Some(pendingIndex) ⇒
        db.selectIndexDef(indexKey.documentUri, indexKey.indexId) map {
          case Some(indexDef) if indexDef.defTransactionId == pendingIndex.defTransactionId ⇒
            logger.info(s"Starting indexing of: $indexDef")
            notifyActor ! BeginIndexing(indexDef, pendingIndex.lastItemId)
          case _ ⇒
            actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, WaitForIndexDef(pendingIndex))
        }

      case None ⇒
        logger.info(s"Can't find pending index for $indexKey, stopping actor")
        notifyActor ! CompletePendingIndex
        Future.successful()
    } recover {
      case e: Throwable ⇒
        logger.error(s"Can't fetch pending index for $indexKey", e)
        actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, StartPendingIndexWorker)
    }
  }

  def deletePendingIndex(notifyActor: ActorRef, pendingIndex: PendingIndex, db: Db)
                        (implicit ec: ExecutionContext, actorSystem: ActorSystem) = {
    db.deletePendingIndex(pendingIndex.partition, pendingIndex.documentUri, pendingIndex.indexId, pendingIndex.defTransactionId) map { _ ⇒

      logger.warn(s"Pending index deleted: $pendingIndex (no corresponding index definition was found)")
      notifyActor ! CompletePendingIndex

    } recover {
      case e: Throwable ⇒
        logger.error(s"Can't delete pending index $pendingIndex", e)
        actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, StartPendingIndexWorker)
    }
  }
}
