/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.workers.secondary

import akka.actor.ActorRef
import com.hypertino.binders.value.Null
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.{IndexDefTransaction, IndexLogic, ItemIndexer}
import com.hypertino.hyperstorage.sharding.WorkerTaskResult
import com.hypertino.hyperstorage.utils.FutureUtils
import com.hypertino.metrics.MetricsTracker
import monix.execution.Scheduler
import monix.execution.atomic.AtomicLong

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class IndexContentTask(ttl: Long, indexDefTransaction: IndexDefTransaction, lastItemId: Option[String], processId: Long, expectsResult: Boolean) extends SecondaryTaskTrait {
  def key = indexDefTransaction.documentUri
}

@SerialVersionUID(1L) case class IndexContentTaskResult(lastItemSegment: Option[String], processId: Long)

@SerialVersionUID(1L) case class IndexContentTaskFailed(processId: Long, reason: String) extends RuntimeException(s"Index content task for process $processId is failed with reason $reason")

trait IndexContentTaskWorker extends ItemIndexer with SecondaryWorkerBase {
  def hyperbus: Hyperbus
  def db: Db
  def tracker: MetricsTracker
  def indexManager: ActorRef
  implicit def scheduler: Scheduler

  def indexNextBucket(task: IndexContentTask): Future[WorkerTaskResult] = {
    try {
      validateCollectionUri(task.key)
      val idFieldName = ContentLogic.getIdFieldName(task.indexDefTransaction.documentUri)
      // todo: cache indexDef
      db.selectIndexDef(task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId) flatMap {
        case Some(indexDef) if indexDef.defTransactionId == task.indexDefTransaction.defTransactionId ⇒ indexDef.status match {
          case IndexDef.STATUS_INDEXING ⇒
            val bucketSize = 256 // todo: move to config, or make adaptive, or per index

            db.selectContentCollection(task.indexDefTransaction.documentUri, bucketSize, task.lastItemId.map((_, FilterGt))) flatMap { collectionItems ⇒
              db.selectIndexContentStatic(indexDef.tableName, indexDef.documentUri, indexDef.indexId) flatMap { indexContentStaticO ⇒
                val countBefore = AtomicLong(indexContentStaticO.flatMap(_.count).getOrElse(0l))
                FutureUtils.serial(collectionItems.toSeq) { item ⇒
                  indexItem(indexDef, item, idFieldName, Some(countBefore.get+1)).andThen {
                    case Success((_, true)) ⇒ countBefore.increment()
                    case _ ⇒
                  }
                } flatMap { insertedItemIds ⇒

                  if (insertedItemIds.isEmpty) {
                    // indexing is finished
                    // todo: fix code format
                    db.updateIndexDefStatus(task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId, IndexDef.STATUS_NORMAL, task.indexDefTransaction.defTransactionId) flatMap { _ ⇒
                      db.deletePendingIndex(TransactionLogic.partitionFromUri(task.indexDefTransaction.documentUri), task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId, task.indexDefTransaction.defTransactionId) map { _ ⇒
                        IndexContentTaskResult(None, task.processId)
                      }
                    }
                  } else {
                    val last = insertedItemIds.last._1
                    db.updatePendingIndexLastItemId(TransactionLogic.partitionFromUri(task.indexDefTransaction.documentUri), task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId, task.indexDefTransaction.defTransactionId, last) map { _ ⇒
                      IndexContentTaskResult(Some(last), task.processId)
                    }
                  }
                }
              }
            }


          case IndexDef.STATUS_NORMAL ⇒
            Future.successful(IndexContentTaskResult(None, task.processId))

          case IndexDef.STATUS_DELETING ⇒
            deleteIndexDefAndData(indexDef) map { _ ⇒
              IndexContentTaskResult(None, task.processId)
            }
        }

        case _ ⇒
          Future.failed(IndexContentTaskFailed(task.processId, s"Can't find index for ${task.indexDefTransaction}")) // todo: test this
      } map { r: IndexContentTaskResult ⇒
        WorkerTaskResult(task.key, task.group, r)
      }
    }
    catch {
      case e: Throwable ⇒
        Future.failed(e)
    }
  }

  def deleteIndexDefAndData(indexDef: IndexDef): Future[Unit] = {
    db.deleteIndex(indexDef.tableName, indexDef.documentUri, indexDef.indexId) flatMap { _ ⇒
      db.deleteIndexDef(indexDef.documentUri, indexDef.indexId) flatMap { _ ⇒
        db.deletePendingIndex(
          TransactionLogic.partitionFromUri(indexDef.documentUri), indexDef.documentUri, indexDef.indexId, indexDef.defTransactionId
        )
      }
    }
  }
}
