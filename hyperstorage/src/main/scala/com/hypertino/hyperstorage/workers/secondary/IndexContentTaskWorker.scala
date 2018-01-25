/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.workers.secondary

import java.util.UUID

import akka.actor.ActorRef
import com.hypertino.binders.value.Null
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{Ok, ResponseBase}
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.ItemIndexer
import com.hypertino.hyperstorage.internal.api.{IndexContentTaskResult, IndexContentTasksPost}
import com.hypertino.hyperstorage.sharding.{LocalTask, WorkerTaskResult}
import com.hypertino.metrics.MetricsTracker
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicLong

import scala.concurrent.Future
import scala.util.Try

trait IndexContentTaskWorker extends ItemIndexer with SecondaryWorkerBase {
  def hyperbus: Hyperbus
  def db: Db
  def tracker: MetricsTracker
  def indexManager: ActorRef
  implicit def scheduler: Scheduler

  def indexNextBucket(task: LocalTask, request: IndexContentTasksPost): Task[ResponseBase] = {
    import request.body.indexDefTransaction._
    implicit val mcx = request
    Task.fromTry {
      Try {
        validateCollectionUri(task.key)
        ContentLogic.getIdFieldName(documentUri)
      }
    }.flatMap { idFieldName ⇒
      val transactionUuid = UUID.fromString(defTransactionId)
      // todo: cache indexDef?
      db.selectIndexDef(documentUri, indexId) flatMap {
        case Some(indexDef) if indexDef.defTransactionId.toString == defTransactionId ⇒ indexDef.status match {
          case IndexDef.STATUS_INDEXING ⇒
            val bucketSize = 256 // todo: move to config, or make adaptive, or per index

            db.selectContentCollection(documentUri, bucketSize, request.body.lastItemId.map((_, FilterGt))) flatMap { collectionItems ⇒
              db.selectIndexContentStatic(indexDef.tableName, indexDef.documentUri, indexDef.indexId) flatMap { indexContentStaticO ⇒
                val countBefore = indexContentStaticO.flatMap(_.count).getOrElse(0l)
                indexItems(indexDef, idFieldName, collectionItems.toList, countBefore, None).flatMap { case (insertedItemIds, countAfter) ⇒
                  if (insertedItemIds.isEmpty) {
                    // indexing is finished
                    // todo: fix code format
                    db.updateIndexDefStatus(documentUri, indexId, IndexDef.STATUS_NORMAL, transactionUuid) flatMap { _ ⇒
                      db.deletePendingIndex(TransactionLogic.partitionFromUri(documentUri), documentUri, indexId, transactionUuid) map { _ ⇒
                        Ok(IndexContentTaskResult(None, request.body.processId, isFailed = false, None))
                      }
                    }
                  } else {
                    val last = insertedItemIds.get
                    db.updatePendingIndexLastItemId(TransactionLogic.partitionFromUri(documentUri), documentUri, indexId, transactionUuid, last) map { _ ⇒
                      Ok(IndexContentTaskResult(Some(last), request.body.processId, isFailed = false, None))
                    }
                  }
                }
              }
            }


          case IndexDef.STATUS_NORMAL ⇒
            Task.now(Ok(IndexContentTaskResult(None, request.body.processId, isFailed = false, None)))

          case IndexDef.STATUS_DELETING ⇒
            deleteIndexDefAndData(indexDef) map { _ ⇒
              Ok(IndexContentTaskResult(None, request.body.processId, isFailed = false, None))
            }
        }

        case _ ⇒
          // todo: test this
          Task.now(Ok(IndexContentTaskResult(None, request.body.processId, isFailed = true, Some(s"Can't find index for $defTransactionId"))))
      }
    }
  }

  private def indexItems(indexDef: IndexDef, idFieldName: String, items: List[Content], countBefore: Long, previousId: Option[String]): Task[(Option[String], Long)] = {
    if (items.isEmpty) {
      Task.now(previousId, countBefore)
    }
    else{
      indexItem(indexDef, items.head, idFieldName, Some(countBefore + 1)).flatMap { r ⇒
        val nextCount = if (r._2) {
            countBefore + 1
          }
        else {
          countBefore
        }
        indexItems(indexDef, idFieldName, items.tail, nextCount, Some(r._1))
      }
    }
  }

  def deleteIndexDefAndData(indexDef: IndexDef): Task[Any] = {
    db.deleteIndex(indexDef.tableName, indexDef.documentUri, indexDef.indexId) flatMap { _ ⇒
      db.deleteIndexDef(indexDef.documentUri, indexDef.indexId) flatMap { _ ⇒
        db.deletePendingIndex(
          TransactionLogic.partitionFromUri(indexDef.documentUri), indexDef.documentUri, indexDef.indexId, indexDef.defTransactionId
        )
      }
    }
  }
}
