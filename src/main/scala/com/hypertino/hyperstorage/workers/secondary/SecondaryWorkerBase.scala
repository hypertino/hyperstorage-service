/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.workers.secondary

import akka.actor.ActorRef
import akka.util.Timeout
import com.datastax.driver.core.utils.UUIDs
import com.hypertino.hyperbus.model.{ErrorBody, HyperbusError, InternalServerError, MessagingContext, RequestBase}
import com.hypertino.hyperstorage.api.HyperStorageIndexSortItem
import com.hypertino.hyperstorage.db.{Db, IndexDef, PendingIndex}
import com.hypertino.hyperstorage.indexing.{IndexLogic, IndexManager}
import com.hypertino.hyperstorage.{ContentLogic, ResourcePath, TransactionLogic}
import com.hypertino.hyperstorage.sharding.{LocalTask, WorkerTaskResult}
import akka.pattern.ask
import com.hypertino.binders.value.Null
import com.hypertino.hyperstorage.internal.api.IndexDefTransaction
import com.hypertino.hyperstorage.utils.ErrorCode
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.concurrent.duration._

trait SecondaryWorkerBase extends StrictLogging {
  def db: Db
  def indexManager: ActorRef
  implicit def executionContext: ExecutionContext

  protected def validateCollectionUri(uri: String) = {
    val ResourcePath(documentUri, itemId) = ContentLogic.splitPath(uri)
    if (!ContentLogic.isCollectionUri(uri) || !itemId.isEmpty || documentUri != uri) {
      throw new IllegalArgumentException(s"'$uri' isn't a collection URI.")
    }
  }

  protected def insertIndexDef(documentUri: String, indexId: String, sortBy: Seq[HyperStorageIndexSortItem], filter: Option[String], materialize: Boolean): Task[IndexDef] = {
    val tableName = IndexLogic.tableName(sortBy)
    val indexDef = IndexDef(documentUri, indexId, IndexDef.STATUS_INDEXING,
      IndexLogic.serializeSortByFields(sortBy), filter, tableName, defTransactionId = UUIDs.timeBased(),
      materialize
    )
    val pendingIndex = PendingIndex(TransactionLogic.partitionFromUri(documentUri), documentUri, indexId, None, indexDef.defTransactionId)
    // validate: id, sort, expression, etc
    Task.fromFuture(db.insertPendingIndex(pendingIndex) flatMap { _ ⇒
      db.insertIndexDef(indexDef) flatMap { _ ⇒
        implicit val timeout = Timeout(60.seconds)
        indexManager ? IndexManager.IndexCreatedOrDeleted(IndexDefTransaction(
          documentUri,
          indexId,
          pendingIndex.defTransactionId.toString
        )) map { _ ⇒
          indexDef
        }
      }
    })
  }

}
