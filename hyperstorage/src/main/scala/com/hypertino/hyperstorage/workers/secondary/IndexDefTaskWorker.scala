/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.workers.secondary

import java.io.Reader

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.datastax.driver.core.utils.UUIDs
import com.hypertino.binders.value.Null
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model._
import com.hypertino.hyperbus.serialization.{MessageReader, RequestDeserializer}
import com.hypertino.hyperbus.util.IdGenerator
import com.hypertino.hyperstorage.TransactionLogic
import com.hypertino.hyperstorage.api.{IndexPost, _}
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.{IndexLogic, IndexManager}
import com.hypertino.hyperstorage.internal.api.IndexDefTransaction
import com.hypertino.hyperstorage.sharding.{LocalTask, WorkerTaskResult}
import com.hypertino.hyperstorage.utils.ErrorCode
import com.hypertino.metrics.MetricsTracker
import monix.eval.Task

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal


//@SerialVersionUID(1L) case class IndexDefTask(ttl: Long, documentUri: String, content: String, expectsResult: Boolean) extends SecondaryTaskTrait {
//  def key = documentUri
//}
//
//@SerialVersionUID(1L) case class IndexDefTaskTaskResult(content: String)

trait IndexDefTaskWorker extends SecondaryWorkerBase {
  def hyperbus: Hyperbus

  def db: Db

  def tracker: MetricsTracker

  def indexManager: ActorRef

  implicit def executionContext: ExecutionContext

  protected def createNewIndex(task: LocalTask, post: IndexPost): Task[ResponseBase] = {
    implicit val mcx = post
    val indexId = post.body.indexId.getOrElse(
      IdGenerator.create()
    )

    post.body.filter.foreach(IndexLogic.validateFilterExpression(_).get)

    db.selectIndexDefs(post.path).flatMap { indexDefs ⇒
      indexDefs.foreach { existingIndex ⇒
        if (existingIndex.indexId == indexId) {
          throw Conflict(ErrorBody(ErrorCode.ALREADY_EXISTS, Some(s"Index '$indexId' already exists")))
        }
      }
      insertIndexDef(post.path,
        indexId,
        post.body.sortBy,
        post.body.filter,
        post.body.materialize.getOrElse(true)).map { _ ⇒ // IndexManager.IndexCommandAccepted

        // todo: !!!! LOCATION header
        Created(HyperStorageIndexCreated(indexId, path = post.path))
      }
    }
  }

  protected def removeIndex(task: LocalTask, delete: IndexDelete): Task[ResponseBase] = {
    implicit val mcx = delete

    db.selectIndexDef(delete.path, delete.indexId) flatMap {
      case Some(indexDef) if indexDef.status != IndexDef.STATUS_DELETING ⇒
        val pendingIndex = PendingIndex(TransactionLogic.partitionFromUri(delete.path), delete.path, delete.indexId, None, UUIDs.timeBased())
        db.insertPendingIndex(pendingIndex) flatMap { _ ⇒
          db.updateIndexDefStatus(pendingIndex.documentUri, pendingIndex.indexId, IndexDef.STATUS_DELETING, pendingIndex.defTransactionId) flatMap { _ ⇒
            implicit val timeout = Timeout(60.seconds)
            Task.fromFuture(indexManager ? IndexManager.IndexCreatedOrDeleted(IndexDefTransaction(
              delete.path,
              delete.indexId,
              pendingIndex.defTransactionId.toString
            ))) map { _ ⇒ // IndexManager.IndexCommandAccepted
              NoContent(EmptyBody)
            }
          }
        }

      case _ ⇒ Task.raiseError(NotFound(ErrorBody(ErrorCode.INDEX_NOT_FOUND, Some(s"Index ${delete.indexId} for ${delete.path} is not found"))))
    }
  }
}
