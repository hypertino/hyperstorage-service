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
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.util.Timeout
import com.datastax.driver.core.utils.UUIDs
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model._
import com.hypertino.hyperbus.serialization.{MessageReader, RequestDeserializer}
import com.hypertino.hyperbus.util.IdGenerator
import com.hypertino.hyperstorage.TransactionLogic
import com.hypertino.hyperstorage.api.{IndexPost, _}
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.{IndexDefTransaction, IndexLogic, IndexManager}
import com.hypertino.hyperstorage.sharding.ShardTaskComplete
import com.hypertino.metrics.MetricsTracker

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal


@SerialVersionUID(1L) case class IndexDefTask(ttl: Long, documentUri: String, content: String, expectsResult: Boolean) extends SecondaryTaskTrait {
  def key = documentUri
}

@SerialVersionUID(1L) case class IndexDefTaskTaskResult(content: String)

trait IndexDefTaskWorker extends SecondaryWorkerBase {
  def hyperbus: Hyperbus

  def db: Db

  def tracker: MetricsTracker

  def log: LoggingAdapter

  def indexManager: ActorRef

  implicit def executionContext: ExecutionContext

  def executeIndexDefTask(task: IndexDefTask): Future[ShardTaskComplete] = {
    {
      try {
        validateCollectionUri(task.key)
        val deserializer: RequestDeserializer[RequestBase] = (reader: Reader, headers: Headers) ⇒ {
          val requestHeaders = RequestHeaders(headers)
          requestHeaders.method match {
            case Method.POST ⇒ IndexPost(reader, headers)
            case Method.DELETE ⇒ IndexDelete(reader, headers)
          }
        }

        MessageReader.fromString[RequestBase](task.content, deserializer) match {
          case post: IndexPost ⇒ startCreatingNewIndex(task, post)
          case delete: IndexDelete ⇒ startRemovingIndex(task, delete)
        }
      } catch {
        case NonFatal(e) ⇒
          Future.failed(e)
      }
    } recover withHyperbusException(task)
  }

  private def startCreatingNewIndex(task: SecondaryTaskTrait, post: IndexPost): Future[ShardTaskComplete] = {
    implicit val mcx = post
    val indexId = post.body.indexId.getOrElse(
      IdGenerator.create()
    )

    post.body.filter.foreach(IndexLogic.validateFilterExpression(_).get)

    db.selectIndexDefs(post.path) flatMap { indexDefs ⇒
      indexDefs.foreach { existingIndex ⇒
        if (existingIndex.indexId == indexId) {
          throw Conflict(ErrorBody("already-exists", Some(s"Index '$indexId' already exists")))
        }
      }
      insertIndexDef(post.path,
        indexId,
        post.body.sortBy,
        post.body.filter,
        post.body.materialize.getOrElse(true)) map { _ ⇒ // IndexManager.IndexCommandAccepted

        // todo: !!!! LOCATION header
        Created(HyperStorageIndexCreated(indexId, path = post.path))
      }
    } map { result ⇒
      ShardTaskComplete(task, result)
    }
  }

  private def startRemovingIndex(task: SecondaryTaskTrait, delete: IndexDelete): Future[ShardTaskComplete] = {
    implicit val mcx = delete

    db.selectIndexDef(delete.path, delete.indexId) flatMap {
      case Some(indexDef) if indexDef.status != IndexDef.STATUS_DELETING ⇒
        val pendingIndex = PendingIndex(TransactionLogic.partitionFromUri(delete.path), delete.path, delete.indexId, None, UUIDs.timeBased())
        db.insertPendingIndex(pendingIndex) flatMap { _ ⇒
          db.updateIndexDefStatus(pendingIndex.documentUri, pendingIndex.indexId, IndexDef.STATUS_DELETING, pendingIndex.defTransactionId) flatMap { _ ⇒
            implicit val timeout = Timeout(60.seconds)
            indexManager ? IndexManager.IndexCreatedOrDeleted(IndexDefTransaction(
              delete.path,
              delete.indexId,
              pendingIndex.defTransactionId
            )) map { _ ⇒ // IndexManager.IndexCommandAccepted
              NoContent(EmptyBody)
            }
          }
        }

      case _ ⇒ Future.successful(NotFound(ErrorBody("index-not-found", Some(s"Index ${delete.indexId} for ${delete.path} is not found"))))
    } map { result ⇒
      ShardTaskComplete(task, result)
    }
  }
}
