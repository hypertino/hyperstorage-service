/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.mock.hyperstorage

import com.hypertino.binders.value.{Lst, Null, Obj, Text, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{Conflict, Created, DynamicBody, EmptyBody, ErrorBody, Headers, MessagingContext, NotFound, Ok, PreconditionFailed, RequestBase, RequestHeaders, ResponseBase}
import com.hypertino.hyperbus.subscribe.Subscribable
import com.hypertino.hyperbus.util.IdGenerator
import com.hypertino.hyperstorage.{ContentLogic, ResourcePath}
import com.hypertino.hyperstorage.api._
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt

import scala.collection.concurrent.TrieMap

class HyperStorageMock(protected val hyperbus: Hyperbus, protected implicit val scheduler: Scheduler) extends Subscribable {
  final val hyperStorageContent = TrieMap[String, (Value, Long)]()
  final val failPreconditions = TrieMap[String, (Int, AtomicInt)]()

  final val handlers = hyperbus.subscribe(this)

  def stop(): Unit = {
    handlers.foreach(_.cancel)
  }

  def reset(): Unit = {
    hyperStorageContent.clear()
    failPreconditions.clear()
  }

  def onContentPost(implicit request: ContentPost): Task[ResponseBase] = {
    val resourcePath = ContentLogic.splitPath(request.path)

    // TODO: not thread safe
    hbpc(resourcePath, request.headers).map { rev ⇒
      val newRev = rev + 1

      val idFieldName = ContentLogic.getIdFieldName(resourcePath.documentUri)
      val contentId = IdGenerator.create()
      val target = Obj.from(idFieldName -> contentId)
      val document = request.body.content % target

      val newDocumentUri = s"${resourcePath.documentUri}/$contentId"

      if (ContentLogic.isCollectionUri(resourcePath.documentUri)){
        if (resourcePath.itemId.nonEmpty){
          throw new IllegalArgumentException(s"POST is not allowed on existing item of collection~")
        }

        val newCollectionItem = Obj.from(contentId -> document)
        val newCollection = hyperStorageContent.get(resourcePath.documentUri) map(_._1 % newCollectionItem) getOrElse newCollectionItem

        hyperStorageContent.put(resourcePath.documentUri, (newCollection, newRev))
      } else {
        hyperStorageContent.put(newDocumentUri, (document, newRev))
      }

      Created(HyperStorageTransactionCreated(IdGenerator.create(), newDocumentUri, newRev, target = target))
    }
  }

  def onContentPut(implicit request: ContentPut): Task[ResponseBase] = {
    val resourcePath = ContentLogic.splitPath(request.path)

    // TODO: not thread safe
    hbpc(resourcePath, request.headers).map { rev ⇒
      val newRev = rev + 1

      val document = if (ContentLogic.isCollectionUri(resourcePath.documentUri)){
        val newCollectionItem = Obj.from(resourcePath.itemId -> request.body.content)
        hyperStorageContent.get(resourcePath.documentUri) map(_._1 % newCollectionItem) getOrElse newCollectionItem
      }else {
        request.body.content
      }

      if (hyperStorageContent.put(resourcePath.documentUri, (document, newRev)).isDefined) {
        Ok(HyperStorageTransaction(IdGenerator.create(), request.path, newRev))
      } else {
        Created(HyperStorageTransactionCreated(IdGenerator.create(), request.path, newRev, target = Obj.empty))
      }
    }
  }

  def onContentPatch(implicit request: ContentPatch): Task[ResponseBase] = {
    val resourcePath = ContentLogic.splitPath(request.path)

    // TODO: not thread safe
    hbpc(resourcePath, request.headers).map { rev ⇒
      val newRev = rev + 1

      if (ContentLogic.isCollectionUri(resourcePath.documentUri)) {
        hyperStorageContent.get(resourcePath.documentUri) flatMap { case (collection, _) =>
          collection.toMap.get(resourcePath.itemId) flatMap { collectionItem =>
            val newCollectionItem = collectionItem % ContentLogic.applyPatch(collectionItem, request)

            val newCollection = collection % Obj.from(resourcePath.itemId -> newCollectionItem)

            hyperStorageContent.put(resourcePath.documentUri, (newCollection, newRev))

            Some(Ok(HyperStorageTransaction(IdGenerator.create(),request.path, newRev)))
          }
        } getOrElse NotFound()
      } else {
        hyperStorageContent.get(resourcePath.documentUri) match {
          case Some((document, _)) ⇒
            val newContent = document % ContentLogic.applyPatch(document, request)
            hyperStorageContent.put(request.path, (newContent, newRev))
            Ok(HyperStorageTransaction(IdGenerator.create(),request.path, newRev))

          case None ⇒
            NotFound()
        }
      }
    }
  }

  def onContentDelete(implicit request: ContentDelete): Task[ResponseBase] = {
    val resourcePath = ContentLogic.splitPath(request.path)

    // TODO: not thread safe
    hbpc(resourcePath, request.headers).map { rev ⇒
      val newRev = rev + 1

      if (ContentLogic.isCollectionUri(resourcePath.documentUri)){
        hyperStorageContent.get(resourcePath.documentUri) flatMap { case (collection, _) =>
          collection.toMap.get(resourcePath.itemId) flatMap { _ =>
            val newCollection = collection - resourcePath.itemId

            hyperStorageContent.put(resourcePath.documentUri, (newCollection, newRev))

            Some(Ok(HyperStorageTransaction(IdGenerator.create(),request.path, newRev)))
          }
        } getOrElse NotFound()
      } else {
        if (hyperStorageContent.remove(request.path).isDefined) {
          Ok(HyperStorageTransaction(IdGenerator.create(),request.path, newRev))
        }
        else {
          NotFound()
        }
      }
    }
  }

  def onContentGet(implicit request: ContentGet): Task[ResponseBase] = Task.eval {
    val resourcePath = ContentLogic.splitPath(request.path)

    hyperStorageContent.get(resourcePath.documentUri) match {
      case Some((value, rev)) ⇒
        lazy val headers = {
          Headers(HyperStorageHeader.ETAG → Text("\"" + rev + "\""))
        }

        if (ContentLogic.isCollectionUri(resourcePath.documentUri)) {
          val docs = value.toMap

          if (resourcePath.itemId.nonEmpty) {
            docs.get(resourcePath.itemId).map { item => Ok(DynamicBody(item), headers) } getOrElse NotFound()
          } else {
            // TODO: filter, sort, paging
            Ok(DynamicBody(docs.values.toList), headers)
          }
        } else {
          Ok(DynamicBody(value), headers)
        }
      case None ⇒ NotFound()
    }
  }

  private def hbpc(resourcePath: ResourcePath, requestHeaders: RequestHeaders)(implicit mcx: MessagingContext): Task[Long] = Task.defer {
    val path = resourcePath.documentUri
    val existingRev = hyperStorageContent.get(path).map { case (value, rev) ⇒
      if (ContentLogic.isCollectionUri(path) && resourcePath.itemId.nonEmpty) {
        // check collection item exists
        value.toMap.get(resourcePath.itemId).map(_ => rev) getOrElse 0l
      }else{
        rev
      }
    } getOrElse {
      0l
    }
    val existingTag = "\"" + existingRev + "\""

    {
      requestHeaders.get("if-match").map { etag ⇒
        checkFailPreconditions(path).flatMap { _ ⇒
          if ((existingTag != etag.toString) && (!(etag.toString == "*" && existingRev != 0))) {
            Task.raiseError(PreconditionFailed(ErrorBody("revision")))
          } else {
            Task.now(existingRev)
          }
        }
      } getOrElse {
        Task.now(0l)
      }
    }.flatMap { r ⇒
      requestHeaders.get("if-none-match").map { etag ⇒
        checkFailPreconditions(path).flatMap { _ ⇒
          if ((existingTag == etag.toString) || (etag.toString == "*" && existingRev != 0)) {
            Task.raiseError(PreconditionFailed(ErrorBody("revision")))
          } else {
            Task.now(existingRev)
          }
        }
      } getOrElse {
        Task.now(r)
      }
    }
  }

  private def checkFailPreconditions(path: String)(implicit mcx: MessagingContext): Task[Unit] = {
    failPreconditions.get(path).map { fp ⇒
      if (fp._2.incrementAndGet() <= fp._1) {
        Task.raiseError(PreconditionFailed(ErrorBody("fake")))
      }
      else {
        Task.unit
      }
    }.getOrElse {
      Task.unit
    }
  }

}
