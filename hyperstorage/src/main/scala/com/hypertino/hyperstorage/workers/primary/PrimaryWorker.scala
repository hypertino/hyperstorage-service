/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.workers.primary

import java.util.Date

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.pipe
import com.datastax.driver.core.utils.UUIDs
import com.google.common.base.Utf8
import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.serialization.SerializationOptions
import com.hypertino.hyperbus.util.{IdGenerator, SeqGenerator}
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.IndexLogic
import com.hypertino.hyperstorage.internal.api.{BackgroundContentTask, BackgroundContentTasksPost}
import com.hypertino.hyperstorage.metrics.Metrics
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage.utils.{ErrorCode, TrackerUtils}
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.metrics.MetricsTracker
import com.hypertino.parser.ast.Identifier
import com.hypertino.parser.eval.Context
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

trait PrimaryWorkerRequest extends Request[DynamicBody] {
  def path: String
}

trait HyperStorageTransactionBase extends Body {
  def transactionId: String
}

case class PrimaryWorkerResults(results: Seq[Any])

// todo: Protect from direct view items updates!!!
class PrimaryWorker(hyperbus: Hyperbus,
                    db: Db,
                    tracker: MetricsTracker,
                    backgroundTaskTimeout: FiniteDuration,
                    maxIncompleteTransaction: Int,
                    maxBatchSizeInBytes: Long,
                    scheduler: monix.execution.Scheduler) extends Actor with StrictLogging {

  import ContentLogic._
  import context._

  def receive = {
    case singleTask: LocalTask ⇒
      run(singleTask, executeSingleTask(singleTask), e => {
        PrimaryWorkerResults(Seq(
          WorkerTaskResult(singleTask, hyperbusException(e, singleTask)(MessagingContext.empty))
        ))
      })

    case batchTask: BatchTask ⇒
      run(batchTask, executeBatch(batchTask), e => {
        val bgTask = LocalTask(
          key = batchTask.key,
          group = HyperstorageWorkerSettings.SECONDARY,
          ttl = System.currentTimeMillis() + backgroundTaskTimeout.toMillis,
          expectsResult = false,
          BackgroundContentTasksPost(BackgroundContentTask(batchTask.key))(MessagingContext.empty),
          extra = Null
        )
        PrimaryWorkerResults(Seq(bgTask,
          WorkerBatchTaskResult(key=batchTask.key,group=batchTask.group,
            batchTask.batch.map { t =>
              (t.id, if (t.inner.expectsResult) Some(hyperbusException(e, t.inner)(t.inner.request)) else None, Null)
            }
        )))
      })
  }

  private def run[T <: ShardTask](task: T, code: Task[Any], recover: (Throwable) => Any): Unit = {
    become(running(sender()))
    import TrackerUtils._
    tracker.timeOfTask(Metrics.PRIMARY_PROCESS_TIME) {
      code.onErrorRecover {
        case e: Throwable =>
          // 4xx responses shouldn't be logged as errors
          if (!e.isInstanceOf[HyperbusClientError[_]]) {
            logger.error(s"Can't complete task: $task", e)
          }
          recover(e)
      }
    }.runAsync(scheduler) pipeTo self
  }

  private def running(owner: ActorRef): Receive = {
    case r: PrimaryWorkerResults =>
      r.results.foreach(owner ! _)
      unbecome()
  }

  private def executeSingleTask(task: LocalTask): Task[PrimaryWorkerResults] = {
    prepareToTransform(task).flatMap { case (documentUri, itemId, idField, request) =>
      selectExisting(documentUri, itemId).flatMap { case (existingContent, existingContentStatic, indexDefs) ⇒
        validateBeforeTransform(documentUri, task, request, existingContentStatic, existingContent) flatMap { _ =>
          val (newTransaction, newContent) = transform(documentUri, itemId, request, existingContent, existingContentStatic, indexDefs)
          saveToDb(existingContentStatic, newTransaction, newContent).map { _ =>
            implicit val mcx = request
            val bgTask = LocalTask(
              key = documentUri,
              group = HyperstorageWorkerSettings.SECONDARY,
              ttl = System.currentTimeMillis() + backgroundTaskTimeout.toMillis,
              expectsResult = false,
              BackgroundContentTasksPost(BackgroundContentTask(documentUri)),
              extra = Null
            )
            val workerResult = WorkerTaskResult(task, transactionToResponse(documentUri, itemId, newTransaction, idField, request, existingContent))
            logger.debug(s"task $task is completed")
            PrimaryWorkerResults(Seq(bgTask, workerResult))
          }
        }
      }
    }
  }

  private def executeBatch(batchTask: BatchTask): Task[PrimaryWorkerResults] = {
    recursiveBatchProcessing(batchTask.batch, None, Map.empty, None).flatMap { case (existingContentStatic, results) =>
      val bgTask = LocalTask(
        key = batchTask.key,
        group = HyperstorageWorkerSettings.SECONDARY,
        ttl = System.currentTimeMillis() + backgroundTaskTimeout.toMillis,
        expectsResult = false,
        BackgroundContentTasksPost(BackgroundContentTask(batchTask.key))(MessagingContext.empty),
        extra = Null
      )

      var nextBatchSize = 0l
      var nextContentStatic: ContentBase = null
      var nextSaveBatch = mutable.Map[String, (mutable.ArrayBuffer[(LocalTaskWithId, PrimaryWorkerRequest, Transaction, Option[ResponseBase], Value)], Content, Long)]()
      val saveBatchTasks = mutable.ArrayBuffer[(Seq[(LocalTaskWithId, PrimaryWorkerRequest, Transaction, Option[ResponseBase], Value)], Task[Seq[(Long, Option[ResponseBase], Value)]])]()

      def addBatchTask(): Unit = {
        val n = nextSaveBatch.values.toList
        val ni = n.flatMap { i => i._1 }
        saveBatchTasks += ((ni, saveBatchToDb(nextContentStatic, n)))
        nextSaveBatch = mutable.Map[String, (mutable.ArrayBuffer[(LocalTaskWithId, PrimaryWorkerRequest, Transaction, Option[ResponseBase], Value)], Content, Long)]()
        nextBatchSize = 0l
      }

      results.foreach {
        case (localTaskWithId, request, Success((transaction, content, response, extra))) =>
          nextContentStatic = content
          val size = contentSize(content)
          nextSaveBatch.get(content.itemId) match {
            case Some(r) =>
              val existingSize = r._3
              if ((nextBatchSize - existingSize + size) > maxBatchSizeInBytes) {
                addBatchTask()
              }
              r._1.+=((localTaskWithId, request, transaction, response, extra))
              nextSaveBatch += content.itemId -> (r._1, content, size)
              nextBatchSize += (if (nextBatchSize == 0) content.transactionList.size * 16 else 0) + size

            case None =>
              if ((nextBatchSize + size) > maxBatchSizeInBytes && nextSaveBatch.nonEmpty) {
                addBatchTask()
              }
              nextSaveBatch += content.itemId -> (mutable.ArrayBuffer((localTaskWithId, request, transaction, response, extra)), content, size)
              nextBatchSize += (if (nextBatchSize == 0) content.transactionList.size * 16 else 0) + size
          }

        case _ =>
      }
      if (nextSaveBatch.nonEmpty) {
        addBatchTask()
      }

      val purgeCollectionTask = existingContentStatic.map { c =>
        if (isCollectionUri(c.documentUri) && c.isDeleted.contains(true)) {
          db.purgeCollection(c.documentUri)
        } else {
          Task.unit
        }
      } getOrElse {
        Task.unit
      }

      // logger.warn(s"total results on ${bgTask.key}: ${results.size}, successfull: ${results.count(_._3.isSuccess)}, failed: ${results.count(_._3.isFailure)}")
      purgeCollectionTask.flatMap { _ =>
        recursiveSaveBatchesToDb(saveBatchTasks).map { saveResults =>
          val failedResults = results.filter(i => i._3.isFailure).map{ r =>
            val res = if (r._1.inner.expectsResult) {
              implicit val mcx = r._2
              Some(hyperbusException(r._3.asInstanceOf[Failure[_]].exception, r._1.inner))
            } else {
              None
            }
            (r._1.id, res, Null)
          }
          PrimaryWorkerResults(Seq(bgTask, WorkerBatchTaskResult(batchTask.key, batchTask.group, saveResults ++ failedResults)))
        }
      }
    }
  }

  private def recursiveSaveBatchesToDb(saveBatchTasks: ArrayBuffer[(Seq[(LocalTaskWithId, PrimaryWorkerRequest, Transaction, Option[ResponseBase], Value)], Task[Seq[(Long, Option[ResponseBase], Value)]])]): Task[Seq[(Long, Option[ResponseBase], Value)]] = {
    if (saveBatchTasks.isEmpty) {
      Task.now(Seq.empty)
    } else {
      val batch = saveBatchTasks.head
      batch._2.materialize.flatMap {
        case Success(r) =>
          if (saveBatchTasks.tail.isEmpty) {
            Task.now(r)
          }
          else {
            recursiveSaveBatchesToDb(saveBatchTasks.tail).map { tailResults =>
              r ++ tailResults
            }
          }

        case Failure(ex) =>
          Task.now(saveBatchTasks.flatMap { i =>
            i._1.map { ir =>
              val r = if (ir._1.inner.expectsResult) {
                implicit val mcx = ir._2
                Some(hyperbusException(ex, ir._1.inner))
              } else {
                None
              }

              (ir._1.id, r, Null)
            }
          })
      }
    }
  }

  private def saveBatchToDb(nextContentStatic: ContentBase,
                            nextSaveBatch: Seq[(Seq[(LocalTaskWithId, PrimaryWorkerRequest, Transaction, Option[ResponseBase], Value)], Content, Long)]):
    Task[Seq[(Long, Option[ResponseBase], Value)]] = {
    val WRITE_TRIES = 3
    val transactionTasks = Task.eval {
      //logger.error(s"!!!INSERTING TRANSACTIONS: ${nextSaveBatch.flatMap(_._1.map(_._3))}")
      Task.gatherUnordered(nextSaveBatch.flatMap { n =>
        n._1.map { i =>
          //logger.info(s"INSTR ${n._2.documentUri} #${i._3.uuid}")
          db.insertTransaction(i._3).onErrorRestart(WRITE_TRIES).map { _ =>
            (i._1.id, i._4, i._5)
          }
        }
      })
    }.flatten
    transactionTasks.flatMap { results =>
      //logger.error(s"!!!INSERTING CONTENT: ${nextSaveBatch.map(_._2)}")
      db.applyBatch(nextSaveBatch.map(_._2))
        .onErrorRestart(WRITE_TRIES)
        .map { _ =>
        results
      }
    }
  }

  private def contentSize(content: Content): Long = {
    128 + Utf8.encodedLength(content.itemId) + Utf8.encodedLength(content.documentUri) +
      (if (content.isDeleted.contains(true)) {
        0
      } else {
        content.body.map(Utf8.encodedLength).getOrElse(0)
      })
  }

  private def recursiveBatchProcessing(batch: Seq[LocalTaskWithId],
                                       existingContentStaticTask: Option[Task[Option[ContentBase]]],
                                       existingContentTasks: Map[String, Task[Option[Content]]],
                                       indexDefsTask: Option[Task[List[IndexDef]]]
                                      ): Task[(Option[ContentBase], Seq[(LocalTaskWithId, PrimaryWorkerRequest, Try[(Transaction, Content, Option[ResponseBase], Value)])])] = {
    val task = batch.head.inner
    val READ_TRIES = 5
    prepareToTransform(task).flatMap { case (documentUri, itemId, idField, request) =>
      val existingContentTasksF = existingContentTasks.getOrElse(itemId, db.selectContent(documentUri, itemId).memoizeOnSuccess)
      val existingContentStaticTaskF = existingContentStaticTask.getOrElse {
        if (ContentLogic.isCollectionUri(documentUri)) {
          db.selectContentStatic(documentUri).memoizeOnSuccess
        } else {
          existingContentTasksF
        }
      }
      val indexDefsTaskF = indexDefsTask.getOrElse {
        if (ContentLogic.isCollectionUri(documentUri)) {
          db.selectIndexDefs(documentUri).map(_.toList).memoizeOnSuccess
        } else {
          Task.now(List.empty)
        }
      }

      Task.zip3(existingContentTasksF, existingContentStaticTaskF, indexDefsTaskF)
        .onErrorRestart(READ_TRIES)
        .flatMap {
        case (existingContent, existingContentStatic, indexDefs) ⇒

          validateBeforeTransform(documentUri, task, request, existingContentStatic, existingContent).map { _ =>
            val (t, c) = transform(documentUri, itemId, request, existingContent, existingContentStatic, indexDefs)
            val response = if (task.expectsResult) {
              Some(transactionToResponse(documentUri, itemId, t, idField, request, existingContent))
            }
            else {
              None
            }
            (t, c, response, Null)
          }.materialize
            .flatMap { r: Try[(Transaction, Content, Option[ResponseBase], Value)] =>
              if (batch.tail.isEmpty) {
                Task.now((existingContentStatic, Seq((batch.head, request, r))))
              }
              else {
                val (existingContentTasksN, existingContentStaticTaskN) = r match {
                  case Success(s) => (existingContentTasks + (itemId -> Task.now(Some(s._2))), Task.now(Some(s._2)))
                  case _ => (existingContentTasks + (itemId -> Task.now(existingContent)), existingContentStaticTaskF)
                }

                recursiveBatchProcessing(batch.tail, Some(existingContentStaticTaskN), existingContentTasksN, Some(indexDefsTaskF))
                  .map { tailResult =>
                    (existingContentStatic, Seq((batch.head, request, r)) ++ tailResult._2)
                  }
              }
            }
      }
    }
  }

  private def transformViewPut(task: LocalTask): PrimaryWorkerRequest = {
    task.request match {
      case request: ViewPut ⇒
        val newHeaders = MessageHeaders.builder
          .++=(request.headers)
          .+=(TransactionLogic.HB_HEADER_VIEW_TEMPLATE_URI → request.body.templateUri.toValue)
          .+=(TransactionLogic.HB_HEADER_FILTER → request.body.filter.toValue)
          .requestHeaders()

        new ContentPut(
          path = request.path,
          body = DynamicBody(Null),
          headers = newHeaders,
          plain__init = true
        )

      case other: PrimaryWorkerRequest => other
    }
  }

  private def prepareToTransform(task: LocalTask) = {
    val request = transformViewPut(task)
    val ResourcePath(documentUri, itemId) = splitPath(request.path)
    if (documentUri != task.key) {
      Task.raiseError(new IllegalArgumentException(s"Task key ${task.key} doesn't correspond to $documentUri"))
    }
    else {
      implicit val mcx = request
      request.headers.method match {
        case Method.POST ⇒
          // posting new item, converting post to put
          if (itemId.isEmpty || !ContentLogic.isCollectionUri(documentUri)) {
            val id = IdGenerator.create()
            val idFieldName = ContentLogic.getIdFieldName(documentUri)
            val idField = idFieldName → id
            val newItemId = if (ContentLogic.isCollectionUri(documentUri)) id else ""
            val newDocumentUri = if (ContentLogic.isCollectionUri(documentUri)) documentUri else documentUri + "/" + id

            // POST becomes PUT with auto Id
            Task.now((newDocumentUri, newItemId, Some(idField), ContentPut(
              path = request.path + "/" + id,
              body = appendId(filterNulls(request.body), id, idFieldName),
              headers = request.headers.underlying,
              query = request.headers.hrl.query
            )))
          }
          else {
            // todo: replace with BadRequest?
            Task.raiseError(new IllegalArgumentException(s"POST is not allowed on existing item of collection~"))
          }

        case Method.PUT ⇒
          if (itemId.isEmpty) {
            Task.now((documentUri, itemId, None,
              ContentPut(
                path = request.path,
                body = filterNulls(request.body),
                headers = request.headers.underlying,
                query = request.headers.hrl.query
              )
            ))
          }
          else {
            val idFieldName = ContentLogic.getIdFieldName(documentUri)
            Task.now((documentUri, itemId, Some(idFieldName → itemId),
              ContentPut(
                path = request.path,
                body = appendId(filterNulls(request.body), itemId, idFieldName),
                headers = request.headers.underlying,
                query = request.headers.hrl.query
              )
            ))
          }

        case _ ⇒
          Task.now((documentUri, itemId, None, request))
      }
    }
  }

  private def validateBeforeTransform(documentUri: String,
                                      task: LocalTask,
                                      request: PrimaryWorkerRequest,
                                      existingContentStatic: Option[ContentBase],
                                      existingContent: Option[Content]): Task[Any] = {
    implicit val mcx = request
    val trSize = if (existingContentStatic.isDefined) existingContentStatic.get.transactionList.size else 0
    if (existingContentStatic.isDefined && (trSize >= maxIncompleteTransaction)) {
      logger.debug(s"'$documentUri' have too many incomplete transactions: ${existingContentStatic.get.transactionList}")
      Task.raiseError(TooManyRequests(ErrorBody(
        ErrorCode.TRANSACTION_LIMIT, Some(s"Transaction limit ($maxIncompleteTransaction) is reached ($trSize) for '$documentUri'.")
      )))
    }
    else {
      if (existingContentStatic.exists(_.isView.contains(true))
        && !task.extra.dynamic.internal_operation.toBoolean
        && !(
        request.headers.hrl.location == ViewDelete.location ||
          (request.headers.hrl.location == ViewPut.location && !request.headers.contains(TransactionLogic.HB_HEADER_VIEW_TEMPLATE_URI))
        )
      ) Task.raiseError {
        Conflict(ErrorBody(ErrorCode.VIEW_MODIFICATION, Some(s"Can't modify view: $documentUri")))
      }
      else {
        if (!ContentLogic.checkPrecondition(request, existingContent)) Task.raiseError {
          PreconditionFailed(ErrorBody(ErrorCode.NOT_MATCHED, Some(s"ETag doesn't match")))
        }
        else {
          Task.unit
        }
      }
    }
  }

  private def selectExisting(documentUri: String,
                             itemId: String): Task[(Option[Content], Option[ContentBase], List[IndexDef])] = {

    val indexDefsTask = if (ContentLogic.isCollectionUri(documentUri)) {
      db.selectIndexDefs(documentUri).map(_.toList)
    } else {
      Task.now(List.empty)
    }
    val contentTask = db.selectContent(documentUri, itemId).memoize
    val contentTaskStatic  = if (ContentLogic.isCollectionUri(documentUri)) {
      db.selectContentStatic(documentUri)
    } else {
      contentTask
    }

    Task.zip3(contentTask, contentTaskStatic, indexDefsTask)
  }

  private def transactionToResponse(documentUri: String,
                                  itemId: String,
                                  newTransaction: Transaction,
                                  idField: Option[(String,String)],
                                  request: PrimaryWorkerRequest,
                                  existingContent: Option[Content]): ResponseBase = {
    implicit val mcx = request
    val resourceCreated = (existingContent.isEmpty || existingContent.exists(_.isDeleted.contains(true))) && request.headers.method != Method.DELETE
    if (resourceCreated) {
      val target = idField.map(kv ⇒ Obj.from(kv._1 → Text(kv._2))).getOrElse(Null)
      Created(HyperStorageTransactionCreated(newTransaction.uuid.toString, request.path, newTransaction.revision, target),
        location = HRL(ContentGet.location, Obj.from("path" → (documentUri + "/" + itemId))))
    }
    else {
      Ok(api.HyperStorageTransaction(newTransaction.uuid.toString, request.path, newTransaction.revision))
    }
  }

  private def saveToDb(existingContentStatic: Option[ContentBase],
                       newTransaction: Transaction,
                       newContent: Content): Task[Any] = {
    db.insertTransaction(newTransaction) flatMap { _ ⇒
      if (!newContent.itemId.isEmpty && newContent.isDeleted.contains(true)) {
        // deleting item
        db.deleteContentItem(newContent, newContent.itemId)
      }
      else {
        val purgeExisting = if (isCollectionUri(newContent.documentUri) && existingContentStatic.exists(_.isDeleted.contains(true))) {
          db.purgeCollection(newContent.documentUri)
        } else {
          Task.unit
        }

        purgeExisting.flatMap { _ =>
          if (isCollectionUri(newContent.documentUri) && newContent.itemId.isEmpty) {
            db.insertStaticContent(ContentStatic(
              documentUri = newContent.documentUri,
              revision = newContent.revision,
              transactionList = newContent.transactionList,
              isDeleted = newContent.isDeleted,
              count = newContent.count,
              isView = newContent.isView
            ))
          }
          else {
            db.insertContent(newContent)
          }
        }
      }
    }
  }

  private def findObsoleteIndexItems(existingContent: Option[Content], newContent: Content, indexDefs: Seq[IndexDef]) : Option[String] = {
    import com.hypertino.binders.json.JsonBinders._
    // todo: refactor, this is crazy method
    // todo: work with Value content instead of string
    val m = existingContent.flatMap { c ⇒
      if (c.isDeleted.contains(true)) None else {
        Some(c.bodyValue)
      }
    } map { existingContentValue: Value ⇒
      val newContentValueOption = if(newContent.isDeleted.contains(true)) None else {
        Some(newContent.bodyValue)
      }

      indexDefs.flatMap { indexDef ⇒
        val idFieldName = ContentLogic.getIdFieldName(newContent.documentUri)
        val sortByExisting = IndexLogic.extractSortFieldValues(idFieldName, indexDef.sortByParsed, existingContentValue)

        val sortByNew = newContentValueOption.map { newContentValue ⇒
          IndexLogic.extractSortFieldValues(idFieldName, indexDef.sortByParsed, newContentValue)
        }

        if (sortByExisting != sortByNew) {
          Some(indexDef.indexId → sortByExisting)
        }
        else{
          None
        }
      }
    }
    m.map { mm ⇒
      val mp: Map[String, Map[String, Value]] = mm.map(kv ⇒ kv._1 → kv._2.toMap).toMap
      mp.toJson
    }
  }

  private def createNewTransaction(documentUri: String,
                                   itemId: String,
                                   request: PrimaryWorkerRequest,
                                   existingContent: Option[Content],
                                   existingContentStatic: Option[ContentBase],
                                   transactionBodyValue: Option[Value] = None
                                  ): Transaction = {
    val revision = existingContentStatic match {
      case None ⇒ 1
      case Some(content) ⇒ content.revision + 1
    }

    val recordCount = existingContent.map(_ ⇒ 0 ).getOrElse(1)

    val uuid = UUIDs.timeBased()

    // always preserve null fields in transaction
    // this is required to correctly publish patch events
    implicit val so = SerializationOptions.forceOptionalFields
    val transaction = DynamicRequest(
      body = transactionBodyValue.map(DynamicBody(_)).getOrElse(request.body),
      headers = MessageHeaders.builder
        .++=(request.headers)
        .+=(Header.REVISION → Number(revision))
        .+=(Header.COUNT → Number(recordCount))
        .+=(HyperStorageHeader.HYPER_STORAGE_TRANSACTION → uuid.toString)
        .withMethod("feed:" + request.headers.method)
        .requestHeaders()
    ).serializeToString
    TransactionLogic.newTransaction(documentUri, itemId, revision, transaction, uuid)
  }

  private def transform(documentUri: String,
                        itemId: String,
                        request: PrimaryWorkerRequest,
                        existingContent: Option[Content],
                        existingContentStatic: Option[ContentBase],
                        indexDefs: List[IndexDef]): (Transaction,Content) = {
    val ttl = request.headers.get(HyperStorageHeader.HYPER_STORAGE_TTL).map(_.toInt)
    val (newTransaction, newContent) = request.headers.method match {
      case Method.PUT ⇒ put(documentUri, itemId, request, existingContent, existingContentStatic, ttl)
      case Method.PATCH ⇒ patch(documentUri, itemId, request, existingContent, existingContentStatic, ttl)
      case Method.DELETE ⇒ delete(documentUri, itemId, request, existingContent, existingContentStatic)
    }
    val obsoleteIndexItems = if (request.headers.method != Method.POST && ContentLogic.isCollectionUri(documentUri) && !itemId.isEmpty) {
      findObsoleteIndexItems(existingContent, newContent, indexDefs)
    }
    else {
      None
    }
    val newTransactionWithOI = newTransaction.copy(obsoleteIndexItems = obsoleteIndexItems)
    (newTransactionWithOI, newContent)
  }

  private def put(documentUri: String,
                         itemId: String,
                         request: PrimaryWorkerRequest,
                         existingContent: Option[Content],
                         existingContentStatic: Option[ContentBase],
                         ttl: Option[Int]
                        ): (Transaction,Content) = {
    implicit val mcx = request

    val isCollection = ContentLogic.isCollectionUri(documentUri)
    val newBody =
      if (isCollection && itemId.isEmpty) {
        if (!request.body.content.isEmpty)
          throw Conflict(ErrorBody(ErrorCode.COLLECTION_PUT_NOT_IMPLEMENTED, Some(s"Can't put non-empty collection")))
        else
          None
      }
      else {
        Some(request.body.serializeToString)
      }

    val newTransaction = createNewTransaction(documentUri, itemId, request, existingContent, existingContentStatic)
    val newContent = existingContentStatic match {
      case None ⇒
        val newCount = if (isCollection) {
          if (itemId.isEmpty) {
            Some(0l)
          }
          else {
            Some(1l)
          }
        }
        else {
          None
        }
        Content(documentUri, itemId, newTransaction.revision,
          transactionList = List(newTransaction.uuid),
          isDeleted = existingContent.flatMap(_.isDeleted.map(_ ⇒ false)),
          count = newCount,
          isView = if (request.headers.contains(TransactionLogic.HB_HEADER_VIEW_TEMPLATE_URI)) Some(true) else None,
          body = newBody,
          createdAt = existingContent.map(_.createdAt).getOrElse(new Date),
          modifiedAt = existingContent.flatMap(_.modifiedAt),
          ttl,
          ttl
        )

      case Some(static) ⇒
        if (request.headers.contains(TransactionLogic.HB_HEADER_VIEW_TEMPLATE_URI) && !static.isView.contains(true)) {
          throw Conflict(ErrorBody(ErrorCode.COLLECTION_VIEW_CONFLICT, Some(s"Can't put view over existing collection")))
        }
        val newCount = if (isCollection) {
          static.count.map(_ + (if (existingContent.isEmpty) 1 else 0))
        } else {
          None
        }
        Content(documentUri, itemId, newTransaction.revision,
          transactionList = newTransaction.uuid +: static.transactionList,
          isDeleted = existingContent.flatMap(_.isDeleted.map(_ ⇒ false)),
          count = newCount,
          isView = static.isView,
          body = newBody,
          createdAt = existingContent.map(_.createdAt).getOrElse(new Date),
          modifiedAt = existingContent.flatMap(_.modifiedAt),
          ttl,
          ttl
        )
    }
    (newTransaction,newContent)
  }

  private def patch(documentUri: String,
                           itemId: String,
                           request: PrimaryWorkerRequest,
                           existingContent: Option[Content],
                           existingContentStatic: Option[ContentBase],
                           ttl: Option[Int]
                          ): (Transaction,Content) = {
    implicit val mcx = request

    val isCollection = ContentLogic.isCollectionUri(documentUri)
    val count: Long = if (isCollection) {
      if (itemId.isEmpty) {
        throw Conflict(ErrorBody(ErrorCode.COLLECTION_PATCH_NOT_IMPLEMENTED, Some(s"PATCH is not allowed for a collection~")))
      }
      existingContentStatic.map(_.count.getOrElse(0l)).getOrElse(0l)
    } else {
      0l
    }

    val existingBody = existingContent
      .find(!_.isDeleted.contains(true))
      .map(_.bodyValue)
      .getOrElse{
        if (isCollection) {
          val idFieldName = ContentLogic.getIdFieldName(documentUri)
          val idField = idFieldName → Text(itemId)
          Obj.from(idField)
        }
        else {
          Obj.empty
        }
      }

    val patch = ContentLogic.applyPatch(existingBody, request)

    if (isCollection) {
      val idFieldName = ContentLogic.getIdFieldName(documentUri)
      val newIdField = patch(idFieldName)
      if (newIdField.nonEmpty && newIdField != existingBody(idFieldName)) {
        throw Conflict(ErrorBody(ErrorCode.FIELD_IS_PROTECTED, Some(s"$idFieldName is read only")))
      }
    }

    val newTransaction = createNewTransaction(documentUri, itemId, request, existingContent, existingContentStatic, Some(patch))
    val newBody = mergeBody(existingBody, patch)

    val now = new Date()
    val newContent = existingContent match {
      case Some(content) if !content.isDeleted.contains(true) ⇒
        Content(documentUri, itemId, newTransaction.revision,
          transactionList = newTransaction.uuid +: content.transactionList,
          isDeleted = existingContent.flatMap(_.isDeleted.map(_ ⇒ false)),
          count = content.count,
          isView = content.isView,
          body = newBody,
          createdAt = content.createdAt,
          modifiedAt = Some(now),
          ttl,
          ttl
        )

      case _ ⇒
        Content(documentUri, itemId, newTransaction.revision,
          transactionList = List(newTransaction.uuid),
          isDeleted = existingContent.flatMap(_.isDeleted.map(_ ⇒ false)),
          count = if (isCollection) Some(count + 1) else None,
          isView = None,
          body = newBody,
          createdAt = now,
          modifiedAt = None,
          ttl,
          ttl
        )
    }
    (newTransaction,newContent)
  }

  private def mergeBody(existing: Value, patch: Value): Option[String] = {
    import com.hypertino.binders.json.JsonBinders._
    val newBodyContent = FilterNulls.filterNulls(existing % patch)
    newBodyContent match {
      case Null ⇒ None
      case other ⇒ Some(other.toJson)
    }
  }

  private def delete(documentUri: String,
                            itemId: String,
                            request: PrimaryWorkerRequest,
                            existingContent: Option[Content],
                            existingContentStatic: Option[ContentBase]): (Transaction,Content) = {
    implicit val mcx: MessagingContext = request
    existingContentStatic match {
      case None ⇒
        throw NotFound(ErrorBody(ErrorCode.NOT_FOUND, Some(s"Hyperstorage resource '${request.path}' is not found")))

      case Some(content) ⇒
        if (content.isView.contains(true) && request.headers.hrl.location != ViewDelete.location && itemId.isEmpty) {
          throw Conflict(ErrorBody(ErrorCode.COLLECTION_IS_VIEW, Some(s"Can't delete view collection directly")))
        }

        val newTransaction = createNewTransaction(documentUri, itemId, request, existingContent, existingContentStatic)
        val newContent = Content(documentUri, itemId, newTransaction.revision,
          transactionList = newTransaction.uuid +: content.transactionList,
          isDeleted = Some(true),
          count = content.count.map(_ - 1),
          isView = content.isView,
          body = None,
          createdAt = existingContent.map(_.createdAt).getOrElse(new Date()),
          modifiedAt = Some(new Date()),
          None,
          None
        )
        (newTransaction,newContent)
    }
  }

  private def hyperbusException(e: Throwable, task: ShardTask)(implicit mcx: MessagingContext): ResponseBase = {
    val (response: HyperbusError[ErrorBody], logException) = e match {
      case h: HyperbusClientError[ErrorBody] @unchecked ⇒ (h, false)
      case h: HyperbusError[ErrorBody] @unchecked ⇒ (h, true)
      case _ ⇒ (InternalServerError(ErrorBody(ErrorCode.UPDATE_FAILED, Some(e.toString))), true)
    }

    if (logException) {
      logger.error(s"task $task is failed", e)
    }

    response
  }

  private def filterNulls(body: DynamicBody): DynamicBody = {
    body.copy(content = body.content ~~ FilterNulls)
  }

  private def appendId(body: DynamicBody, id: Value, idFieldName: String): DynamicBody = {
    body.copy(content = Obj(body.content.toMap + (idFieldName → id)))
  }
}

object PrimaryWorker {
  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, backgroundTaskTimeout: FiniteDuration, maxIncompleteTransaction: Int, maxBatchSizeInBytes: Long, scheduler: monix.execution.Scheduler) =
    Props(new PrimaryWorker(hyperbus, db, tracker, backgroundTaskTimeout, maxIncompleteTransaction, maxBatchSizeInBytes, scheduler))
}

case class ExpressionEvaluatorContext(request: PrimaryWorkerRequest, original: Value) extends Context{
  private lazy val obj = Obj.from(
    "headers" → Obj(request.headers),
    "location" → request.headers.hrl.location,
    "query" → request.headers.hrl.query,
    "method" → request.headers.method,
    "body" → request.body.content,
    "original" → original
  )

  override def identifier = {
    case identifier ⇒ obj(identifier.segments.map(Text))
  }
  override def binaryOperation: PartialFunction[(Value, Identifier, Value), Value] = Map.empty
  override def customOperators = Seq.empty
  override def function: PartialFunction[(Identifier, Seq[Value]), Value] = {
    case (Identifier(Seq("new_id")), _) ⇒ IdGenerator.create()
    case (Identifier(Seq("new_seq")), _) ⇒ SeqGenerator.create()
  }
  override def unaryOperation = Map.empty
  override def binaryOperationLeftArgument = Map.empty
}

object FilterNulls extends ValueVisitor[Value] {
  override def visitNumber(d: Number): Value = d
  override def visitNull(): Value = Null
  override def visitBool(d: Bool): Value = d
  override def visitObj(d: Obj): Value = Obj(d.v.flatMap {
    case (k, Null) ⇒ None
    case (k, other) ⇒ Some(k → filterNulls(other))
  })
  override def visitText(d: Text): Value = d
  override def visitLst(d: Lst): Value = d
  def filterNulls(content: Value): Value = {
    content ~~ this
  }
}
