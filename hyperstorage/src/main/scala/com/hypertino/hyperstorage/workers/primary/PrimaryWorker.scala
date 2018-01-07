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
import com.hypertino.hyperstorage.sharding.{BatchTask, LocalTask, ShardTask, WorkerTaskResult}
import com.hypertino.hyperstorage.utils.{ErrorCode, TrackerUtils}
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.metrics.MetricsTracker
import com.hypertino.parser.ast.Identifier
import com.hypertino.parser.eval.Context
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

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
      run(singleTask, executeSingleTask(singleTask))

//    case batch: BatchTask ⇒
//      run(batch, executeBatch(batch))
  }

  private def run(task: ShardTask, code: Task[Any]): Unit = {
    become(running(sender()))
    import TrackerUtils._
    tracker.timeOfTask(Metrics.PRIMARY_PROCESS_TIME) {
      code.onErrorRecover {
        case e: Throwable =>
          logger.error(s"Can't complete task: $task", e)
          PrimaryWorkerResults(Seq(
            WorkerTaskResult(task, hyperbusException(e, task)(MessagingContext.empty))
          ))
      }
    }.runAsync(scheduler) pipeTo self
  }

  def running(owner: ActorRef): Receive = {
    case r: PrimaryWorkerResults =>
      r.results.foreach(owner ! _)
      unbecome()
  }

  def executeSingleTask(task: LocalTask): Task[PrimaryWorkerResults] = {
    Task.fromTry(Try {
      val (documentUri, itemId, idField, request) = prepareToTransform(task)
      selectExisting(documentUri, itemId, request).flatMap { case (existingContent, existingContentStatic, indexDefs) ⇒
        validateBeforeTransform(documentUri, task, request, existingContentStatic, existingContent) flatMap { _ =>
          val (newTransaction, newContent) = transform(documentUri, itemId, request, existingContent, existingContentStatic, indexDefs)
          saveToDb(documentUri, itemId, existingContentStatic, newTransaction, newContent).map { _ =>
            implicit val mcx = request
            val bgTask = LocalTask(
              key = documentUri,
              group = HyperstorageWorkerSettings.SECONDARY,
              ttl = System.currentTimeMillis() + backgroundTaskTimeout.toMillis,
              expectsResult = false,
              BackgroundContentTasksPost(BackgroundContentTask(documentUri)),
              extra = Null
            )
            val workerResult =  WorkerTaskResult(task, transactionToResponse(documentUri,itemId,newTransaction,idField,request,existingContent))
            logger.debug(s"task $task is completed")
            PrimaryWorkerResults(Seq(bgTask, workerResult))
          }
        }
      }
    }).flatten
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
      throw new IllegalArgumentException(s"Task key ${task.key} doesn't correspond to $documentUri")
    }

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
          (newDocumentUri, newItemId, Some(idField), ContentPut(
            path = request.path + "/" + id,
            body = appendId(filterNulls(request.body), id, idFieldName),
            headers = request.headers.underlying,
            query = request.headers.hrl.query
          ))
        }
        else {
          // todo: replace with BadRequest?
          throw new IllegalArgumentException(s"POST is not allowed on existing item of collection~")
        }

      case Method.PUT ⇒
        if (itemId.isEmpty) {
          (documentUri, itemId, None,
            ContentPut(
              path=request.path,
              body=filterNulls(request.body),
              headers=request.headers.underlying,
              query = request.headers.hrl.query
            )
          )
        }
        else {
          val idFieldName = ContentLogic.getIdFieldName(documentUri)
          (documentUri, itemId, Some(idFieldName → itemId),
            ContentPut(
              path=request.path,
              body=appendId(filterNulls(request.body), itemId, idFieldName),
              headers=request.headers.underlying,
              query = request.headers.hrl.query
            )
          )
        }

      case _ ⇒
        (documentUri, itemId, None, request)
    }
  }

  private def validateBeforeTransform(documentUri: String,
                                      task: LocalTask,
                                      request: PrimaryWorkerRequest,
                                      existingContentStatic: Option[ContentBase],
                                      existingContent: Option[Content]): Task[Any] = {
    implicit val mcx = request
    if (existingContentStatic.isDefined && existingContentStatic.get.transactionList.size >= maxIncompleteTransaction) {
      Task.raiseError(TooManyRequests(ErrorBody(
        ErrorCode.TRANSACTION_LIMIT, Some(s"Transaction limit ($maxIncompleteTransaction) is reached for '$documentUri'")
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
                             itemId: String,
                             request: PrimaryWorkerRequest): Task[(Option[Content], Option[ContentBase], List[IndexDef])] = {
    if (request.headers.method == Method.POST) {
      Task.now((None, None, List.empty))
    } else {
      val indexDefsTask = if (ContentLogic.isCollectionUri(documentUri)) {
        db.selectIndexDefs(documentUri).map(_.toList)
      } else {
        Task.now(List.empty)
      }
      val contentTask = db.selectContent(documentUri, itemId)

      Task.zip2(contentTask, indexDefsTask).flatMap { case (contentOption, indexDefs) ⇒
        contentOption match {
          case Some(_) ⇒ Task.now((contentOption, contentOption, indexDefs))
          case None if ContentLogic.isCollectionUri(documentUri) ⇒ db.selectContentStatic(documentUri).map(contentStatic ⇒
            (contentOption, contentStatic, indexDefs)
          )
          case _ ⇒ Task.now((contentOption, None, indexDefs))
        }
      }
    }
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

  private def saveToDb(documentUri: String,
                       itemId: String,
                       existingContentStatic: Option[ContentBase],
                       newTransaction: Transaction,
                       newContent: Content): Task[Any] = {
    db.insertTransaction(newTransaction) flatMap { _ ⇒ {
      if (!itemId.isEmpty && newContent.isDeleted.contains(true)) {
        // deleting item
        db.deleteContentItem(newContent, itemId)
      }
      else {
        if (isCollectionUri(documentUri) && existingContentStatic.exists(_.isDeleted.contains(true))) {
          db.purgeCollection(documentUri).flatMap { _ ⇒
            db.insertContent(newContent)
          }
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
    (newTransaction,newContent)
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
