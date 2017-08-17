package com.hypertino.hyperstorage.workers.primary

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import com.codahale.metrics.Timer
import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.serialization.{MessageReader, SerializationOptions}
import com.hypertino.hyperbus.util.IdGenerator
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.IndexLogic
import com.hypertino.hyperstorage.metrics.Metrics
import com.hypertino.hyperstorage.sharding.{ShardTask, ShardTaskComplete}
import com.hypertino.hyperstorage.workers.secondary.BackgroundContentTask
import com.hypertino.metrics.MetricsTracker

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class PrimaryContentTask(key: String, ttl: Long, content: String, expectsResult: Boolean) extends ShardTask {
  def group = "hyperstorage-primary-worker"
  def isExpired = ttl < System.currentTimeMillis()
}

@SerialVersionUID(1L) case class PrimaryWorkerTaskResult(content: String)

case class PrimaryWorkerTaskFailed(task: ShardTask, inner: Throwable)

case class PrimaryWorkerTaskCompleted(task: ShardTask, transaction: Transaction, resourceCreated: Boolean)

// todo: Protect from direct view items updates!!!
class PrimaryWorker(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, backgroundTaskTimeout: FiniteDuration) extends Actor with ActorLogging {

  import ContentLogic._
  import context._

  val filterNullsVisitor = new ValueVisitor[Value] {
    override def visitNumber(d: Number): Value = d

    override def visitNull(): Value = Null

    override def visitBool(d: Bool): Value = d

    override def visitObj(d: Obj): Value = Obj(d.v.flatMap {
      case (k, Null) ⇒ None
      case (k, other) ⇒ Some(k → filterNulls(other))
    })

    override def visitText(d: Text): Value = d

    override def visitLst(d: Lst): Value = d
  }

  def receive = {
    case task: PrimaryContentTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: PrimaryContentTask): Unit = {
    val trackProcessTime = tracker.timer(Metrics.PRIMARY_PROCESS_TIME).time()

    Try {
      val request = MessageReader.fromString(task.content, DynamicRequest.apply)
      val ResourcePath(documentUri, itemId) = splitPath(request.path)
      if (documentUri != task.key) {
        throw new IllegalArgumentException(s"Task key ${task.key} doesn't correspond to $documentUri")
      }
      val (updatedItemId, updatedRequest) = request.headers.method match {
        case Method.POST ⇒
          // posting new item into collection, converting post to put
          val id = IdGenerator.create()
          if (ContentLogic.isCollectionUri(documentUri) && itemId.isEmpty) {
            val hrl = request.headers.hrl
            val newHrl = HRL(hrl.location, Obj.from(
              hrl.query.toMap.toSeq.filterNot(_._1=="path") ++ Seq("path" → Text(request.path + "/" + id))
                : _*)
            )
            (id, request.copy(
              //uri = Uri(request.uri.pattern, request.uri.args + "path" → Specific(request.path + "/" + id)),
              headers = Headers
                .builder
                .++=(request.headers)
                .withMethod(Method.PUT)
                .withHRL(newHrl)
                .requestHeaders(), // POST becomes PUT with auto Id
              body = appendId(filterNulls(request.body), id, ContentLogic.getIdFieldName(documentUri))
            ))
          }
          else {
            // todo: replace with BadRequest?
            throw new IllegalArgumentException(s"POST is allowed only for a collection~")
          }

        case Method.PUT ⇒
          if (itemId.isEmpty) {
            if (ContentLogic.isCollectionUri(documentUri) && itemId.isEmpty && request.headers.hrl.location == ViewPut.location) {
              // todo: validate template_uri & filter_by
              val newHeaders = Headers
                .builder
                .++=(request.headers)
                .+=(TransactionLogic.HB_HEADER_TEMPLATE_URI → request.body.content.template_uri)
                .+=(TransactionLogic.HB_HEADER_FILTER → request.body.content.filter_by)
                .requestHeaders()
              (itemId, request.copy(body=DynamicBody(Null), headers=newHeaders))
            }
            else {
              (itemId, request.copy(body = filterNulls(request.body)))
            }
          }
          else {
            (itemId, request.copy(body = appendId(filterNulls(request.body), itemId, ContentLogic.getIdFieldName(documentUri))))
          }
        case _ ⇒
          (itemId, request)
      }
      (documentUri, updatedItemId, updatedRequest)
    } map {
      case (documentUri: String, itemId: String, request: DynamicRequest) ⇒
        become(taskWaitResult(owner, task, request, trackProcessTime)(request))

        // fetch and complete existing content
        executeResourceUpdateTask(owner, documentUri, itemId, task, request)
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't deserialize and split path for: $task")
        owner ! ShardTaskComplete(task, hyperbusException(e, task)(MessagingContext.empty))
    }
  }

  private def executeResourceUpdateTask(owner: ActorRef, documentUri: String, itemId: String, task: PrimaryContentTask, request: DynamicRequest) = {
    val futureExisting : Future[(Option[Content], Option[ContentBase], List[IndexDef])] =
      if (request.headers.method == Method.POST) {
        Future.successful((None, None, List.empty))
      } else {
        val futureIndexDefs = if (ContentLogic.isCollectionUri(documentUri)) {
          db.selectIndexDefs(documentUri).map(_.toList)
        } else {
          Future.successful(List.empty)
        }
        val futureContent = db.selectContent(documentUri, itemId)

        (for (
          contentOption ← futureContent;
          indexDefs ← futureIndexDefs
        ) yield {
          (contentOption, indexDefs)
        }).flatMap { case (contentOption, indexDefs) ⇒
          contentOption match {
            case Some(_) ⇒ Future.successful((contentOption, contentOption, indexDefs))
            case None if ContentLogic.isCollectionUri(documentUri) ⇒ db.selectContentStatic(documentUri).map(contentStatic ⇒
              (contentOption, contentStatic, indexDefs)
            )
            case _ ⇒ Future.successful((contentOption, None, indexDefs))
          }
        }
      }

    futureExisting.flatMap { case (existingContent, existingContentStatic, indexDefs) ⇒
      updateResource(documentUri, itemId, request, existingContent, existingContentStatic, indexDefs) map { newTransaction ⇒
        PrimaryWorkerTaskCompleted(task, newTransaction, existingContent.isEmpty && request.headers.method != Method.DELETE)
      }
    } recover {
      case NonFatal(e) ⇒
        PrimaryWorkerTaskFailed(task, e)
    } pipeTo context.self
  }

  private def updateResource(documentUri: String,
                             itemId: String,
                             request: DynamicRequest,
                             existingContent: Option[Content],
                             existingContentStatic: Option[ContentBase],
                             indexDefs: Seq[IndexDef]
                            ): Future[Transaction] = {
    val newTransaction = createNewTransaction(documentUri, itemId, request, existingContent, existingContentStatic)
    val newContent = updateContent(documentUri, itemId, newTransaction, request, existingContent, existingContentStatic)
    val obsoleteIndexItems = if (request.headers.method != Method.POST && ContentLogic.isCollectionUri(documentUri) && !itemId.isEmpty) {
      findObsoleteIndexItems(existingContent, newContent, indexDefs)
    }
    else {
      None
    }
    val newTransactionWithOI = newTransaction.copy(obsoleteIndexItems = obsoleteIndexItems)
    db.insertTransaction(newTransactionWithOI) flatMap { _ ⇒ {
      if (!itemId.isEmpty && newContent.isDeleted) {
        // deleting item
        db.deleteContentItem(newContent, itemId)
      }
      else {
        db.insertContent(newContent)
      }
    } map { _ ⇒
      newTransactionWithOI
    }
    }
  }

  private def findObsoleteIndexItems(existingContent: Option[Content], newContent: Content, indexDefs: Seq[IndexDef]) : Option[String] = {
    import com.hypertino.binders.json.JsonBinders._
    // todo: refactor, this is crazy method
    // todo: work with Value content instead of string
    val m = existingContent.flatMap { c ⇒
      if (c.isDeleted) None else {
        Some(c.bodyValue)
      }
    } map { existingContentValue: Value ⇒
      val newContentValueOption = if(newContent.isDeleted) None else {
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
                                   request: DynamicRequest,
                                   existingContent: Option[Content],
                                   existingContentStatic: Option[ContentBase]): Transaction = {
    val revision = existingContentStatic match {
      case None ⇒ 1
      case Some(content) ⇒ content.revision + 1
    }

    val recordCount = existingContent.map(_ ⇒ 0 ).getOrElse(1)

    // always preserve null fields in transaction
    // this is required to correctly publish patch events
    implicit val so = SerializationOptions.forceOptionalFields
    val transactionBody = request.copy(
      headers = Headers.builder
        .++=(request.headers)
        .+=(Header.REVISION → Number(revision))
        .+=(Header.COUNT → Number(recordCount))
        .withMethod("feed:" + request.headers.method)
        .requestHeaders()
    ).serializeToString
    TransactionLogic.newTransaction(documentUri, itemId, revision, transactionBody)
  }

  private def updateContent(documentUri: String,
                            itemId: String,
                            newTransaction: Transaction,
                            request: DynamicRequest,
                            existingContent: Option[Content],
                            existingContentStatic: Option[ContentBase]): Content =
    request.headers.method match {
      case Method.PUT ⇒ putContent(documentUri, itemId, newTransaction, request, existingContent, existingContentStatic)
      case Method.PATCH ⇒ patchContent(documentUri, itemId, newTransaction, request, existingContent)
      case Method.DELETE ⇒ deleteContent(documentUri, itemId, newTransaction, request, existingContent, existingContentStatic)
    }

  private def putContent(documentUri: String,
                         itemId: String,
                         newTransaction: Transaction,
                         request: DynamicRequest,
                         existingContent: Option[Content],
                         existingContentStatic: Option[ContentBase]): Content = {
    implicit val mcx = request

    val isCollection = ContentLogic.isCollectionUri(documentUri)
    val newBody =
      if (isCollection && itemId.isEmpty) {
        if (!request.body.content.isEmpty)
          throw Conflict(ErrorBody("collection-put-not-implemented", Some(s"Can't put non-empty collection")))
        else
          None
      }
      else {
        Some(request.body.serializeToString)
      }

    existingContentStatic match {
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
          isDeleted = false,
          count = newCount,
          isView = request.headers.hrl.location == ViewPut.location,
          body = newBody,
          createdAt = existingContent.map(_.createdAt).getOrElse(new Date),
          modifiedAt = existingContent.flatMap(_.modifiedAt)
        )

      case Some(static) ⇒
        if (request.headers.hrl.location == ViewPut.location && !static.isView) {
          throw Conflict(ErrorBody("collection-view-conflict", Some(s"Can't put view over existing collection")))
        }
        val newCount = if (isCollection) {
          static.count.map(_ + (if (existingContent.isEmpty) 1 else 0))
        } else {
          None
        }
        Content(documentUri, itemId, newTransaction.revision,
          transactionList = newTransaction.uuid +: static.transactionList,
          isDeleted = false,
          count = newCount,
          isView = static.isView,
          body = newBody,
          createdAt = existingContent.map(_.createdAt).getOrElse(new Date),
          modifiedAt = existingContent.flatMap(_.modifiedAt)
        )
    }
  }

  private def patchContent(documentUri: String,
                           itemId: String,
                           newTransaction: Transaction,
                           request: DynamicRequest,
                           existingContent: Option[Content]): Content = {
    implicit val mcx = request

    if (ContentLogic.isCollectionUri(documentUri) && itemId.isEmpty) {
      throw Conflict(ErrorBody("collection-patch-not-implemented", Some(s"PATCH is not allowed for a collection~")))
    }

    existingContent match {
      case Some(content) if !content.isDeleted ⇒
        Content(documentUri, itemId, newTransaction.revision,
          transactionList = newTransaction.uuid +: content.transactionList,
          isDeleted = false,
          count = content.count,
          isView = content.isView,
          body = mergeBody(content.bodyValue, request.body.content),
          createdAt = content.createdAt,
          modifiedAt = Some(new Date())
        )

      case _ ⇒
        throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    }
  }

  private def mergeBody(existing: Value, patch: Value): Option[String] = {
    import com.hypertino.binders.json.JsonBinders._
    val newBodyContent = filterNulls(existing + patch)
    newBodyContent match {
      case Null ⇒ None
      case other ⇒ Some(other.toJson)
    }
  }

  def filterNulls(content: Value): Value = {
    content ~~ filterNullsVisitor
  }

  private def deleteContent(documentUri: String,
                            itemId: String,
                            newTransaction: Transaction,
                            request: DynamicRequest,
                            existingContent: Option[Content],
                            existingContentStatic: Option[ContentBase]): Content = existingContentStatic match {
    case None ⇒
      implicit val mcx = request
      throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))

    case Some(content) ⇒
      implicit val mcx = request
      if (content.isView && request.headers.hrl.location != ViewDelete.location && itemId.isEmpty) {
        throw Conflict(ErrorBody("collection-is-view", Some(s"Can't delete view collection directly")))
      }

      Content(documentUri, itemId, newTransaction.revision,
        transactionList = newTransaction.uuid +: content.transactionList,
        isDeleted = true,
        count = content.count.map(_ - 1),
        isView = content.isView,
        body = None,
        createdAt = existingContent.map(_.createdAt).getOrElse(new Date()),
        modifiedAt = Some(new Date())
      )
  }

  private def taskWaitResult(owner: ActorRef, originalTask: PrimaryContentTask, request: DynamicRequest, trackProcessTime: Timer.Context)
                            (implicit mcf: MessagingContext): Receive = {
    case PrimaryWorkerTaskCompleted(task, transaction, created) if task == originalTask ⇒
      if (log.isDebugEnabled) {
        log.debug(s"task $originalTask is completed")
      }
      owner ! BackgroundContentTask(System.currentTimeMillis() + backgroundTaskTimeout.toMillis, transaction.documentUri, expectsResult=false)
      val transactionId = transaction.documentUri + ":" + transaction.uuid + ":" + transaction.revision
      val result: Response[Body] = if (created) {
        Created(HyperStorageTransactionCreated(transactionId,
          path = request.path), location=HRL(TransactionGet.location, Obj.from("transaction_id"→transactionId)))
      }
      else {
        Ok(api.HyperStorageTransaction(transactionId))
      }
      owner ! ShardTaskComplete(task, PrimaryWorkerTaskResult(result.serializeToString))
      trackProcessTime.stop()
      unbecome()

    case PrimaryWorkerTaskFailed(task, e) if task == originalTask ⇒
      owner ! ShardTaskComplete(task, hyperbusException(e, task))
      trackProcessTime.stop()
      unbecome()
  }

  private def hyperbusException(e: Throwable, task: ShardTask)(implicit mcx: MessagingContext): PrimaryWorkerTaskResult = {
    val (response: HyperbusError[ErrorBody], logException) = e match {
      case h: NotFound[ErrorBody] ⇒ (h, false)
      case h: HyperbusError[ErrorBody] ⇒ (h, true)
      case other ⇒ (InternalServerError(ErrorBody("update-failed", Some(e.toString))), true)
    }

    if (logException) {
      log.error(e, s"task $task is failed")
    }

    PrimaryWorkerTaskResult(response.serializeToString)
  }

  private def filterNulls(body: DynamicBody): DynamicBody = {
    body.copy(content = body.content ~~ filterNullsVisitor)
  }

  private def appendId(body: DynamicBody, id: Value, idFieldName: String): DynamicBody = {
    body.copy(content = Obj(body.content.toMap + (idFieldName → id)))
  }

  implicit class RequestWrapper(val request: DynamicRequest) {
    def path: String = request.headers.hrl.query.path.toString
  }
}

object PrimaryWorker {
  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, backgroundTaskTimeout: FiniteDuration) =
    Props(classOf[PrimaryWorker], hyperbus, db, tracker, backgroundTaskTimeout)
}
