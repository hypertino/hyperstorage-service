/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage

import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorRef
import akka.pattern.{AskTimeoutException, ask}
import com.hypertino.binders.value.{Lst, Null, Number, Obj, Text, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model._
import com.hypertino.hyperbus.serialization.{MessageReader, SerializationOptions}
import com.hypertino.hyperbus.subscribe.Subscribable
import com.hypertino.hyperbus.util.IdGenerator
import com.hypertino.hyperstorage.api.{HyperStorageIndexSortItem, _}
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing._
import com.hypertino.hyperstorage.metrics.Metrics
import com.hypertino.hyperstorage.sharding.LocalTask
import com.hypertino.metrics.MetricsTracker
import com.hypertino.parser.ast.{Expression, Identifier}
import com.hypertino.parser.eval.ValueContext
import com.hypertino.parser.{HEval, HFormatter, HParser}
import monix.eval.{Callback, Task}
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.execution.Ack.Continue
import com.hypertino.hyperstorage.utils.{ErrorCode, Sort, SortBy}
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.hyperstorage.workers.primary.{HyperStorageTransactionBase, PrimaryExtra, PrimaryWorkerRequest}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success
import com.hypertino.hyperstorage.utils.TrackerUtils._

// todo: convert Task.fromFuture to tasks...

class HyperbusAdapter(hyperbus: Hyperbus,
                      hyperStorageProcessor: ActorRef,
                      db: Db,
                      tracker: MetricsTracker,
                      requestTimeout: FiniteDuration)
                     (implicit scheduler: Scheduler) extends Subscribable with StrictLogging {

  //final val COLLECTION_FILTER_NAME = "filter"
  //final val COLLECTION_SIZE_FIELD_NAME = "size"
  //final val COLLECTION_SKIP_MAX_FIELD_NAME = "skipMax"
  final val DEFAULT_MAX_SKIPPED_ROWS = 10000
  final val MAX_COLLECTION_SELECTS = 20
  final val DEFAULT_PAGE_SIZE = 100
  implicit val so = SerializationOptions.forceOptionalFields
  private val waitTasks = new ConcurrentHashMap[String, () ⇒ Unit]
  private val fixedGroupName = Some("hs-adapter-" + IdGenerator.create())
  private val subscriptions = hyperbus.subscribe(this, logger)

  def onContentGet(implicit get: ContentGet) = {
    tracker.timeOfTask(Metrics.RETRIEVE_TIME) {
      val resourcePath = ContentLogic.splitPath(get.path)
      if (ContentLogic.isCollectionUri(resourcePath.documentUri) && resourcePath.itemId.isEmpty) {
        queryCollection(resourcePath, get)
      }
      else {
        queryDocument(resourcePath, get)
      }
    }
  }


  def onContentPut(implicit request: ContentPut) = executeRequest(request, request.path)
  def onContentPost(implicit request: ContentPost) = executeRequest(request, request.path)
  def onContentPatch(implicit request: ContentPatch) = executeRequest(request, request.path)
  def onContentDelete(implicit request: ContentDelete) = executeRequest(request, request.path)
  def onIndexPost(implicit request: IndexPost) = executeIndexRequest(request)
  def onIndexDelete(implicit request: IndexDelete) = executeIndexRequest(request)

  def onViewPut(implicit request: ViewPut) = executeRequest(request, request.path)
  def onViewDelete(implicit request: ViewDelete) = executeRequest(request, request.path)

  def onTemplateIndexPut(implicit request: TemplateIndexPut) = Task.eval{
    val templateIndexDef = TemplateIndexDef(
      key = "*",
      indexId=request.indexId,
      templateUri=request.body.templateUri,
      sortBy=IndexLogic.serializeSortByFields(request.body.sortBy),
      filter=request.body.filter,
      materialize=request.body.materialize.getOrElse(false)
    )

    db.insertTemplateIndexDef(templateIndexDef).map { _ ⇒
      Created(HyperStorageTemplateIndexCreated(templateIndexDef.indexId))
    }
  }.flatten

  def onTemplateIndexDelete(implicit request: TemplateIndexDelete) = db.deleteTemplateIndexDef("*", request.indexId).map { _ ⇒
    NoContent(EmptyBody)
  }


  def onContentFeedPut(event: ContentFeedPut): Ack = notifyWaitTask(event)
  def onContentFeedPatch(event: ContentFeedPatch): Ack = notifyWaitTask(event)
  def onContentFeedDelete(event: ContentFeedDelete): Ack = notifyWaitTask(event)

  override def groupName(existing: Option[String]): Option[String] = existing.map(_ + "-" + fixedGroupName).orElse(fixedGroupName)

  // todo: implement onViewGet/onViewsGet/onTemplateIndexesGet

  def off(): Task[Unit] = {
    Task.eval(subscriptions.foreach(_.cancel()))
  }

  private def executeRequest(implicit request: RequestBase, uri: String): Task[ResponseBase] = {
    val ttl = System.currentTimeMillis() + Math.max(requestTimeout.toMillis - 100, 100)
    val documentUri = ContentLogic.splitPath(uri).documentUri
    val task = LocalTask(documentUri, HyperstorageWorkerSettings.PRIMARY, ttl, expectsResult = true, request, Obj.from(PrimaryExtra.INTERNAL_OPERATION → false))

    val primaryTask = Task.fromFuture {
      implicit val timeout: akka.util.Timeout = requestTimeout
      (hyperStorageProcessor ? task).asInstanceOf[Future[Response[HyperStorageTransactionBase]]]
    }

    primaryTask.flatMap { result ⇒
      if ((result.headers.statusCode == Ok.statusCode || result.headers.statusCode == Created.statusCode) &&
        request.headers.get(HyperStorageHeader.HYPER_STORAGE_WAIT).contains(Text("full"))) {
        val transactionId = result.body.transactionId
        waitForTransaction(transactionId).map(_ ⇒ result)
      }
      else {
        Task.now(result)
      }
    }.onErrorRecover(withHyperbusError)
  }

  private def notifyWaitTask(event: RequestBase): Ack = {
    event.headers.get(HyperStorageHeader.HYPER_STORAGE_TRANSACTION).foreach { transaction ⇒
      val task = waitTasks.get(transaction.toString)
      if (task != null)
        task()
    }
    Continue
  }

  private def waitForTransaction(transactionId: String): Task[Unit] = {
    Task.create { (_, callback) ⇒
      waitTasks.put(transactionId, () ⇒ {
        callback(Success())
      })

      new Cancelable {
        override def cancel(): Unit = waitTasks.remove(transactionId)
      }
    }
  }

  private def executeIndexRequest(implicit request: RequestBase): Task[ResponseBase] = {
    val ttl = System.currentTimeMillis() + Math.max(requestTimeout.toMillis - 100, 100)
    val key = request match {
      case post: IndexPost ⇒ post.path
      case delete: IndexDelete ⇒ delete.path
    }
    val indexDefTask = LocalTask(key, HyperstorageWorkerSettings.SECONDARY, ttl, expectsResult = true, request, Obj.from(PrimaryExtra.INTERNAL_OPERATION → false))
    implicit val timeout: akka.util.Timeout = requestTimeout

    Task.fromFuture {
      hyperStorageProcessor ? indexDefTask map {
        case r: ResponseBase ⇒
          r
      }
    }.onErrorRecover(withHyperbusError)
  }

  private def queryCollection(resourcePath: ResourcePath, request: ContentGet): Task[ResponseBase] = {
    implicit val mcx = request
    val notFound = NotFound(ErrorBody(ErrorCode.NOT_FOUND, Some(s"Hyperstorage resource '${request.path}' is not found")))

    val sortBy = Sort.parseQueryParam(request.sortBy)

    val indexDefsTask = if (request.filter.isEmpty && sortBy.isEmpty) {
      Task.now(Iterator.empty)
    } else {
      db.selectIndexDefs(resourcePath.documentUri)
    }

    val pageSize = request.perPage.getOrElse(DEFAULT_PAGE_SIZE)
    val skipMax = request.skipMax.getOrElse(DEFAULT_MAX_SKIPPED_ROWS)
    val idFieldName = ContentLogic.getIdFieldName(resourcePath.documentUri)


    Task.zip2(
      db.selectContentStatic(resourcePath.documentUri),
      indexDefsTask.flatMap { indexDefs ⇒
        if (pageSize > 0) {
          selectCollection(resourcePath.documentUri, indexDefs, request.filter, sortBy, pageSize, skipMax, idFieldName)
        }
        else {
          Task.now{(List.empty, None, None, None)}
        }
      }
    ).map {
      case (contentStatic, (collectionStream, revisionOpt, countOpt, nextPageFieldFilter)) ⇒
      if (contentStatic.isDefined && contentStatic.forall(!_.isDeleted.contains(true))) {
        val result = Lst(collectionStream)
        val revision = if (pageSize == 0) Some(contentStatic.get.revision) else revisionOpt
        val count = if (pageSize == 0) contentStatic.get.count else countOpt

        val nextPageUrl: Option[HRL] = if (nextPageFieldFilter.isDefined) Some {
          ContentGet(path = resourcePath.documentUri,
            sortBy = Some(Sort.generateQueryParam(sortBy)),
            filter = nextPageFieldFilter.map(HFormatter(_)),
            perPage = Some(pageSize)
          ).headers.hrl
        }
        else {
          None
        }

        val hb = MessageHeaders.builder
        revision.foreach { r ⇒
          hb += Header.REVISION → Number(r)
//          hb += HyperStorageHeader.ETAG → Text('"' + r.toHexString + '"')
//          todo: it's very dangerous to have ETAG for the whole collection, because of filtering/sorting
        }

        count.foreach(c ⇒ hb += Header.COUNT → Number(c))
        nextPageUrl.foreach(url ⇒ hb.withLink(Map("next_page_url" → url)))

        Ok(DynamicBody(result), hb.result())
      }
      else {
        notFound
      }
    }
  }

  // todo: refactor this method
  private def selectCollection(documentUri: String,
                               indexDefs: Iterator[IndexDef],
                               queryFilter: Option[String],
                               querySortBy: Seq[SortBy],
                               pageSize: Int,
                               skipMax: Int,
                               idFieldName: String)
                              (implicit messagingContext: MessagingContext): Task[(List[Value], Option[Long], Option[Long], Option[Expression])] = {

    val queryFilterExpression = queryFilter.flatMap(Option.apply).map(HParser(_))
    val defIdSort = HyperStorageIndexSortItem(idFieldName, Some(HyperStorageIndexSortFieldType.TEXT), Some(HyperStorageIndexSortOrder.ASC))

    // todo: this should be cached, heavy operations here
    val sources = indexDefs.flatMap { indexDef ⇒
      if (indexDef.status == IndexDef.STATUS_NORMAL) Some {
        val filterAST = indexDef.filter.map(HParser(_))
        val indexSortBy = indexDef.sortByParsed :+ defIdSort
        val ffe = new FieldFiltersExtractor(idFieldName, indexSortBy)
        val queryFilterFields = queryFilterExpression.map(ffe.extract).getOrElse(Seq.empty)
        (IndexLogic.weighIndex(idFieldName, queryFilterExpression, querySortBy, queryFilterFields, filterAST, indexSortBy), indexSortBy, queryFilterFields, Some(indexDef): Option[IndexDef])
      }
      else {
        None
      }
    }.toSeq :+ {
      val indexSortBy = Seq(defIdSort)
      val ffe = new FieldFiltersExtractor(idFieldName, indexSortBy)
      val queryFilterFields = queryFilterExpression.map(ffe.extract).getOrElse(Seq.empty)
      (IndexLogic.weighIndex(idFieldName, queryFilterExpression, querySortBy, queryFilterFields, None, indexSortBy), indexSortBy, queryFilterFields, None)
    }
    val (weight,indexSortFields,queryFilterFields,indexDefOpt) = sources.reduceLeft((left,right) ⇒ if (left._1 > right._1) left else right)

    // todo: detect filter exact match

    val (ckFields,reversed) = OrderFieldsLogic.extractIndexSortFields(idFieldName, querySortBy, indexSortFields)
    val sortMatchIsExact = ckFields.size == querySortBy.size || querySortBy.isEmpty
    val endOfTime = System.currentTimeMillis + requestTimeout.toMillis

    if (sortMatchIsExact) {
      queryUntilFetched(
        CollectionQueryOptions(documentUri, indexDefOpt, indexSortFields, reversed, pageSize, pageSize, skipMax, endOfTime, queryFilterFields, ckFields, queryFilterExpression, sortMatchIsExact, idFieldName),
        Seq.empty,0,0,None
      )  map { case (list, revisionOpt, countOpt, nextPageFieldFilter) ⇒

        (list.take(pageSize), revisionOpt, countOpt, nextPageFilterExpression(idFieldName, queryFilterExpression, nextPageFieldFilter))
      }
    }
    else {
      queryUntilFetched(
        CollectionQueryOptions(documentUri, indexDefOpt, indexSortFields, reversed, pageSize, pageSize + skipMax, pageSize + skipMax, endOfTime, queryFilterFields, ckFields, queryFilterExpression, sortMatchIsExact, idFieldName),
        Seq.empty,0,0,None
      ) map { case (list, revisionOpt, countOpt, nextPageFieldFilter) ⇒
        if (list.size>=(pageSize+skipMax)) {
          throw GatewayTimeout(ErrorBody(ErrorCode.QUERY_SKIPPED_ROWS_LIMITED, Some(s"Maximum skipped row limit is reached: $skipMax")))
        } else {
          if (querySortBy.nonEmpty) {
            implicit val ordering = new CollectionOrdering(querySortBy)
            (list.sorted.take(pageSize), revisionOpt, countOpt, nextPageFilterExpression(idFieldName, queryFilterExpression, nextPageFieldFilter))
          }
          else
            (list.take(pageSize), revisionOpt, countOpt, nextPageFilterExpression(idFieldName, queryFilterExpression, nextPageFieldFilter))
        }
      }
    }
  }

  private def nextPageFilterExpression(idFieldName: String, queryFilterExpression: Option[Expression], nextPageFieldFilter: Seq[FieldFilter]): Option[Expression] = {
    if (nextPageFieldFilter.isEmpty) {
      None
    }
    else {
      val translated = nextPageFieldFilter.map { f ⇒
        FieldFilter(FieldFiltersExpression.translate(f.name, idFieldName), f.value, f.op)
      }
      FieldFilterMerger.merge(queryFilterExpression, translated)
    }
  }

  private def queryAndFilterRows(ops: CollectionQueryOptions): Task[(List[Value], Int, Int, Option[Obj], Option[Long], Option[Long])] = {

    val f: Task[Iterator[CollectionContent]] = ops.indexDefOpt match {
      case None ⇒
        db.selectContentCollection(ops.documentUri,
          ops.limit,
          ops.filterFields.find(_.name == "item_id").map(ff ⇒ (ff.value.toString, ff.op)),
          ops.ckFields.find(_.name == "item_id").forall(_.ascending)
        )

      case Some(indexDef) ⇒
        val filterFields = if(ops.filterFields.isEmpty) {
          val v = if (ops.indexSortBy.head.fieldType.getOrElse(HyperStorageIndexSortFieldType.TEXT) == HyperStorageIndexSortFieldType.TEXT) {
            Text("")
          } else {
            Number(BigDecimal("1e-100"))
          }
          val fieldName = IndexLogic.tableFieldName(ops.idFieldName, ops.indexSortBy.head, ops.indexSortBy.size, 0)
          Seq(FieldFilter(fieldName, v, FilterGt))
        }
        else {
          ops.filterFields
        }

        val si = db.selectIndexCollection(
          indexDef.tableName,
          ops.documentUri,
          indexDef.indexId,
          filterFields,
          ops.ckFields,
          ops.limit
        )

        if (!indexDef.materialize) {
          si.flatMap { iterator ⇒
            Task.sequence {
              iterator.map { c ⇒ // todo: optimize, we are fetching all page every time, when materialize is true
                db.selectContent(c.documentUri, c.itemId).map {
                  cnt ⇒ cnt.map(_.copy(count=c.count))
                }
              }
            }.map(_.flatten)
          }
        }
        else {
          si
        }
    }

    f.map { iterator ⇒
      var totalFetched = 0
      var totalAccepted = 0
      var lastValue: Option[Obj] = None
      var revision: Option[Long] = None
      var count: Option[Long] = None

      val acceptedStream = iterator.flatMap { c ⇒
        if (!c.itemId.isEmpty) {
          totalFetched += 1
          val optVal = c.bodyValue match {
            case o: Obj ⇒ Some(o)
            case _ ⇒ None
          }

          val accepted = optVal map { o ⇒
            lastValue = Some(o)
            ops.queryFilterExpression.forall { qfe ⇒
              try {
                new HEval(o).eval(qfe).toBoolean
              } catch {
                case e: Throwable ⇒ false
              }
            }
          } getOrElse {
            false
          }

          if (accepted) {
            totalAccepted += 1
            if (revision.isEmpty) {
              revision = Some(c.revision)
            }
            if (count.isEmpty) {
              count = c.count
            }
            optVal
          } else {
            None
          }
        } else {
          // record was returned, however there was no item, only static part
          // sometimes cassandra do this
          totalFetched += 1
          None
        }
      }.toList
      (acceptedStream, totalAccepted, totalFetched, lastValue, revision, count)
    }
  }

  private def queryUntilFetched(ops: CollectionQueryOptions,
                                leastFieldFilter: Seq[FieldFilter],
                                recursionCounter: Int,
                                skippedRows: Int,
                                lastValueOpt: Option[Obj]
                               )
                               (implicit messagingContext: MessagingContext): Task[(List[Value], Option[Long], Option[Long], Seq[FieldFilter])] = {

    //println(s"queryUntilFetched($ops,$leastFieldFilter,$recursionCounter,$skippedRows,$lastValueOpt)")

  //todo exception context
    if (recursionCounter >= MAX_COLLECTION_SELECTS)
      Task.raiseError(GatewayTimeout(ErrorBody(ErrorCode.QUERY_COUNT_LIMITED, Some(s"Maximum query count is reached: $recursionCounter"))))
    else if (ops.endTimeInMillis <= System.currentTimeMillis)
      Task.raiseError(GatewayTimeout(ErrorBody(ErrorCode.QUERY_TIMEOUT, Some(s"Timed out performing query #$recursionCounter"))))
    else if (skippedRows >= ops.skipRowsLimit)
      Task.raiseError(GatewayTimeout(ErrorBody(ErrorCode.QUERY_SKIPPED_ROWS_LIMITED, Some(s"Maximum skipped row limit is reached: $skippedRows"))))
    else {
      val fetchLimit = ops.limit + Math.max((recursionCounter * (ops.skipRowsLimit - ops.limit)/(MAX_COLLECTION_SELECTS*1.0)).toInt, 0)
      queryAndFilterRows(ops.copy(filterFields=IndexLogic.mergeLeastQueryFilterFields(ops.filterFields, leastFieldFilter),limit=fetchLimit)) flatMap {
        case(stream,totalAccepted,totalFetched,newLastValueOpt,revisionOpt,countOpt) ⇒
          val taken = stream.take(ops.limit)
          if (totalAccepted >= ops.limit ||
            ((leastFieldFilter.isEmpty ||
              (leastFieldFilter.size==1 && leastFieldFilter.head.op != FilterEq)) && totalFetched < fetchLimit)) {
            val nextLeastFieldFilter = if (taken.nonEmpty && totalAccepted >= ops.pageSize) {
              val l = taken.reverse.head.asInstanceOf[Obj]
              IndexLogic.leastRowsFilterFields(ops.idFieldName, ops.indexSortBy, ops.filterFields, leastFieldFilter.size, totalFetched < fetchLimit, l, ops.reversed)
            }
            else {
              Seq.empty
            }

            Task.now((stream, revisionOpt, countOpt, nextLeastFieldFilter))
          }
          else {
            val l = newLastValueOpt.orElse(lastValueOpt)
            if (l.isEmpty) Task.now((stream, revisionOpt, countOpt, Seq.empty))
            else {
              val nextLeastFieldFilter = IndexLogic.leastRowsFilterFields(ops.idFieldName, ops.indexSortBy, ops.filterFields, leastFieldFilter.size, totalFetched < fetchLimit, l.get, ops.reversed)
              if (nextLeastFieldFilter.isEmpty) Task.now((stream, revisionOpt, countOpt, nextLeastFieldFilter))
              else {
                queryUntilFetched(ops, nextLeastFieldFilter, recursionCounter + 1, skippedRows + totalFetched - totalAccepted, l) map {
                  case (newStream, newRevisionOpt, newCountOpt, recursiveNextLeastFieldFilter) ⇒
                    (stream ++ newStream, revisionOpt.flatMap(a ⇒ newRevisionOpt.map(b ⇒ Math.min(a, b))), newCountOpt, recursiveNextLeastFieldFilter)
                }
              }
            }
          }
      }
    }
  }

  private def queryDocument(resourcePath: ResourcePath, request: ContentGet): Task[ResponseBase] = {
    implicit val mcx = request
    val notFound = NotFound(ErrorBody(ErrorCode.NOT_FOUND, Some(s"Hyperstorage resource '${request.path}' is not found")))
    db.selectContent(resourcePath.documentUri, resourcePath.itemId) map {
      case None ⇒
        notFound
      case Some(content) ⇒
        val headers = Headers(Header.REVISION → content.revision, HyperStorageHeader.ETAG → Text('"' + content.revision.toHexString + '"'))
        if (!content.isDeleted.contains(true)) {
          if (
            (request.headers.contains(HyperStorageHeader.IF_MATCH) ||
              request.headers.contains(HyperStorageHeader.IF_NONE_MATCH)) &&
              ContentLogic.checkPrecondition(request,Some(content))) {
            NotModified(EmptyBody, headers)
          }
          else {
            val body = DynamicBody(content.bodyValue)
            Ok(body, Headers(Header.REVISION → content.revision, HyperStorageHeader.ETAG → Text('"' + content.revision.toHexString + '"')))
          }
        } else {
          notFound
        }
    }
  }

  private def withHyperbusError(implicit mcx: MessagingContext): PartialFunction[Throwable, ResponseBase] = {
    case h: HyperbusError[ErrorBody]@unchecked ⇒ h
    case t: AskTimeoutException ⇒ GatewayTimeout(ErrorBody(ErrorCode.REQUEST_TIMEOUT, Some(t.toString)))
    case e ⇒ InternalServerError(ErrorBody(ErrorCode.UPDATE_FAILED, Some(e.toString)))
  }
}

case class CollectionQueryOptions(documentUri: String,
                                  indexDefOpt: Option[IndexDef],
                                  indexSortBy: Seq[HyperStorageIndexSortItem],
                                  reversed: Boolean,
                                  pageSize: Int,
                                  limit: Int,
                                  skipRowsLimit: Int,
                                  endTimeInMillis: Long,
                                  filterFields: Seq[FieldFilter],
                                  ckFields: Seq[CkField],
                                  queryFilterExpression: Option[Expression],
                                  exactSortMatch: Boolean,
                                  idFieldName: String
                                 )

class CollectionOrdering(querySortBy: Seq[SortBy]) extends Ordering[Value] {
  private val sortIdentifiersStream = querySortBy.map { sb ⇒
    new HParser(sb.fieldName).Ident.run().get → sb.descending
  }.toStream


  override def compare(x: Value, y: Value): Int = {
    if (querySortBy.isEmpty) throw new UnsupportedOperationException("sort fields are required to compare collection items") else {
      sortIdentifiersStream.map { case (identifier,descending) ⇒
        val xv = extract(x, identifier)
        val yv = extract(y, identifier)
        if (descending)
          cmp(yv,xv)
        else
          cmp(xv,yv)
      }.find(_ != 0).getOrElse(0)
    }
  }

  private def extract(v: Value, identifier: Identifier): Value = {
    val valueContext = v match {
      case obj: Obj ⇒ ValueContext(obj)
      case _ ⇒ ValueContext(Obj.empty)
    }
    valueContext.identifier.applyOrElse(identifier, emptyValue)
  }

  private def emptyValue(i: Identifier) = Null

  private def cmp(x: Value, y: Value): Int = {
    (x,y) match {
      case (Number(xn),Number(yn)) ⇒ xn.compare(yn)
      case (xs,ys) ⇒ xs.toString.compareTo(ys.toString)
    }
  }
}
