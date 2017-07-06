package com.hypertino.hyperstorage

import akka.actor.ActorRef
import akka.pattern.ask
import com.hypertino.binders.value.{Lst, Null, Number, Obj, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model._
import com.hypertino.hyperbus.serialization.MessageReader
import com.hypertino.hyperbus.transport.api.CommandEvent
import com.hypertino.hyperstorage.api.{HyperStorageIndexSortItem, _}
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.{FieldFiltersExtractor, IndexLogic, OrderFieldsLogic}
import com.hypertino.hyperstorage.metrics.Metrics
import com.hypertino.hyperstorage.workers.primary.{PrimaryTask, PrimaryWorkerTaskResult}
import com.hypertino.hyperstorage.workers.secondary.IndexDefTask
import com.hypertino.metrics.MetricsTracker
import com.hypertino.parser.ast.{Expression, Identifier}
import com.hypertino.parser.eval.ValueContext
import com.hypertino.parser.{HEval, HParser}
import monix.eval.Task
import monix.execution.{Ack, Scheduler}
import monix.execution.Ack.Continue
import com.hypertino.hyperstorage.utils.Sort._
import com.hypertino.hyperstorage.utils.{Sort, SortBy}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

// todo: convert Task.fromFuture to tasks...

class HyperbusAdapter(hyperbus: Hyperbus,
                      hyperStorageProcessor: ActorRef,
                      db: Db,
                      tracker: MetricsTracker,
                      requestTimeout: FiniteDuration)
                     (implicit scheduler: Scheduler) {

  //final val COLLECTION_FILTER_NAME = "filter"
  //final val COLLECTION_SIZE_FIELD_NAME = "size"
  //final val COLLECTION_SKIP_MAX_FIELD_NAME = "skipMax"
  final val DEFAULT_MAX_SKIPPED_ROWS = 10000
  final val MAX_COLLECTION_SELECTS = 20
  final val DEFAULT_PAGE_SIZE = 100

  private val subscriptions = Seq(
    hyperbus.commands[ContentGet].subscribe { implicit command ⇒
      val request = command.request
      Task.fromFuture[ResponseBase] {
        tracker.timeOfFuture(Metrics.RETRIEVE_TIME) {
          val resourcePath = ContentLogic.splitPath(request.path)
          val notFound = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
          if (ContentLogic.isCollectionUri(resourcePath.documentUri) && resourcePath.itemId.isEmpty) {
            queryCollection(resourcePath, request)
          }
          else {
            queryDocument(resourcePath, request)
          }
        }
      }.runOnComplete(command.reply)
      Continue
    },

    hyperbus.commands[ContentPut].subscribe { implicit command ⇒
      executeRequest(command, command.request.path)
    },

    hyperbus.commands[ContentPost].subscribe { implicit command ⇒
      executeRequest(command, command.request.path)
    },

    hyperbus.commands[ContentPatch].subscribe { implicit command ⇒
      executeRequest(command, command.request.path)
    },

    hyperbus.commands[ContentDelete].subscribe { implicit command ⇒
      executeRequest(command, command.request.path)
    },

    hyperbus.commands[IndexPost].subscribe { implicit command ⇒
      executeIndexRequest(command)
    },

    hyperbus.commands[IndexDelete].subscribe { implicit command ⇒
      executeIndexRequest(command)
    }
  )

  def off(): Task[Unit] = {
    Task.eval(subscriptions.foreach(_.cancel()))
  }

  private def executeRequest(implicit command: CommandEvent[RequestBase], uri: String): Future[Ack] = {
    val str = command.request.serializeToString
    val ttl = Math.max(requestTimeout.toMillis - 100, 100)
    val documentUri = ContentLogic.splitPath(uri).documentUri
    val task = PrimaryTask(documentUri, System.currentTimeMillis() + ttl, str)
    implicit val timeout: akka.util.Timeout = requestTimeout

    // todo: what happens when error is returned
    Task.fromFuture {
      hyperStorageProcessor ? task map {
        case PrimaryWorkerTaskResult(content) ⇒
          MessageReader.fromString(content, StandardResponse.apply) // todo: we are deserializing just to serialize back here
      }
    } runOnComplete command.reply
    Continue
  }

  private def executeIndexRequest(implicit command: CommandEvent[RequestBase]): Future[Ack] = {
    val ttl = Math.max(requestTimeout.toMillis - 100, 100)
    val key = command.request match {
      case post: IndexPost ⇒ post.path
      case delete: IndexDelete ⇒ delete.path
    }
    val indexDefTask = IndexDefTask(System.currentTimeMillis() + ttl, key, command.request.serializeToString)
    implicit val timeout: akka.util.Timeout = requestTimeout

    Task.fromFuture {
      hyperStorageProcessor ? indexDefTask map {
        case r: ResponseBase ⇒
          r
      }
    } runOnComplete command.reply
    Continue
  }

  private def queryCollection(resourcePath: ResourcePath, request: ContentGet): Future[ResponseBase] = {
    implicit val mcx = request
    val notFound = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))

    val sortBy = Sort.parseQueryParam(request.sortBy)

    val indexDefsFuture = if (request.filter.isEmpty && sortBy.isEmpty) {
      Future.successful(Iterator.empty)
    } else {
      db.selectIndexDefs(resourcePath.documentUri)
    }

    val pageSize = request.size.getOrElse(DEFAULT_PAGE_SIZE)
    val skipMax = request.skipMax.getOrElse(DEFAULT_MAX_SKIPPED_ROWS)

    for {
      contentStatic ← db.selectContentStatic(resourcePath.documentUri)
      indexDefs ← indexDefsFuture
      (collectionStream, revisionOpt) ← selectCollection(resourcePath.documentUri, indexDefs, request.filter, sortBy, pageSize, skipMax)
    } yield {
      if (contentStatic.isDefined && contentStatic.forall(!_.isDeleted)) {
        val result = Obj(Map("_embedded" →
          Obj(Map("els" →
            Lst(collectionStream)
          ))))

        val headersMap: HeadersMap = HeadersMap(
          revisionOpt.map(r ⇒ Seq(Header.REVISION → Number(r))).toSeq.flatten : _*
        )

        Ok(DynamicBody(result), headersMap)
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
                               skipMax: Int)
                              (implicit messagingContext: MessagingContext): Future[(List[Value], Option[Long])] = {

    val queryFilterExpression = queryFilter.flatMap(Option.apply).map(HParser(_))

    val defIdSort = HyperStorageIndexSortItem("id", Some(HyperStorageIndexSortFieldType.TEXT), Some(HyperStorageIndexSortOrder.ASC))

    // todo: this should be cached, heavy operations here
    val sources = indexDefs.flatMap { indexDef ⇒
      if (indexDef.status == IndexDef.STATUS_NORMAL) Some {
        val filterAST = indexDef.filterBy.map(HParser(_))
        val indexSortBy = indexDef.sortByParsed :+ defIdSort
        (IndexLogic.weighIndex(queryFilterExpression, querySortBy, filterAST, indexSortBy), indexSortBy, Some(indexDef))
      }
      else {
        None
      }
    }.toSeq :+
      (IndexLogic.weighIndex(queryFilterExpression, querySortBy, None, Seq(defIdSort)), Seq(defIdSort), None)

    val (weight,indexSortFields,indexDefOpt) = sources.reduceLeft((left,right) ⇒ if (left._1 > right._1) left else right)

    val ffe = new FieldFiltersExtractor(indexSortFields)
    val queryFilterFields = queryFilterExpression.map(ffe.extract).getOrElse(Seq.empty)
    // todo: detect filter exact match

    val (ckFields,reversed) = OrderFieldsLogic.extractIndexSortFields(querySortBy, indexSortFields)
    val sortMatchIsExact = ckFields.size == querySortBy.size || querySortBy.isEmpty
    val endOfTime = System.currentTimeMillis + requestTimeout.toMillis

    if (sortMatchIsExact) {
      queryUntilFetched(
        CollectionQueryOptions(documentUri, indexDefOpt, indexSortFields, reversed, pageSize, skipMax, endOfTime, queryFilterFields, ckFields, queryFilterExpression, sortMatchIsExact),
        Seq.empty,0,0,None
      )  map { case (list, revisionOpt) ⇒
        (list.take(pageSize), revisionOpt)
      }
    }
    else {
      queryUntilFetched(
        CollectionQueryOptions(documentUri, indexDefOpt, indexSortFields, reversed, pageSize + skipMax, pageSize + skipMax, endOfTime, queryFilterFields, ckFields, queryFilterExpression, sortMatchIsExact),
        Seq.empty,0,0,None
      ) map { case (list, revisionOpt) ⇒
        if (list.size==(pageSize+skipMax)) {
          throw GatewayTimeout(ErrorBody("query-skipped-rows-limited", Some(s"Maximum skipped row limit is reached: $skipMax")))
        } else {
          if (querySortBy.nonEmpty) {
            implicit val ordering = new CollectionOrdering(querySortBy)
            (list.sorted.take(pageSize), revisionOpt)
          }
          else
            (list.take(pageSize), revisionOpt)
        }
      }
    }
  }

  private def queryAndFilterRows(ops: CollectionQueryOptions): Future[(List[Value], Int, Int, Option[Obj], Option[Long])] = {

    val f: Future[Iterator[CollectionContent]] = ops.indexDefOpt match {
      case None ⇒
        db.selectContentCollection(ops.documentUri,
          ops.limit,
          ops.filterFields.find(_.name == "item_id").map(ff ⇒ (ff.value.toString, ff.op)),
          ops.ckFields.find(_.name == "item_id").forall(_.ascending)
        )

      case Some(indexDef) ⇒
        db.selectIndexCollection(
          indexDef.tableName,
          ops.documentUri,
          indexDef.indexId,
          ops.filterFields,
          ops.ckFields,
          ops.limit
        )
    }

    f.map { iterator ⇒
      var totalFetched = 0
      var totalAccepted = 0
      var lastValue: Option[Obj] = None
      var revision: Option[Long] = None

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
                case NonFatal(e) ⇒ false
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
      (acceptedStream, totalAccepted, totalFetched, lastValue, revision)
    }
  }

  private def queryUntilFetched(ops: CollectionQueryOptions,
                                leastFieldFilter: Seq[FieldFilter],
                                recursionCounter: Int,
                                skippedRows: Int,
                                lastValueOpt: Option[Obj]
                               )
                               (implicit messagingContext: MessagingContext): Future[(List[Value], Option[Long])] = {

    //println(s"queryUntilFetched($ops,$leastFieldFilter,$recursionCounter,$skippedRows,$lastValueOpt)")

  //todo exception context
    if (recursionCounter >= MAX_COLLECTION_SELECTS)
      Future.failed(GatewayTimeout(ErrorBody("query-count-limited", Some(s"Maximum query count is reached: $recursionCounter"))))
    else if (ops.endTimeInMillis <= System.currentTimeMillis)
      Future.failed(GatewayTimeout(ErrorBody("query-timeout", Some(s"Timed out performing query #$recursionCounter"))))
    else if (skippedRows >= ops.skipRowsLimit)
      Future.failed(GatewayTimeout(ErrorBody("query-skipped-rows-limited", Some(s"Maximum skipped row limit is reached: $skippedRows"))))
    else {
      val fetchLimit = ops.limit + Math.max((recursionCounter * (ops.skipRowsLimit - ops.limit)/(MAX_COLLECTION_SELECTS*1.0)).toInt, 0)
      queryAndFilterRows(ops.copy(filterFields=IndexLogic.mergeLeastQueryFilterFields(ops.filterFields, leastFieldFilter),limit=fetchLimit)) flatMap {
        case(stream,totalAccepted,totalFetched,newLastValueOpt,revisionOpt) ⇒
          val taken = stream.take(ops.limit)
          if (totalAccepted >= ops.limit ||
            ((leastFieldFilter.isEmpty || (leastFieldFilter.size==1 && leastFieldFilter.head.op != FilterEq)) && totalFetched < fetchLimit)) {
            Future.successful((stream, revisionOpt))
          }
          else {
            val l = newLastValueOpt.orElse(lastValueOpt)
            if (l.isEmpty) Future.successful((stream, revisionOpt))
            else {
              val nextLeastFieldFilter = IndexLogic.leastRowsFilterFields(ops.indexSortBy, ops.filterFields, leastFieldFilter.size, totalFetched < fetchLimit, l.get, ops.reversed)
              if (nextLeastFieldFilter.isEmpty) Future.successful((stream, revisionOpt))
              else {
                queryUntilFetched(ops, nextLeastFieldFilter, recursionCounter + 1, skippedRows + totalFetched - totalAccepted, l) map {
                  case (newStream, newRevisionOpt) ⇒
                    (stream ++ newStream, revisionOpt.flatMap(a ⇒ newRevisionOpt.map(b ⇒ Math.min(a, b))))
                }
              }
            }
          }
      }
    }
  }

  private def queryDocument(resourcePath: ResourcePath, request: ContentGet): Future[ResponseBase] = {
    implicit val mcx = request
    val notFound = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    db.selectContent(resourcePath.documentUri, resourcePath.itemId) map {
      case None ⇒
        notFound
      case Some(content) ⇒
        if (!content.isDeleted) {
          val body = DynamicBody(content.bodyValue)
          Ok(body, HeadersMap(Header.REVISION → content.revision))
        } else {
          notFound
        }
    }
  }
}

case class CollectionQueryOptions(documentUri: String,
                                  indexDefOpt: Option[IndexDef],
                                  indexSortBy: Seq[HyperStorageIndexSortItem],
                                  reversed: Boolean,
                                  limit: Int,
                                  skipRowsLimit: Int,
                                  endTimeInMillis: Long,
                                  filterFields: Seq[FieldFilter],
                                  ckFields: Seq[CkField],
                                  queryFilterExpression: Option[Expression],
                                  exactSortMatch: Boolean
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
