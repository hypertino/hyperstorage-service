/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.db

import java.util.{Date, UUID}

import com.hypertino.binders._
import com.hypertino.binders.cassandra._
import com.hypertino.binders.value.{Null, Number, Text, Value}
import com.hypertino.hyperstorage.{CassandraConnector, ContentLogic}
import com.hypertino.hyperstorage.api.HyperStorageIndexSortItem
import com.hypertino.inflector.naming.CamelCaseToSnakeCaseConverter
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

// todo: check if Dynamic's are statments are cached

trait ContentBase {
  def documentUri: String
  def revision: Long
  def transactionList: List[UUID]
  def isDeleted: Option[Boolean]
  def count: Option[Long]
  def isView: Option[Boolean]
}

trait CollectionContent {
  def documentUri: String
  def itemId: String
  def revision: Long
  def count: Option[Long]
  def body: Option[String]
  def createdAt: Date
  def modifiedAt: Option[Date]

  lazy val bodyValue : Value = {
    import com.hypertino.binders.json.JsonBinders._
    body.map(_.parseJson[Value]).getOrElse(Null)
  }
}

case class Content(
                    documentUri: String,
                    itemId: String,
                    revision: Long,
                    transactionList: List[UUID],
                    isDeleted: Option[Boolean],
                    count: Option[Long],
                    isView: Option[Boolean],
                    body: Option[String],
                    createdAt: Date,
                    modifiedAt: Option[Date],
                    ttl: Option[Int],
                    ttlLeft: Option[Int]
                  ) extends ContentBase with CollectionContent {

  // selecting ttl(body) which is ttlLeft returns null just before expiration
  // to handle this we set new ttl to 1 second (minum possible value)
  def realTtl: Int = {
    ttlLeft.getOrElse {
      if (ttl.exists(_ > 0)) { // we're already expired, set to 1 second
        1
      }
      else {
        0
      }
    }
  }
}

case class ContentStatic(
                          documentUri: String,
                          revision: Long,
                          transactionList: List[UUID],
                          isDeleted: Option[Boolean],
                          count: Option[Long],
                          isView: Option[Boolean]
                        ) extends ContentBase

case class Transaction(
                        dtQuantum: Long,
                        partition: Int,
                        documentUri: String,
                        itemId: String,
                        uuid: UUID,
                        revision: Long,
                        body: String,
                        obsoleteIndexItems: Option[String],
                        completedAt: Option[Date]
                      )

case class PendingIndex(
                         partition: Int,
                         documentUri: String,
                         indexId: String,
                         lastItemId: Option[String],
                         defTransactionId: UUID
                       )

trait WithSortBy {
  def sortBy: Option[String]

  lazy val sortByParsed: Seq[HyperStorageIndexSortItem] = sortBy.map{ str ⇒
    import com.hypertino.binders.json.JsonBinders._
    str.parseJson[Seq[HyperStorageIndexSortItem]]
  }.getOrElse(Seq.empty)
}

case class IndexDef(
                     documentUri: String,
                     indexId: String,
                     status: Int,
                     sortBy: Option[String],
                     filter: Option[String],
                     tableName: String,
                     defTransactionId: UUID,
                     materialize: Boolean
                   ) extends WithSortBy


case class IndexContentStatic(
                          documentUri: String,
                          revision: Long,
                          count: Option[Long]
                        ) extends ContentBase {
  override def transactionList = List.empty
  override def isDeleted = None
  override def isView = None
}

case class ViewDef(
                    key: String,
                    documentUri: String,
                    templateUri: String,
                    filter: Option[String]
                  )

case class IndexContent(
                         documentUri: String,
                         indexId: String,
                         itemId: String,
                         revision: Long,
                         count: Option[Long],
                         body: Option[String],
                         createdAt: Date,
                         modifiedAt: Option[Date]
                       ) extends CollectionContent

case class TemplateIndexDef(
                             key: String,
                             indexId: String,
                             templateUri: String,
                             sortBy: Option[String],
                             filter: Option[String],
                             materialize: Boolean
                           ) extends WithSortBy

object IndexDef {
  val STATUS_INDEXING = 0
  val STATUS_DELETING = 1
  val STATUS_NORMAL = 2
}

sealed trait FilterOperator {
  def stringOp: String
  def swap: FilterOperator
}
case object FilterEq extends FilterOperator {
  final val stringOp = "="
  def swap: FilterOperator = FilterEq
}
case object FilterGt extends FilterOperator {
  final val stringOp = ">"
  def swap: FilterOperator = FilterLt
}
case object FilterGtEq extends FilterOperator {
  final val stringOp = ">="
  def swap: FilterOperator = FilterLtEq
}
case object FilterLt extends FilterOperator {
  final val stringOp = "<"
  def swap: FilterOperator = FilterGt
}
case object FilterLtEq extends FilterOperator {
  final val stringOp = "<="
  def swap: FilterOperator = FilterGtEq
}

object FilterOperator {
  def apply(op: String): FilterOperator = {
    op match {
      case FilterGt.`stringOp` ⇒ FilterGt
      case FilterGtEq.`stringOp` ⇒ FilterGtEq
      case FilterLt.`stringOp` ⇒ FilterLt
      case FilterLtEq.`stringOp` ⇒ FilterLtEq
      case FilterEq.`stringOp` ⇒ FilterEq
    }
  }
}

case class CkField(name: String, ascending: Boolean)
case class FieldFilter(name: String, value: Value, op: FilterOperator)

private[db] case class CheckPoint(lastQuantum: Long)
private[db] case class IndexItemId(itemId: String)
private[db] case class ContentTtl(ttl: Int)

class Db(connector: CassandraConnector)(implicit scheduler: Scheduler) extends StrictLogging {
  private[this] lazy val session: com.datastax.driver.core.Session = connector.connect()
  private[this] lazy implicit val sessionQueryCache = new GuavaSessionQueryCache[CamelCaseToSnakeCaseConverter.type](session)

  def preStart(): Unit = {
    session
  }

  def close() = {
    try {
      val cluster = session.getCluster
      session.close()
      cluster.close()
    }
    catch {
      case e: Throwable ⇒
        logger.error(s"Can't close C* session", e)
    }
  }

  def selectContent(documentUri: String, itemId: String): Task[Option[Content]] = cql"""
      select document_uri,item_id,revision,transaction_list,is_deleted,count,is_view,body,created_at,modified_at,ttl,ttl(body) as ttl_left from content
      where document_uri=$documentUri and item_id=$itemId
    """.oneOption[Content]

  def selectContentCollection(documentUri: String, limit: Int, itemIdFilter: Option[(String, FilterOperator)], ascending: Boolean = true): Task[Iterator[Content]] = {
    //println(s"selectContentCollection($documentUri,$limit,$itemIdFilter,$ascending)")
    val orderClause = if(ascending) {
      Dynamic("order by item_id asc")
    }
    else {
      Dynamic("order by item_id desc")
    }
    val itemIdFilterDynamic = Dynamic(itemIdFilter.map {
      case (_, FilterEq) ⇒ s"and item_id = ?"
      case (_, FilterGt) ⇒ s"and item_id > ?"
      case (_, FilterGtEq) ⇒ s"and item_id >= ?"
      case (_, FilterLt) ⇒ s"and item_id < ?"
      case (_, FilterLtEq) ⇒ s"and item_id <= ?"
    }.getOrElse(""))

    val c = cql"""
      select document_uri,item_id,revision,transaction_list,is_deleted,count,is_view,body,created_at,modified_at,ttl,ttl(body) as ttl_left from content
      where document_uri=? $itemIdFilterDynamic
      $orderClause
      limit ?
    """
    if (itemIdFilter.isDefined) {
      c.bindArgs(documentUri, itemIdFilter.get._1, limit)
    }
    else {
      c.bindArgs(documentUri, limit)
    }
    c.stmt.task.map(rows => rows.iterator.flatMap{it ⇒
      if(it.row.isNull("item_id")) None else Some(it.unbind[Content])
    })
  }

  def selectContentStatic(documentUri: String): Task[Option[ContentStatic]] = cql"""
      select document_uri,revision,transaction_list,is_deleted,count,is_view from content
      where document_uri=$documentUri
      limit 1
    """.oneOption[ContentStatic]

  def insertContent(content: Content): Task[Any] = {
    logger.debug(s"Inserting: $content")

    val staticFields = Seq("revision", "transaction_list") ++
      content.isView.map(_ ⇒ "is_view") ++
      content.isDeleted.map(_ ⇒ "is_deleted") ++
      content.count.map(_ ⇒ "count")

    val nonStaticfields = Seq("item_id", "body", "created_at") ++
      content.modifiedAt.map(_ ⇒ "modified_at") ++
      content.ttl.map(_ ⇒ "ttl")

    val ttlValue = Dynamic(content.realTtl.toString)

    val cql = if (content.realTtl > 0 && content.itemId.length > 0) {
      val nsFields = Seq("document_uri") ++ nonStaticfields
      val nsFieldNames = nsFields mkString ","
      val nsQs = nsFields.map(_ ⇒ "?") mkString ","
      val sFields = Seq("document_uri") ++ staticFields
      val sFieldNames = sFields mkString ","
      val sQs = sFields.map(_ ⇒ "?") mkString ","

      cql"""
        begin batch
          insert into content(${Dynamic(sFieldNames)})
          values( ${Dynamic(sQs)} );
          insert into content(${Dynamic(nsFieldNames)})
          values( ${Dynamic(nsQs)} )
          using ttl $ttlValue;
        apply batch;
      """
    }
    else
    {
      val fields = Seq("document_uri") ++ staticFields ++ nonStaticfields
      val fieldNames = fields mkString ","
      val qs = fields.map(_ ⇒ "?") mkString ","
      cql"""
        insert into content( ${Dynamic(fieldNames)})
        values( ${Dynamic(qs)} )
        using ttl $ttlValue
      """
    }
    cql.bindPartial(content).task
  }

  def deleteContentItem(content: ContentBase, itemId: String): Task[Any] = {
    logger.debug(s"Deleting: $content")
    cql"""
      begin batch
        update content
        set transaction_list = ${content.transactionList}, revision = ${content.revision}, count = ${content.count}
        where document_uri = ${content.documentUri};
        delete from content
        where document_uri = ${content.documentUri} and item_id = $itemId;
      apply batch;
    """.task
  }

  def purgeCollection(documentUri: String): Task[Any] = {
    logger.debug(s"Purging collection: $documentUri")
    cql"""
      delete from content
      where document_uri = $documentUri;
    """.task
  }

  def selectTransaction(dtQuantum: Long, partition: Int, documentUri: String, uuid: UUID): Task[Option[Transaction]] = cql"""
      select dt_quantum,partition,document_uri,item_id,uuid,revision,body,obsolete_index_items,completed_at from transaction
      where dt_quantum=$dtQuantum and partition=$partition and document_uri=$documentUri and uuid=$uuid
    """.oneOption[Transaction]

  def selectPartitionTransactions(dtQuantum: Long, partition: Int): Task[Iterator[Transaction]] = cql"""
      select dt_quantum,partition,document_uri,item_id,uuid,revision,body,obsolete_index_items,completed_at from transaction
      where dt_quantum=$dtQuantum and partition=$partition
    """.all[Transaction]

  def insertTransaction(transaction: Transaction): Task[Any] = cql"""
      insert into transaction(dt_quantum,partition,document_uri,item_id,uuid,revision,body,obsolete_index_items,completed_at)
      values(?,?,?,?,?,?,?,?,?)
    """.bind(transaction).task

  def completeTransaction(transaction: Transaction): Task[Any] = cql"""
      update transaction set completed_at=toTimestamp(now())
      where dt_quantum=${transaction.dtQuantum}
        and partition=${transaction.partition}
        and document_uri=${transaction.documentUri}
        and uuid=${transaction.uuid}
    """.task

  def deleteTransaction(transaction: Transaction): Task[Any] = cql"""
      delete transaction
      where dt_quantum=${transaction.dtQuantum}
        and partition=${transaction.partition}
        and document_uri=${transaction.documentUri}
        and uuid=${transaction.uuid}
    """.task

  def removeCompleteTransactionsFromList(documentUri: String, transactions: List[UUID]) = cql"""
      update content
        set transaction_list = transaction_list - $transactions
      where document_uri = $documentUri
    """.task

  def selectCheckpoint(partition: Int): Task[Option[Long]] = cql"""
      select last_quantum from checkpoint where partition = $partition
    """.oneOption[CheckPoint].map(_.map(_.lastQuantum))

  def updateCheckpoint(partition: Int, lastQuantum: Long): Task[Any] = cql"""
      insert into checkpoint(partition, last_quantum) values($partition, $lastQuantum)
    """.task

  def selectPendingIndexes(partition: Int, limit: Int): Task[Iterator[PendingIndex]] = cql"""
      select partition, document_uri, index_id, last_item_id, def_transaction_id
      from pending_index
      where partition=$partition
      limit $limit
    """.all[PendingIndex]

  def selectPendingIndex(partition: Int, documentId: String, indexId: String, defTransactionId: UUID): Task[Option[PendingIndex]] = cql"""
      select partition, document_uri, index_id, last_item_id, def_transaction_id
      from pending_index
      where partition=$partition and document_uri=$documentId and index_id=$indexId and def_transaction_id=$defTransactionId
    """.oneOption[PendingIndex]

  def deletePendingIndex(partition: Int, documentId: String, indexId: String, defTransactionId: UUID) = cql"""
      delete
      from pending_index
      where partition=$partition and document_uri=$documentId and index_id=$indexId and def_transaction_id=$defTransactionId
    """.task

  def updatePendingIndexLastItemId(partition: Int, documentId: String, indexId: String, defTransactionId: UUID, lastItemId: String) = cql"""
      update pending_index
      set last_item_id = $lastItemId
      where partition=$partition and document_uri=$documentId and index_id=$indexId and def_transaction_id=$defTransactionId
    """.task

  def insertPendingIndex(pendingIndex: PendingIndex): Task[Any] = cql"""
      insert into pending_index(partition, document_uri, index_id, last_item_id, def_transaction_id)
      values (?,?,?,?,?)
    """.bind(pendingIndex).task

  def selectIndexDef(documentUri: String, indexId: String): Task[Option[IndexDef]] = cql"""
      select document_uri, index_id, status, sort_by, filter, table_name, def_transaction_id, materialize
      from index_def
      where document_uri = $documentUri and index_id=$indexId
    """.oneOption[IndexDef]

  def selectIndexDefs(documentUri: String): Task[Iterator[IndexDef]] = cql"""
      select document_uri, index_id, status, sort_by, filter, table_name, def_transaction_id, materialize
      from index_def
      where document_uri = $documentUri
    """.all[IndexDef]

  def insertIndexDef(indexDef: IndexDef): Task[Any] = cql"""
      insert into index_def(document_uri, index_id, status, sort_by, filter, table_name, def_transaction_id, materialize)
      values (?,?,?,?,?,?,?,?)
    """.bind(indexDef).task

  def updateIndexDefStatus(documentUri: String, indexId: String, newStatus: Int, defTransactionId: UUID): Task[Any] = cql"""
      update index_def
      set status = $newStatus, def_transaction_id = $defTransactionId
      where document_uri = $documentUri and index_id = $indexId
    """.task

  def deleteIndexDef(documentUri: String, indexId: String): Task[Any] = cql"""
      delete from index_def
      where document_uri = $documentUri and index_id = $indexId
    """.task

  def selectIndexContentStatic(indexTable: String, documentUri: String, indexId: String): Task[Option[IndexContentStatic]] = {
    val tableName = Dynamic(indexTable)
    cql"""
      select document_uri,revision,count from $tableName
      where document_uri=$documentUri and index_id=$indexId
    """.oneOption[IndexContentStatic]
  }

  def insertIndexItem(indexTable: String, sortFields: Seq[(String, Value)], indexContent: IndexContent, ttl: Int): Task[Any] = {
    logger.debug(s"Inserting indexed: $indexTable, $sortFields, $ttl, $indexContent")
    val tableName = Dynamic(indexTable)
    val sortFieldNames = if (sortFields.isEmpty) Dynamic("") else Dynamic(sortFields.map(_._1).mkString(",", ",", ""))
    val sortFieldPlaces = if (sortFields.isEmpty) Dynamic("") else Dynamic(sortFields.map(_ ⇒ "?").mkString(",", ",", ""))
    val ttlValue = Dynamic(ttl.toString)

    val cql =
    if (ttl > 0) {
      cql"""
        begin batch
          insert into $tableName(document_uri,index_id,revision,count)
          values(?,?,?,?);
          insert into $tableName(document_uri,index_id,item_id,body,created_at,modified_at$sortFieldNames)
          values(?,?,?,?,?,?$sortFieldPlaces)
          using ttl $ttlValue;
        apply batch;
      """.bindPartial(indexContent)
    }
    else {
      cql"""
        insert into $tableName(document_uri,index_id,item_id,revision,count,body,created_at,modified_at$sortFieldNames)
        values(?,?,?,?,?,?,?,?$sortFieldPlaces)
        using ttl $ttlValue
      """.bindPartial(indexContent)
    }

    bindSortFields(cql, sortFields)
    cql.task
  }

  def selectIndexCollection(indexTable: String, documentUri: String, indexId: String,
                            filter: Seq[FieldFilter],
                            orderByFields: Seq[CkField],
                            limit: Int): Task[Iterator[IndexContent]] = {
    val tableName = Dynamic(indexTable)
    val filterEqualFields = if (filter.isEmpty)
      Dynamic("")
    else
      Dynamic {
        filter.map {
          case FieldFilter(name, _, FilterEq) ⇒ s"$name = ?"
          case FieldFilter(name, _, FilterGt) ⇒ s"$name > ?"
          case FieldFilter(name, _, FilterGtEq) ⇒ s"$name >= ?"
          case FieldFilter(name, _, FilterLt) ⇒ s"$name < ?"
          case FieldFilter(name, _, FilterLtEq) ⇒ s"$name <= ?"
        } mkString(" and ", " and ", "")
      }

    val orderByDynamic = if (orderByFields.isEmpty)
      Dynamic("")
    else
      Dynamic(orderByFields.map {
        case CkField(name, true) ⇒ s"$name asc"
        case CkField(name, false) ⇒ s"$name desc"
      } mkString ("order by ", ",", ""))

    val c = cql"""
      select document_uri,index_id,item_id,revision,count,body,created_at,modified_at from $tableName
      where document_uri=? and index_id=?$filterEqualFields
      $orderByDynamic
      limit ?
    """

    c.bindArgs(documentUri, indexId)
    filter foreach {
      case FieldFilter(name, Text(s), _) ⇒ c.bindArgs(s)
      case FieldFilter(name, Number(n), _) ⇒ c.bindArgs(n)
      case FieldFilter(name, other, _) ⇒ throw new IllegalArgumentException(s"Can't bind $name value $other") // todo: do something
    }

    c.bindArgs(limit)
    c.all[IndexContent]
  }

  def deleteIndexItem(indexTable: String,
                      documentUri: String,
                      indexId: String,
                      itemId: String,
                      sortFields: Seq[(String,Value)]): Task[Long] = {
    logger.debug(s"Deleting indexed: $indexTable, $sortFields, $indexId, $documentUri, $itemId")

    val tableName = Dynamic(indexTable)

    val sortFieldsFilter = Dynamic(sortFields map { case (name, _) ⇒
        s"$name = ?"
      } mkString(if (sortFields.isEmpty) "" else " and ", " and ", "")
    )

    val cqlCount = cql"""
      select item_id from $tableName
      where document_uri=$documentUri and index_id=$indexId and item_id=$itemId$sortFieldsFilter
    """
    bindSortFields(cqlCount, sortFields)
    cqlCount.all[IndexItemId].flatMap { iterator ⇒
      val deletedCount = iterator.count(i ⇒ Option(i.itemId).exists(_.nonEmpty)).toLong
      val cql = cql"""
        delete from $tableName
        where document_uri=$documentUri and index_id=$indexId and item_id = $itemId$sortFieldsFilter
      """
      bindSortFields(cql, sortFields)
      cql.task.map { _ ⇒
        deletedCount
      }
    }
  }

  def deleteIndex(indexTable: String, documentUri: String, indexId: String): Task[Any] = {
    val tableName = Dynamic(indexTable)
    cql"""
      delete from $tableName
      where document_uri = $documentUri and index_id=$indexId
    """.task
  }

  def updateIndexRevisionAndCount(indexTable: String, documentUri: String, indexId: String, revision: Long, count: Long): Task[Any] = {
    logger.debug(s"Update index revision: $indexTable, $indexId, $documentUri, rev $revision, count $count")
    val tableName = Dynamic(indexTable)
    cql"""
      update $tableName
      set revision = $revision, count = $count
      where document_uri = $documentUri and index_id = $indexId
    """.task
  }

  def selectViewDefs(key: String = "*"): Task[Iterator[ViewDef]] = cql"""
      select key, document_uri, template_uri, filter
      from view_def
      where key = $key
    """.all[ViewDef]

  def insertViewDef(viewDef: ViewDef): Task[Any] = cql"""
      insert into view_def(key, document_uri, template_uri, filter) values (?,?,?,?)
    """.bind(viewDef).task

  def deleteViewDef(key: String, documentUri: String): Task[Any] = cql"""
      delete from view_def where key=$key and document_uri=$documentUri
    """.task

  def selectTemplateIndexDefs(key: String = "*"): Task[Iterator[TemplateIndexDef]] = cql"""
      select key, index_id, template_uri, sort_by, filter, materialize
      from template_index_def
      where key = $key
    """.all[TemplateIndexDef]

  def insertTemplateIndexDef(templateIndexDef: TemplateIndexDef): Task[Any] = cql"""
      insert into template_index_def(key, index_id, template_uri, sort_by, filter, materialize) values (?,?,?,?,?,?)
    """.bind(templateIndexDef).task

  def deleteTemplateIndexDef(key: String, indexId: String): Task[Any] = cql"""
      delete from template_index_def where key=$key and index_id=$indexId
    """.task

  private def bindSortFields(cql: Statement[CamelCaseToSnakeCaseConverter.type], sortFields: Seq[(String, Value)]) = {
    sortFields.foreach {
      case (name, Text(s)) ⇒ cql.boundStatement.setString(name, s)
      case (name, Number(n)) ⇒ cql.boundStatement.setDecimal(name, n.bigDecimal)
      case (name, v) ⇒ throw new IllegalArgumentException(s"Can't bind $name value $v") // todo: do something
    }
  }
}

