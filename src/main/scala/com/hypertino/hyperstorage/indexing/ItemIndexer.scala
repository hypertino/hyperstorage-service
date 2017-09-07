package com.hypertino.hyperstorage.indexing

import akka.event.LoggingAdapter
import com.hypertino.hyperstorage.db.{Content, Db, IndexContent, IndexDef}
import monix.execution.Scheduler

import scala.concurrent.Future
import scala.util.control.NonFatal

trait ItemIndexer {
  def log: LoggingAdapter
  def db: Db
  implicit def scheduler: Scheduler

  def indexItem(indexDef: IndexDef, item: Content, idFieldName: String, countBefore: Long): Future[String] = {
    val contentValue = item.bodyValue
    val sortBy = IndexLogic.extractSortFieldValues(idFieldName, indexDef.sortByParsed, contentValue)

    val write: Boolean = !item.isDeleted.contains(true) && (indexDef.filterBy.map { filterBy ⇒
      try {
        IndexLogic.evaluateFilterExpression(filterBy, contentValue)
      } catch {
        case NonFatal(e) ⇒
          if (log.isDebugEnabled) {
            log.debug(s"Can't evaluate expression: `$filterBy` for $item", e)
          }
          false
      }
    } getOrElse {
      true
    })

    if (log.isDebugEnabled) {
      log.debug(s"Indexing item $item with $indexDef ... ${if (write) "Accepted" else "Rejected"}")
    }

    if (write) {
      val indexContent = IndexContent(
        item.documentUri, indexDef.indexId, item.itemId, item.revision, Some(countBefore+1),
        if (indexDef.materialize) item.body else None,
        item.createdAt, item.modifiedAt
      )
      db.insertIndexItem(indexDef.tableName, sortBy, indexContent) map { _ ⇒
        item.itemId
      }
    }
    else {
      Future.successful {
        item.itemId
      }
    }
  }
}
