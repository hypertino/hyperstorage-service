/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.workers.secondary

import akka.actor.ActorRef
import akka.pattern.ask
import com.datastax.driver.core.utils.UUIDs
import com.hypertino.binders.value.{Null, Number, Obj, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{DynamicRequest, _}
import com.hypertino.hyperbus.serialization.MessageReader
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.db.{Transaction, _}
import com.hypertino.hyperstorage.indexing.{IndexLogic, ItemIndexer}
import com.hypertino.hyperstorage.internal.api.{BackgroundContentTaskResult, BackgroundContentTasksPost}
import com.hypertino.hyperstorage.metrics.Metrics
import com.hypertino.hyperstorage.sharding.{LocalTask, WorkerTaskResult}
import com.hypertino.hyperstorage.utils.{ErrorCode, FutureUtils}
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.hyperstorage.workers.primary.PrimaryExtra
import com.hypertino.hyperstorage.{ResourcePath, _}
import com.hypertino.metrics.MetricsTracker
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicBoolean

import scala.concurrent.duration
import scala.concurrent.duration.Duration

trait BackgroundContentTaskCompleter extends ItemIndexer with SecondaryWorkerBase {
  def hyperbus: Hyperbus
  def db: Db
  def tracker: MetricsTracker
  implicit def scheduler: Scheduler

  def deleteIndexDefAndData(indexDef: IndexDef): Task[Any]

  def executeBackgroundTask(owner: ActorRef, task: LocalTask, request: BackgroundContentTasksPost): Task[ResponseBase] = {
    implicit val mcx = request
    val ResourcePath(documentUri, itemId) = ContentLogic.splitPath(request.body.documentUri)
    if (!itemId.isEmpty) {
      Task.now(BadRequest(ErrorBody(ErrorCode.BACKGROUND_TASK_FAILED, Some(s"Background task path ${request.body.documentUri} doesn't correspond to $documentUri"))))
    }
    else {
      import com.hypertino.hyperstorage.utils.TrackerUtils._
      tracker.timeOfTask(Metrics.SECONDARY_PROCESS_TIME) {
        db.selectContentStatic(documentUri).flatMap {
          case None ⇒
            logger.warn(s"Didn't found resource to background complete, dismissing task: $request")
            Task.now(NotFound(ErrorBody(ErrorCode.NOT_FOUND, Some(s"$documentUri is not found"))))

          case Some(content) ⇒
            logger.debug(s"Background task for $content")
            completeTransactions(task, request, content, owner)
        }
      }
    }
  }

  // todo: split & refactor method
  private def completeTransactions(task: LocalTask,
                                   request: BackgroundContentTasksPost,
                                   content: ContentStatic,
                                   owner: ActorRef)(implicit mcx: MessagingContext): Task[ResponseBase] = {
    if (content.transactionList.isEmpty) {
      Task.now(Ok(BackgroundContentTaskResult(request.body.documentUri, Seq.empty)))
    }
    else {
      selectIncompleteTransactions(content).flatMap { incompleteTransactions ⇒
        val updateIndexTask: Task[Any] = updateIndexes(content, incompleteTransactions, owner, task.ttl)

        updateIndexTask.flatMap { _ ⇒
          Task.sequence{
            incompleteTransactions.map { it ⇒
              val event = it.unwrappedBody
              val viewTask: Task[Any] = {
                event.headers.get(TransactionLogic.HB_HEADER_VIEW_TEMPLATE_URI).map { templateUri ⇒
                  val filter = event.headers.getOrElse(TransactionLogic.HB_HEADER_FILTER, Null) match {
                    case Null ⇒ None
                    case other ⇒ Some(other.toString)
                  }
                  db.insertViewDef(ViewDef(key = "*", request.body.documentUri, templateUri.toString, filter))
                } getOrElse {
                  if (event.headers.hrl.location == ViewDelete.location) {
                    db.deleteViewDef(key = "*", request.body.documentUri)
                  }
                  else {
                    Task.unit
                  }
                }
              }

              viewTask
                .flatMap(_ ⇒ hyperbus.publish(event))
                .flatMap { publishResult ⇒
                  logger.debug(s"Event $event is published with result $publishResult")

                  db.completeTransaction(it.transaction) map { _ ⇒
                    logger.debug(s"${it.transaction} is complete")

                    it.transaction
                  }
                }
            }
          }
        } flatMap { updatedTransactions ⇒
          logger.debug(s"Removing completed transactions $updatedTransactions from ${request.body.documentUri}")
          db.removeCompleteTransactionsFromList(request.body.documentUri, updatedTransactions.map(_.uuid).toList) onErrorRecover {
            case e: Throwable ⇒
              logger.error(s"Can't remove complete transactions $updatedTransactions from ${request.body.documentUri}", e)
          } map { _ ⇒
            Ok(BackgroundContentTaskResult(request.body.documentUri, updatedTransactions.map(_.uuid.toString)))
          }
        }
      }
    }
  }

  private def selectIncompleteTransactions(content: ContentStatic): Task[Seq[UnwrappedTransaction]] = {
    import ContentLogic._
    val reachedCompleteTransaction = AtomicBoolean(false)
    Task.sequence {
      content.transactionList.map { transactionUuid ⇒
        if (reachedCompleteTransaction.get) {
          Task.now(None)
        }
        else {
          val quantum = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(transactionUuid))
          db.selectTransaction(quantum, content.partition, content.documentUri, transactionUuid) map {
            case Some(transaction) ⇒ Some(UnwrappedTransaction(transaction))
            case None ⇒
              reachedCompleteTransaction.set(true)
              None
          }
        }
      }
    } map {
      _.flatten.reverse
    }
//
//    val transactionsFStream = content.transactionList.toStream.map { transactionUuid ⇒
//      val quantum = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(transactionUuid))
//      db.selectTransaction(quantum, content.partition, content.documentUri, transactionUuid)
//    }
//    FutureUtils.collectWhile(transactionsFStream) {
//      case Some(transaction) ⇒ UnwrappedTransaction(transaction)
//    } map (_.reverse)
  }

  private def updateIndexes(contentStatic: ContentStatic,
                            incompleteTransactions: Seq[UnwrappedTransaction],
                            owner: ActorRef,
                            ttl: Long): Task[Any] = {
    if (ContentLogic.isCollectionUri(contentStatic.documentUri)) {
      val idFieldName = ContentLogic.getIdFieldName(contentStatic.documentUri)
      val isCollectionDelete = incompleteTransactions.exists { it ⇒
        it.transaction.itemId.isEmpty && it.unwrappedBody.headers.method == Method.FEED_DELETE
      }

      // todo: cache index meta
      val indexDefsTask = createIndexFromTemplateOrLoad(contentStatic.documentUri).memoize

      if (isCollectionDelete) {
        // todo: what if after collection delete, there is a transaction with insert?
        // todo: delete view if collection is deleted
        // todo: cache index meta
        indexDefsTask
          .flatMap { indexDefs ⇒
            Task.wander(indexDefs) { indexDef ⇒
              logger.debug(s"Removing index $indexDef")
              deleteIndexDefAndData(indexDef)
            }
          }
      }
      else {
        // todo: refactor, this is crazy
        val itemIds = incompleteTransactions.collect {
          case it if it.transaction.itemId.nonEmpty ⇒
            import com.hypertino.binders.json.JsonBinders._
            val obsoleteMap = it.transaction.obsoleteIndexItems.map(_.parseJson[Map[String, Map[String, Value]]])
            val obsoleteSeq = obsoleteMap.map(_.map(kv ⇒ kv._1 → kv._2.toSeq).toSeq).getOrElse(Seq.empty)
            it.transaction.itemId → obsoleteSeq
        }.groupBy(_._1).mapValues(_.map(_._2)).map(kv ⇒ kv._1 → kv._2.flatten)

        Task.traverse(itemIds.keys) { itemId ⇒
          // todo: cache content
          val contentTask = Task.eval {
            logger.debug(s"Looking for content ${contentStatic.documentUri}/$itemId to index/update view")
            db.selectContent(contentStatic.documentUri, itemId)
          }.flatten.memoize
          val lastTransaction = incompleteTransactions.filter(_.transaction.itemId == itemId).last
          logger.debug(s"Update view/index for ${contentStatic.documentUri}/$itemId, lastTransaction=$lastTransaction, contentTask=$contentTask")

          updateView(contentStatic.documentUri + "/" + itemId, contentTask, lastTransaction, owner, ttl)
            .flatMap { _ ⇒
              indexDefsTask
                .flatMap { indexDefs ⇒
                  Task.traverse(indexDefs) { indexDef ⇒
                    logger.debug(s"Indexing content ${contentStatic.documentUri}/$itemId for $indexDef")

                    db.selectIndexContentStatic(indexDef.tableName, indexDef.documentUri, indexDef.indexId)
                      .flatMap { indexContentStaticO ⇒
                        logger.debug(s"Index $indexDef static data: $indexContentStaticO")
                        val countBefore: Long = indexContentStaticO.flatMap(_.count).getOrElse(0l)
                        // todo: refactor, this is crazy
                        val seq: Seq[Seq[(String, Value)]] = itemIds(itemId).filter(_._1 == indexDef.indexId).map(_._2)
                        val deleteObsoleteTask = Task.sequence {
                          seq.map { s ⇒
                            db.deleteIndexItem(indexDef.tableName, indexDef.documentUri, indexDef.indexId, itemId, s)
                          }
                        }.map { deleted: Seq[Long] ⇒
                          Math.max(countBefore - deleted.sum, 0)
                        }.memoize

                        contentTask
                          .flatMap {
                            case Some(item) if !item.isDeleted.contains(true) ⇒
                              deleteObsoleteTask.flatMap { countAfterDelete ⇒
                                val count = if (indexDef.status == IndexDef.STATUS_NORMAL) Some(countAfterDelete + 1) else None
                                indexItem(indexDef, item, idFieldName, count).map(_._2)
                              }

                            case _ ⇒
                              logger.debug(s"No content to index for ${contentStatic.documentUri}/$itemId")
                              Task.now(false)
                          }
                          .flatMap {
                            case true ⇒
                              Task.now(Unit)

                            case false ⇒
                              deleteObsoleteTask.flatMap { countAfterDelete ⇒
                                val revision = incompleteTransactions.map(t ⇒ t.transaction.revision).max
                                db.updateIndexRevisionAndCount(indexDef.tableName, indexDef.documentUri, indexDef.indexId, revision, countAfterDelete)
                              }
                          }
                      }
                  }
                }
            }
        }
      }
    } else {
      val contentTask = db.selectContent(contentStatic.documentUri, "").memoize
      updateView(contentStatic.documentUri,contentTask,incompleteTransactions.last, owner, ttl)
    }
  }

  private def createIndexFromTemplateOrLoad(documentUri: String): Task[List[IndexDef]] = {
    Task.zip2(
      db.selectIndexDefs(documentUri).map(_.toList),
      db.selectTemplateIndexDefs().map(_.toList)
    ).flatMap { case (existingIndexes, templateDefs) ⇒
      val newIndexTasks = templateDefs.filterNot(t ⇒ existingIndexes.exists(_.indexId == t.indexId)).map { templateDef ⇒
        ContentLogic.pathAndTemplateToId(documentUri, templateDef.templateUri).map { _ ⇒
          insertIndexDef(documentUri, templateDef.indexId, templateDef.sortByParsed, templateDef.filter, templateDef.materialize)
            .map(Some(_))
        } getOrElse {
          Task.now(None) // todo: cache that this document doesn't match to template
        }
      }
      Task.gatherUnordered(newIndexTasks).map { newIndexes ⇒
        existingIndexes ++ newIndexes.flatten
      }
    }
  }

  private def updateView(path: String,
                         contentTask: Task[Option[Content]],
                         lastTransaction: UnwrappedTransaction,
                         owner: ActorRef,
                         ttl: Long): Task[Any] = {
    val viewDefsTask = db.selectViewDefs().map(_.toList)
    viewDefsTask.flatMap { viewDefs ⇒
      Task.wander(viewDefs) { viewDef ⇒
        ContentLogic.pathAndTemplateToId(path, viewDef.templateUri).map { id ⇒
          implicit val mcx = lastTransaction.unwrappedBody
          if (lastTransaction.unwrappedBody.headers.method == Method.FEED_DELETE) {
            deleteViewItem(viewDef.documentUri, id, owner, ttl)
          }
          else {
            contentTask.flatMap {
              case Some(content) ⇒
                val matches = viewDef.filter.map { filter ⇒
                  try {
                    IndexLogic.evaluateFilterExpression(filter, content.bodyValue)
                  } catch {
                    case e: Throwable ⇒
                      logger.debug(s"Can't evaluate expression: `$filter` for $path", e)
                      false
                  }
                } getOrElse {
                  true
                }
                if (matches) {
                  addViewItem(viewDef.documentUri, id, content, owner, ttl)
                } else {
                  deleteViewItem(viewDef.documentUri, id, owner, ttl)
                }
              case None ⇒ // удалить
                deleteViewItem(viewDef.documentUri, id, owner, ttl)
            }
          }
        } getOrElse {
          Task.unit // todo: cache that this document doesn't match to template
        }
      }
    }
  }


  private def addViewItem(documentUri: String, itemId: String, content: Content, owner: ActorRef, ttl: Long)
                         (implicit mcx: MessagingContext): Task[Any] = {
    implicit val timeout = akka.util.Timeout(Duration(ttl - System.currentTimeMillis() + 3000, duration.MILLISECONDS))

    val contentTtl = content.realTtl
    val headers = if (contentTtl>0) {
      Headers(HyperStorageHeader.HYPER_STORAGE_TTL → Number(contentTtl))
    } else {
      Headers.empty
    }

    val task = LocalTask(documentUri, HyperstorageWorkerSettings.PRIMARY, ttl, expectsResult = true, ContentPut(
      documentUri + "/" + itemId,
      DynamicBody(content.bodyValue),
      headers=headers
    ), Obj.from(PrimaryExtra.INTERNAL_OPERATION → true))

    Task.fromFuture(owner ? task).map{
      _ ⇒ handlePrimaryWorkerTaskResult
    }
  }

  private def deleteViewItem(documentUri: String, itemId: String, owner: ActorRef, ttl: Long)(implicit mcx: MessagingContext): Task[Any] = {
    implicit val timeout = akka.util.Timeout(Duration(ttl - System.currentTimeMillis() + 3000, duration.MILLISECONDS))
    val task = LocalTask(documentUri, HyperstorageWorkerSettings.PRIMARY, ttl, expectsResult = true,
      ContentDelete(documentUri + "/" + itemId),
      Obj.from(PrimaryExtra.INTERNAL_OPERATION → true)
    )

    Task.fromFuture(owner ? task).map{
      _ ⇒ handlePrimaryWorkerTaskResult
    }
  }

  private def handlePrimaryWorkerTaskResult: PartialFunction[ResponseBase, Unit] = {
    case e: Throwable ⇒ throw e
  }
}

case class UnwrappedTransaction(transaction: Transaction, unwrappedBody: DynamicRequest)

object UnwrappedTransaction {
  def apply(transaction: Transaction): UnwrappedTransaction = UnwrappedTransaction(
    transaction, MessageReader.fromString(transaction.body, DynamicRequest.apply)
  )
}
