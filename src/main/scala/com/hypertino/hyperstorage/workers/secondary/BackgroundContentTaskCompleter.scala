package com.hypertino.hyperstorage.workers.secondary

import java.util.UUID

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.pattern.ask
import com.datastax.driver.core.utils.UUIDs
import com.hypertino.binders.value.{Null, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{DynamicRequest, _}
import com.hypertino.hyperbus.serialization.MessageReader
import com.hypertino.hyperstorage.api.{ContentDelete, ContentPut, ViewDelete, ViewPut}
import com.hypertino.hyperstorage.db.{Transaction, _}
import com.hypertino.hyperstorage.indexing.{IndexLogic, ItemIndexer}
import com.hypertino.hyperstorage.metrics.Metrics
import com.hypertino.hyperstorage.sharding.ShardTaskComplete
import com.hypertino.hyperstorage.utils.FutureUtils
import com.hypertino.hyperstorage.workers.primary.{PrimaryContentTask, PrimaryWorkerTaskResult}
import com.hypertino.hyperstorage.{ResourcePath, _}
import com.hypertino.metrics.MetricsTracker
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, duration}
import scala.util.Success
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class BackgroundContentTask(ttl: Long, documentUri: String, expectsResult: Boolean) extends SecondaryTaskTrait {
  def key = documentUri
}

@SerialVersionUID(1L) case class BackgroundContentTaskResult(documentUri: String, transactions: Seq[UUID])

@SerialVersionUID(1L) case class BackgroundContentTaskNoSuchResourceException(documentUri: String) extends RuntimeException(s"No such resource: $documentUri")

@SerialVersionUID(1L) case class BackgroundContentTaskFailedException(documentUri: String, reason: String) extends RuntimeException(s"Background task for $documentUri is failed: $reason")

trait BackgroundContentTaskCompleter extends ItemIndexer {
  def hyperbus: Hyperbus
  def db: Db
  def tracker: MetricsTracker
  def log: LoggingAdapter
  implicit def scheduler: Scheduler

  def deleteIndexDefAndData(indexDef: IndexDef): Future[Unit]

  def executeBackgroundTask(owner: ActorRef, task: BackgroundContentTask): Future[ShardTaskComplete] = {
    try {
      val ResourcePath(documentUri, itemId) = ContentLogic.splitPath(task.documentUri)
      if (!itemId.isEmpty) {
        throw new IllegalArgumentException(s"Background task key ${task.key} doesn't correspond to $documentUri")
      }
      else {
        tracker.timeOfFuture(Metrics.SECONDARY_PROCESS_TIME) {
          db.selectContentStatic(task.documentUri) flatMap {
            case None ⇒
              log.error(s"Didn't found resource to background complete, dismissing task: $task")
              Future.failed(BackgroundContentTaskNoSuchResourceException(task.documentUri))
            case Some(content) ⇒
              try {
                if (log.isDebugEnabled) {
                  log.debug(s"Background task for $content")
                }
                completeTransactions(task, content, owner, task.ttl)
              } catch {
                case NonFatal(e) ⇒
                  log.error(e, s"Background task $task didn't complete")
                  Future.failed(e)
              }
          }
        }
      }
    }
    catch {
      case NonFatal(e) ⇒
        log.error(e, s"Background task $task didn't complete")
        Future.failed(e)
    }
  }

  // todo: split & refactor method
  private def completeTransactions(task: BackgroundContentTask,
                                   content: ContentStatic,
                                   owner: ActorRef,
                                   ttl: Long): Future[ShardTaskComplete] = {
    if (content.transactionList.isEmpty) {
      Future.successful(ShardTaskComplete(task, BackgroundContentTaskResult(task.documentUri, Seq.empty)))
    }
    else {
      selectIncompleteTransactions(content) flatMap { incompleteTransactions ⇒
        val updateIndexFuture: Future[Any] = updateIndexes(content, incompleteTransactions, owner, ttl).runAsync
        updateIndexFuture.flatMap { _ ⇒
          FutureUtils.serial(incompleteTransactions) { it ⇒
            val event = it.unwrappedBody
            val viewTask: Task[Unit] = event.headers.hrl.location match {
              case l if l == ViewPut.location ⇒
                val templateUri = event.headers.getOrElse(TransactionLogic.HB_HEADER_TEMPLATE_URI, Null).toString
                val filterBy = event.headers.getOrElse(TransactionLogic.HB_HEADER_FILTER, Null) match {
                  case Null ⇒ None
                  case other ⇒ Some(other.toString)
                }
                Task.fromFuture(db.insertViewDef(ViewDef(key="*", task.documentUri, templateUri, filterBy)))
              case l if l == ViewDelete.location ⇒
                Task.fromFuture(db.deleteViewDef(key="*", task.documentUri))
              case _ ⇒
                Task.unit
            }

            viewTask
              .flatMap( _ ⇒ hyperbus.publish(event))
              .runAsync
              .flatMap{ publishResult ⇒
              if (log.isDebugEnabled) {
                log.debug(s"Event $event is published with result $publishResult")
              }
              db.completeTransaction(it.transaction) map { _ ⇒
                if (log.isDebugEnabled) {
                  log.debug(s"${it.transaction} is complete")
                }
                it.transaction
              }
            }
          }
        } map { updatedTransactions ⇒
          ShardTaskComplete(task, BackgroundContentTaskResult(task.documentUri, updatedTransactions.map(_.uuid)))
        } recover {
          case NonFatal(e) ⇒
            log.error(e, s"Task failed: $task")
            ShardTaskComplete(task, BackgroundContentTaskFailedException(task.documentUri, e.toString))
        } andThen {
          case Success(ShardTaskComplete(_, BackgroundContentTaskResult(documentUri, updatedTransactions))) ⇒
            log.debug(s"Removing completed transactions $updatedTransactions from $documentUri")
            db.removeCompleteTransactionsFromList(documentUri, updatedTransactions.toList) recover {
              case NonFatal(e) ⇒
                log.error(e, s"Can't remove complete transactions $updatedTransactions from $documentUri")
            }
        }
      }
    }
  }

  private def selectIncompleteTransactions(content: ContentStatic): Future[Seq[UnwrappedTransaction]] = {
    import ContentLogic._
    val transactionsFStream = content.transactionList.toStream.map { transactionUuid ⇒
      val quantum = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(transactionUuid))
      db.selectTransaction(quantum, content.partition, content.documentUri, transactionUuid)
    }
    FutureUtils.collectWhile(transactionsFStream) {
      case Some(transaction) ⇒ UnwrappedTransaction(transaction)
    } map (_.reverse)
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
      val indexDefsTask = Task.fromFuture(db.selectIndexDefs(contentStatic.documentUri)).memoize
      if (isCollectionDelete) {
        // todo: what if after collection delete, there is a transaction with insert?
        // todo: delete view if collection is deleted
        // todo: cache index meta
        indexDefsTask
          .flatMap { indexDefsIterator ⇒
            val indexDefs = indexDefsIterator.toSeq
            Task.gather(indexDefs.map { indexDef ⇒
              log.debug(s"Removing index $indexDef")
              Task.fromFuture(deleteIndexDefAndData(indexDef))
            }: Seq[Task[Unit]])
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

        Task.gather(itemIds.keys.toSeq.map { itemId ⇒
          log.debug(s"Looking for content $itemId")

          // todo: cache content
          val contentTask = Task.fromFuture(db.selectContent(contentStatic.documentUri, itemId)).memoize

          val lastTransaction = incompleteTransactions.filter(_.transaction.itemId == itemId).last
          updateView(contentStatic.documentUri + "/" + itemId, contentTask, lastTransaction, owner, ttl)
            .flatMap { _ ⇒
              indexDefsTask
                .flatMap { indexDefsIterator ⇒
                  val indexDefs = indexDefsIterator.toSeq
                  Task.gather {
                    indexDefs.map { indexDef ⇒
                      if (log.isDebugEnabled) {
                        log.debug(s"Indexing content ${contentStatic.documentUri}/$itemId for $indexDefs")
                      }

                      // todo: refactor, this is crazy
                      val seq: Seq[Seq[(String, Value)]] = itemIds(itemId).filter(_._1 == indexDef.indexId).map(_._2)
                      val deleteObsoleteFuture = FutureUtils.serial(seq) { s ⇒
                        db.deleteIndexItem(indexDef.tableName, indexDef.documentUri, indexDef.indexId, itemId, s)
                      }

                      contentTask.flatMap {
                        case Some(item) if !item.isDeleted ⇒
                          Task.fromFuture(deleteObsoleteFuture.flatMap(_ ⇒ indexItem(indexDef, item, idFieldName)))

                        case _ ⇒
                          Task.fromFuture(
                            deleteObsoleteFuture.flatMap { _ ⇒
                              val revision = incompleteTransactions.map(t ⇒ t.transaction.revision).max
                              db.updateIndexRevision(indexDef.tableName, indexDef.documentUri, indexDef.indexId, revision)
                            })
                      }
                    }
                  }
                }
            }
        }: Seq[Task[Any]])
      }
    } else {
      val contentTask = Task.fromFuture(db.selectContent(contentStatic.documentUri, "")).memoize
      updateView(contentStatic.documentUri,contentTask,incompleteTransactions.last, owner, ttl)
    }
  }

  private def updateView(path: String,
                         contentTask: Task[Option[Content]],
                         lastTransaction: UnwrappedTransaction,
                         owner: ActorRef,
                         ttl: Long): Task[Any] = {
    val viewDefsTask = Task.fromFuture(db.selectViewDefs())
    viewDefsTask.flatMap { viewDefsIterator ⇒
      val viewDefs = viewDefsIterator.toSeq
      Task.gather {
        viewDefs.map { viewDef ⇒
          ContentLogic.pathAndTemplateToId(path, viewDef.templateUri).map { id ⇒
            implicit val mcx = lastTransaction.unwrappedBody
            if (lastTransaction.unwrappedBody.headers.method == Method.FEED_DELETE) {
              deleteViewItem(viewDef.documentUri, id, owner, ttl)
            }
            else {
              contentTask.flatMap {
                case Some(content) ⇒
                  val matches = viewDef.filterBy.map { filterBy ⇒
                    try {
                      IndexLogic.evaluateFilterExpression(filterBy, content.bodyValue)
                    } catch {
                      case NonFatal(e) ⇒
                        if (log.isDebugEnabled) {
                          log.debug(s"Can't evaluate expression: `$filterBy` for $path", e)
                        }
                        false
                    }
                  } getOrElse {
                    true
                  }
                  if (matches) {
                    addViewItem(viewDef.documentUri, id, content.bodyValue, owner, ttl)
                  } else {
                    deleteViewItem(viewDef.documentUri, id, owner, ttl)
                  }
                case None ⇒ // удалить
                  deleteViewItem(viewDef.documentUri, id, owner, ttl)
              }
            }
          } getOrElse {
            Task.unit
          }
        } : Seq[Task[Any]]
      }
    }
  }

  private def addViewItem(documentUri: String, itemId: String, bodyValue: Value, owner: ActorRef, ttl: Long)(implicit mcx: MessagingContext): Task[Any] = {
    implicit val timeout = akka.util.Timeout(Duration(ttl - System.currentTimeMillis() + 3000, duration.MILLISECONDS))
    Task.fromFuture(owner ? PrimaryContentTask(documentUri, ttl, ContentPut(documentUri + "/" + itemId, DynamicBody(bodyValue)).serializeToString, expectsResult = true)).map{
      _ ⇒ handlePrimaryWorkerTaskResult
    }
  }

  private def deleteViewItem(documentUri: String, itemId: String, owner: ActorRef, ttl: Long)(implicit mcx: MessagingContext): Task[Any] = {
    implicit val timeout = akka.util.Timeout(Duration(ttl - System.currentTimeMillis() + 3000, duration.MILLISECONDS))
    Task.fromFuture(owner ? PrimaryContentTask(documentUri, ttl, ContentDelete(documentUri + "/" + itemId).serializeToString, expectsResult = true)).map{
      _ ⇒ handlePrimaryWorkerTaskResult
    }
  }

  private def handlePrimaryWorkerTaskResult: PartialFunction[PrimaryWorkerTaskResult, Unit] = {
    case result: PrimaryWorkerTaskResult ⇒ MessageReader.fromString(result.content, StandardResponse.apply) match {
      case NonFatal(e) ⇒ throw e
    }
  }
}

case class UnwrappedTransaction(transaction: Transaction, unwrappedBody: DynamicRequest)

object UnwrappedTransaction {
  def apply(transaction: Transaction): UnwrappedTransaction = UnwrappedTransaction(
    transaction, MessageReader.fromString(transaction.body, DynamicRequest.apply)
  )
}
