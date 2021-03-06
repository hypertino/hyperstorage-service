/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.recovery

import java.util.Date

import akka.actor._
import akka.pattern.ask
import com.codahale.metrics.Meter
import com.hypertino.binders.value.Null
import com.hypertino.hyperbus.model.{MessagingContext, NotFound, Ok}
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.db.{Content, Db}
import com.hypertino.hyperstorage.internal.api.{BackgroundContentTask, BackgroundContentTaskResult, BackgroundContentTasksPost, NodeStatus}
import com.hypertino.hyperstorage.metrics.Metrics
import com.hypertino.hyperstorage.sharding.{LocalTask, ShardedClusterData, UpdateShardStatus}
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.metrics.MetricsTracker
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import com.hypertino.hyperstorage.utils.TrackerUtils._
import scala.concurrent.duration._

/*
 * recovery job:
        1. recovery control actor
            1.1. track cluster data
            1.2. track list of self partitions
            1.3. run hot recovery worker on partition
            1.4. run stale recovery worker on partition
            1.5. warns if hot isn't covered within hot period, extend hot period [range]
        2. hot data recovery worker
            2.1. selects data from partition for last (30) minutes
            2.1. sends tasks for recovery, waits each to answer (limit?)
        3. stale data recovery worker
            3.1. selects data from partition starting from last check, until now - 30min (hot level)
            3.2. sends tasks for recovery, waits each to anwser (indefinitely)
            3.3. updates check dates (with replication lag date)
*/

case class StartCheck(processId: Long)

case object ShutdownRecoveryWorker

case class CheckQuantum[T <: WorkerState](processId: Long, dtQuantum: Long, partitions: Seq[Int], state: T, step: Long)

trait WorkerState {
  def startedAt: Long

  def startQuantum: Long

  def nextCheck(currentQuantum: Long, multiplier: Int): Long = {
    if (currentQuantum <= startQuantum) {
      // just starting
      0
    }
    else {
      val quantumTime = TransactionLogic.getUnixTimeFromQuantum(1)
      val millisNow = System.currentTimeMillis()
      val tookTime = millisNow - startedAt
      val timeSpentPerQuantum = tookTime / (currentQuantum - startQuantum)
      if (timeSpentPerQuantum * multiplier < quantumTime) {
        // if we are x (multiplier) times faster, then we can delay our checks
        // but no more than 15seconds
        quantumTime - timeSpentPerQuantum * multiplier
      }
      else {
        0
      }
    }
  }
}

abstract class RecoveryWorker[T <: WorkerState](
                                                 db: Db,
                                                 shardProcessor: ActorRef,
                                                 tracker: MetricsTracker,
                                                 retryPeriod: FiniteDuration,
                                                 backgroundTaskTimeout: FiniteDuration,
                                                 transactionTtl: FiniteDuration,
                                                 scheduler: monix.execution.Scheduler
                                               ) extends Actor with StrictLogging {

  import context._

  private var currentProcessId: Long = 0

  def jobName: String
  def checkQuantumTimerName: String
  def trackIncompleteMeter: Meter

  def receive = {
    case UpdateShardStatus(_, NodeStatus.ACTIVE, stateData) ⇒
      clusterActivated(stateData, TransactionLogic.getPartitions(stateData))

    case ShutdownRecoveryWorker ⇒
      context.stop(self)
  }

  def running(stateData: ShardedClusterData, workerPartitions: Seq[Int]): Receive = {
    case UpdateShardStatus(_, NodeStatus.ACTIVE, newStateData) ⇒
      if (newStateData != stateData) {
        // restart with new partition list
        clusterActivated(newStateData, TransactionLogic.getPartitions(newStateData))
      }

    case UpdateShardStatus(_, NodeStatus.DEACTIVATING, _) ⇒
      context.unbecome()

    case StartCheck(processId) if processId == currentProcessId ⇒
      currentProcessId = currentProcessId + 1 // this protects from parallel duplicate checks when rebalancing
      runNewRecoveryCheck(currentProcessId, workerPartitions)

    case q @ CheckQuantum(processId, dtQuantum, partitionsForQuantum, state, step) if processId == currentProcessId ⇒
      tracker.timeOfTask(checkQuantumTimerName) {
        checkQuantum(dtQuantum, partitionsForQuantum, step) map { _ ⇒
          runNextRecoveryCheck(processId, q.asInstanceOf[CheckQuantum[T]])
        } onErrorRecover {
          case e: Throwable ⇒
            logger.error(s"Quantum check for $jobName/$dtQuantum is failed. Will retry in $retryPeriod", e)
            system.scheduler.scheduleOnce(retryPeriod, self, CheckQuantum(processId, dtQuantum, partitionsForQuantum, state, step))(dispatcher)
        }
      } runAsync scheduler

    case ShutdownRecoveryWorker ⇒
      logger.info(s"$self is shutting down...")
      context.become(shuttingDown)
  }

  def shuttingDown: Receive = {
    case _: StartCheck | _: CheckQuantum[_] ⇒
      context.stop(self)
  }

  def clusterActivated(stateData: ShardedClusterData, partitions: Seq[Int]): Unit = {
    logger.info(s"Cluster is active $jobName is running. Current data: $stateData.")
    currentProcessId += 1
    context.become(running(stateData, partitions))
    self ! StartCheck(currentProcessId)
  }

  def runNewRecoveryCheck(processId: Long, partitions: Seq[Int]): Unit

  def runNextRecoveryCheck(processId: Long, previous: CheckQuantum[T]): Unit

  def checkQuantum(dtQuantum: Long, partitions: Seq[Int], step: Long): Task[Any] = {
    if (step == 0) {
      logger.info(s"Running $jobName recovery check starting from ${qts(dtQuantum)}. Partitions to process: ${partitions.size}")
    }
    else {
      logger.debug(s"Running $jobName partition check for ${qts(dtQuantum)}. Partitions to process: ${partitions.size}. Step #$step")
    }
    Task.sequence {
      val tasks: Seq[Task[Any]] = partitions.map { partition ⇒
          // todo: selectPartitionTransactions selects body which isn't eficient
          db.selectPartitionTransactions(dtQuantum, partition).flatMap { partitionTransactions ⇒
            val incompleteTransactions = partitionTransactions.toList.filter(_.completedAt.isEmpty).groupBy(_.documentUri)
            Task.sequence {
              incompleteTransactions.map {
                case (documentUri, transactions) ⇒
                  trackIncompleteMeter.mark(transactions.length)
                  implicit val mcx = MessagingContext.empty
                  val task = LocalTask(
                    key = documentUri,
                    group = HyperstorageWorkerSettings.SECONDARY,
                    ttl = System.currentTimeMillis() + backgroundTaskTimeout.toMillis,
                    expectsResult = true,
                    BackgroundContentTasksPost(BackgroundContentTask(documentUri)),
                    extra = Null
                  )
                  logger.warn(s"Incomplete resource at $documentUri. Sending $jobName task i#${task.request.correlationId} to finish or remove transaction.")
                  Task.fromFuture(shardProcessor.ask(task)(backgroundTaskTimeout + 500.milliseconds)) flatMap {
                    case Ok(BackgroundContentTaskResult(completePath, completedTransactions), _) ⇒
                      logger.debug(s"$jobName of '$completePath' completed successfully: $completedTransactions")
                      if (documentUri == completePath) {
                        val set = completedTransactions.toSet
                        val abandonedTransactions = transactions.filterNot(m ⇒ set.contains(m.uuid.toString))
                        if (abandonedTransactions.nonEmpty) {
                          logger.warn(s"$jobName: Abandoned transactions for '$completePath' were found: '${abandonedTransactions.map(_.uuid).mkString(",")}'. Deleting...")
                          Task.sequence {
                            abandonedTransactions.map { abandonedTransaction ⇒
                              db.completeTransaction(abandonedTransaction, transactionTtl.toSeconds)
                            }
                          }
                        } else {
                          Task.unit
                        }
                      }
                      else {
                        logger.error(s"$jobName result received for '$completePath' while expecting for the '$documentUri'")
                        Task.unit
                      }
                    // todo: do we need this here?
                    case NotFound(errorBody, _) ⇒
                      logger.debug(s"$jobName: Tried to recover not existing resource. '$errorBody'")
                      val olderThan5Min = System.currentTimeMillis() - 5 * 60 * 1000
                      if (transactions.forall(t ⇒ TransactionLogic.getUnixTimeFromQuantum(t.dtQuantum) < olderThan5Min)) {
                        logger.debug(s"$jobName: removing incomplete transactions without content older than 5 min: $transactions")
                        Task.sequence {
                          transactions.map { t ⇒
                            db.deleteTransaction(t)
                          }
                        }
                      }
                      else {
                        Task.unit
                      }
                    case e: Throwable ⇒
                      Task.raiseError(e)
                    case other ⇒
                      Task.raiseError(throw new RuntimeException(s"$jobName: Unexpected result from recovery task: $other"))
                  }
              }
            }
          }
      }
      tasks
    }
  }

  def qts(qt: Long) = s"$qt [${new Date(TransactionLogic.getUnixTimeFromQuantum(qt))}]"

  def scheduleNext(next: CheckQuantum[T]) = {
    val nextRun = Math.min(next.state.nextCheck(next.dtQuantum, 5), retryPeriod.toMillis)
    //log.trace(s"Next run in $nextRun")
    if (nextRun <= 0)
      self ! next
    else
      system.scheduler.scheduleOnce(Duration(nextRun, MILLISECONDS), self, next)
  }
}

case class HotWorkerState(workerPartitions: Seq[Int],
                          startQuantum: Long,
                          startedAt: Long = System.currentTimeMillis()) extends WorkerState

class HotRecoveryWorker(
                         hotPeriod: (Long, Long),
                         db: Db,
                         shardProcessor: ActorRef,
                         tracker: MetricsTracker,
                         retryPeriod: FiniteDuration,
                         recoveryCompleterTimeout: FiniteDuration,
                         transactionTtl: FiniteDuration,
                         scheduler: monix.execution.Scheduler
                       ) extends RecoveryWorker[HotWorkerState](
  db, shardProcessor, tracker, retryPeriod, recoveryCompleterTimeout, transactionTtl, scheduler
) {

  import context._

  def runNewRecoveryCheck(processId: Long, partitions: Seq[Int]): Unit = {
    val millis = System.currentTimeMillis()
    val lowerBound = TransactionLogic.getDtQuantum(millis - hotPeriod._1)
    self ! CheckQuantum(processId, lowerBound, partitions, HotWorkerState(partitions, lowerBound), 0)
  }

  // todo: detect if lag is increasing and print warning
  def runNextRecoveryCheck(processId: Long, previous: CheckQuantum[HotWorkerState]): Unit = {
    val millis = System.currentTimeMillis()
    val upperBound = TransactionLogic.getDtQuantum(millis - hotPeriod._2)
    val nextQuantum = previous.dtQuantum + 1
    if (nextQuantum < upperBound) {
      scheduleNext(CheckQuantum(processId, nextQuantum, previous.state.workerPartitions, previous.state, previous.step+1))
    } else {
      logger.info(s"$jobName complete on ${qts(previous.dtQuantum)}. Will start new in $retryPeriod")
      system.scheduler.scheduleOnce(retryPeriod, self, StartCheck(processId))
    }
  }

  override def checkQuantumTimerName: String = Metrics.HOT_QUANTUM_TIMER

  override def trackIncompleteMeter: Meter = tracker.meter(Metrics.HOT_INCOMPLETE_METER)

  override def jobName: String = "HOT Recovery"
}

case class StaleWorkerState(workerPartitions: Seq[Int],
                            partitionsPerQuantum: Map[Long, Seq[Int]],
                            startQuantum: Long,
                            startedAt: Long = System.currentTimeMillis()) extends WorkerState

//  We need StaleRecoveryWorker because of cassandra data can reappear later due to replication
class StaleRecoveryWorker(
                           stalePeriod: (Long, Long),
                           db: Db,
                           shardProcessor: ActorRef,
                           tracker: MetricsTracker,
                           retryPeriod: FiniteDuration,
                           backgroundTaskTimeout: FiniteDuration,
                           transactionTtl: FiniteDuration,
                           scheduler: monix.execution.Scheduler
                         ) extends RecoveryWorker[StaleWorkerState](
  db, shardProcessor, tracker, retryPeriod, backgroundTaskTimeout, transactionTtl, scheduler
) {

  import context._

  def runNewRecoveryCheck(processId: Long, partitions: Seq[Int]): Unit = {
    val lowerBound = TransactionLogic.getDtQuantum(System.currentTimeMillis() - stalePeriod._1)

    Task.sequence {
      partitions.map { partition ⇒
        db.selectCheckpoint(partition) map {
          case Some(lastQuantum) ⇒
            lastQuantum → partition
          case None ⇒
            lowerBound → partition
        }
      }
    } map { partitionQuantums ⇒

      val stalest = partitionQuantums.minBy(_._1)._1
      val partitionsPerQuantum: Map[Long, Seq[Int]] = partitionQuantums.groupBy(_._1).map(kv ⇒ kv._1 → kv._2.map(_._2))
      val (startFrom, partitionsToProcess) = if (stalest < lowerBound) {
        (stalest, partitionsPerQuantum(stalest))
      } else {
        (lowerBound, partitions)
      }
      val state = StaleWorkerState(partitions, partitionsPerQuantum, startFrom)

      self ! CheckQuantum(processId, startFrom, partitionsToProcess, state, 0)
    } onErrorRecover {
      case e: Throwable ⇒
        logger.error(s"Can't fetch checkpoints. Will retry in $retryPeriod", e)
        system.scheduler.scheduleOnce(retryPeriod, self, StartCheck(processId))(dispatcher)
    } runAsync scheduler
  }

  // todo: detect if lag is increasing and print warning
  def runNextRecoveryCheck(processId: Long, previous: CheckQuantum[StaleWorkerState]): Unit = {
    val millis = System.currentTimeMillis()
    val lowerBound = TransactionLogic.getDtQuantum(millis - stalePeriod._1)
    val upperBound = TransactionLogic.getDtQuantum(millis - stalePeriod._2)
    val nextQuantum = previous.dtQuantum + 1

    val updateCheckpoints = if (previous.dtQuantum <= lowerBound) {
      Task.sequence {
        previous.partitions.map { partition ⇒
          db.updateCheckpoint(partition, previous.dtQuantum)
        }
      }
    } else {
      Task.unit
    }

    updateCheckpoints map { _ ⇒
      if (nextQuantum < upperBound) {
        if (nextQuantum >= lowerBound || previous.partitions == previous.state.workerPartitions) {
          scheduleNext(CheckQuantum(processId, nextQuantum, previous.state.workerPartitions, previous.state, previous.step+1))
        } else {
          val nextQuantumPartitions = previous.state.partitionsPerQuantum.getOrElse(nextQuantum, Seq.empty)
          val partitions = (previous.partitions.toSet ++ nextQuantumPartitions).toSeq
          scheduleNext(CheckQuantum(processId, nextQuantum, partitions, previous.state, previous.step+1))
        }
      }
      else {
        logger.info(s"$jobName complete on ${qts(previous.dtQuantum)}. Will start new in $retryPeriod")
        system.scheduler.scheduleOnce(retryPeriod, self, StartCheck(processId))
      }
    } onErrorRecover {
      case e: Throwable ⇒
        logger.error(s"Can't update checkpoints. Will restart in $retryPeriod", e)
        system.scheduler.scheduleOnce(retryPeriod, self, StartCheck(processId))
    } runAsync scheduler
  }

  override def checkQuantumTimerName: String = Metrics.STALE_QUANTUM_TIMER

  override def trackIncompleteMeter: Meter = tracker.meter(Metrics.STALE_INCOMPLETE_METER)

  override def jobName: String = "STALE Recovery"
}

object HotRecoveryWorker {
  def props(
             hotPeriod: (Long, Long),
             db: Db,
             shardProcessor: ActorRef,
             tracker: MetricsTracker,
             retryPeriod: FiniteDuration,
             recoveryCompleterTimeout: FiniteDuration,
             transactionTtl: FiniteDuration,
             scheduler: monix.execution.Scheduler
           ) = Props(new HotRecoveryWorker(hotPeriod, db, shardProcessor, tracker, retryPeriod, recoveryCompleterTimeout, transactionTtl, scheduler))
}

object StaleRecoveryWorker {
  def props(
             stalePeriod: (Long, Long),
             db: Db,
             shardProcessor: ActorRef,
             tracker: MetricsTracker,
             retryPeriod: FiniteDuration,
             backgroundTaskTimeout: FiniteDuration,
             transactionTtl: FiniteDuration,
             scheduler: monix.execution.Scheduler
           ) = Props(new StaleRecoveryWorker(stalePeriod, db, shardProcessor, tracker, retryPeriod, backgroundTaskTimeout, transactionTtl, scheduler))
}
