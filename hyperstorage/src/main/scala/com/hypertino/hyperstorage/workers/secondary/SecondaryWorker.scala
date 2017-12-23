/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.workers.secondary

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.pipe
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model._
import com.hypertino.hyperstorage.api.{IndexDelete, IndexPost}
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.internal.api.{BackgroundContentTasksPost, IndexContentTasksPost}
import com.hypertino.hyperstorage.sharding.{LocalTask, WorkerTaskResult}
import com.hypertino.hyperstorage.utils.ErrorCode
import com.hypertino.metrics.MetricsTracker
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.ExecutionContext
import scala.util.Try

class SecondaryWorker(val hyperbus: Hyperbus,
                      val db: Db,
                      val tracker: MetricsTracker,
                      val indexManager: ActorRef,
                      implicit val scheduler: monix.execution.Scheduler) extends Actor with StrictLogging
  with BackgroundContentTaskCompleter
  with IndexDefTaskWorker
  with IndexContentTaskWorker {

  override def executionContext: ExecutionContext = context.dispatcher // todo: use other instead of this?

  override def receive: Receive = {
    case task: LocalTask ⇒
      val owner = sender()
      implicit val mcx = task.request
      val t: Task[WorkerTaskResult] = Task.fromTry {
        Try {
          {
            task.request match {
              case r: BackgroundContentTasksPost ⇒ executeBackgroundTask(owner, task, r)
              case r: IndexContentTasksPost ⇒ indexNextBucket(task, r)
              case r: IndexPost ⇒ createNewIndex(task, r)
              case r: IndexDelete ⇒ removeIndex(task, r)
            }
          }.map { r ⇒
            WorkerTaskResult(task, r)
          }
        }
      }
        .flatten
        .onErrorRecoverWith {
          case e: HyperbusError[_] ⇒
            logger.error(s"Secondary task $task didn't complete", e)
            Task.now(WorkerTaskResult(task, e))

          case e: Throwable ⇒
            logger.error(s"Secondary task $task didn't complete", e)
            Task.now(WorkerTaskResult(task, InternalServerError(ErrorBody(ErrorCode.BACKGROUND_TASK_FAILED, Some(s"Task on ${task.key} is failed: ${e.toString}")))))
        }

      t.runAsync pipeTo owner
  }
}

object SecondaryWorker {
  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, indexManager: ActorRef, scheduler: Scheduler) = Props(
    new SecondaryWorker(hyperbus, db, tracker, indexManager, scheduler)
  )
}

