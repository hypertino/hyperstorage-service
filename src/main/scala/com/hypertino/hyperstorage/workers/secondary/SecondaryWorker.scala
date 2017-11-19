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
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.sharding.{ShardTask, WorkerTaskResult}
import com.hypertino.metrics.MetricsTracker
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

// todo: do we really need a ShardTaskComplete ?

trait SecondaryTaskTrait extends ShardTask {
  def ttl: Long

  def isExpired = ttl < System.currentTimeMillis()

  def group = "hyperstorage-secondary-worker"
}

trait SecondaryTaskError

@SerialVersionUID(1L) case class SecondaryTaskFailed(key: String, reason: String) extends RuntimeException(s"Secondary task for '$key' is failed with reason $reason") with SecondaryTaskError

class SecondaryWorker(val hyperbus: Hyperbus, val db: Db, val tracker: MetricsTracker, val indexManager: ActorRef, implicit val scheduler: Scheduler) extends Actor with StrictLogging
  with BackgroundContentTaskCompleter
  with IndexDefTaskWorker
  with IndexContentTaskWorker {

  override def executionContext: ExecutionContext = context.dispatcher // todo: use other instead of this?

  override def receive: Receive = {
    case task: BackgroundContentTask ⇒
      val owner = sender()
      executeBackgroundTask(owner, task) recover withSecondaryTaskFailed(task) pipeTo owner

    case task: IndexDefTask ⇒
      val owner = sender()
      executeIndexDefTask(task) recover withSecondaryTaskFailed(task) pipeTo owner

    case task: IndexContentTask ⇒
      val owner = sender()
      indexNextBucket(task) recover withSecondaryTaskFailed(task) pipeTo owner
  }


  private def withSecondaryTaskFailed(task: SecondaryTaskTrait): PartialFunction[Throwable, WorkerTaskResult] = {
    case e: SecondaryTaskError ⇒
      logger.error(s"Can't execute $task", e)
      WorkerTaskResult(task.key, task.group, e)

    case NonFatal(e) ⇒
      logger.error(s"Can't execute $task", e)
      WorkerTaskResult(task.key, task.group, SecondaryTaskFailed(task.key, e.toString))
  }
}

object SecondaryWorker {
  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, indexManager: ActorRef, scheduler: Scheduler) = Props(
    new SecondaryWorker(hyperbus, db, tracker, indexManager, scheduler)
  )
}

