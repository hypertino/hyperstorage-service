/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.workers

import akka.actor.ActorRef
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.db.Db
import com.hypertino.hyperstorage.sharding.WorkerGroupSettings
import com.hypertino.hyperstorage.workers.primary.PrimaryWorker
import com.hypertino.hyperstorage.workers.secondary.SecondaryWorker
import com.hypertino.metrics.MetricsTracker
import monix.execution.Scheduler

import scala.concurrent.duration.FiniteDuration

object HyperstorageWorkerSettings {
  final val PRIMARY = "hyperstorage-primary-worker"
  final val SECONDARY = "hyperstorage-secondary-worker"

  def apply(hyperbus: Hyperbus,
            db: Db,
            metricsTracker: MetricsTracker,
            primaryWorkerCount: Int,
            secondaryWorkerCount: Int,
            backgroundTaskTimeout: FiniteDuration,
            indexManager: ActorRef,
            scheduler: Scheduler): Map[String, WorkerGroupSettings] = {
    val primaryWorkerProps = PrimaryWorker.props(hyperbus, db, metricsTracker, backgroundTaskTimeout)
    val primaryRequestMeta = Seq(ContentPut, ContentPatch, ContentDelete, ViewPut, ViewDelete)

    val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, metricsTracker, indexManager, scheduler)
    val secondaryRequestMeta = Seq(IndexPost, IndexDelete)
    Map(
      PRIMARY → WorkerGroupSettings(primaryWorkerProps, primaryWorkerCount, "pgw-", primaryRequestMeta),
      SECONDARY → WorkerGroupSettings(secondaryWorkerProps, secondaryWorkerCount, "sgw-", secondaryRequestMeta)
    )
  }
}
