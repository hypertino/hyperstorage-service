/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.pattern.gracefulStop
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperstorage.db.Db
import com.hypertino.hyperstorage.indexing.IndexManager
import com.hypertino.hyperstorage.metrics.MetricsReporter
import com.hypertino.hyperstorage.recovery.{HotRecoveryWorker, ShutdownRecoveryWorker, StaleRecoveryWorker}
import com.hypertino.hyperstorage.sharding.akkacluster.AkkaClusterShardingTransport
import com.hypertino.hyperstorage.sharding.{ShardProcessor, ShutdownProcessor, SubscribeToShardStatus, WorkerGroupSettings}
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.hyperstorage.workers.primary.PrimaryWorker
import com.hypertino.hyperstorage.workers.secondary.SecondaryWorker
import com.hypertino.metrics.MetricsTracker
import com.hypertino.service.control.api.Service
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import scaldi.{Injectable, Injector}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

case class HyperStorageConfig(
                               shutdownTimeout: FiniteDuration,
                               shardSyncTimeout: FiniteDuration,
                               maxWorkers: Int,
                               backgroundTaskTimeout: FiniteDuration,
                               requestTimeout: FiniteDuration,
                               failTimeout: FiniteDuration,
                               hotRecovery: FiniteDuration,
                               hotRecoveryRetry: FiniteDuration,
                               staleRecovery: FiniteDuration,
                               staleRecoveryRetry: FiniteDuration
                        )

class HyperStorageService(implicit val scheduler: Scheduler,
                          implicit val injector: Injector) extends Service with Injectable with StrictLogging {

  logger.info(s"Starting Hyperstorage service v${BuildInfo.version}...")

  // configuration
  private val config: Config = inject[Config]
  private val connector: CassandraConnector = inject[CassandraConnector]

  import com.hypertino.binders.config.ConfigBinders._

  private val serviceConfig = config.getValue("hyperstorage").read[HyperStorageConfig]

  logger.info(s"Hyperstorage configuration: $config")

  // metrics tracker
  private val tracker = inject[MetricsTracker]
  MetricsReporter.startReporter(tracker)

  import serviceConfig._

  // initialize
  logger.info(s"Initializing hyperbus...")
  private val hyperbus = inject[Hyperbus]

  // currently we rely on the name of system
  private val actorSystem = ActorSystem("hyperstorage", config.getConfig("hyperstorage.actor-system"))
  // ActorSystemRegistry.get("eu-inn").get
  private val cluster = Cluster(actorSystem)

  //
  private val db = new Db(connector)
  // trigger connect to c* but continue initialization
  try {
    logger.info(s"Initializing database connection...")
    db.preStart()
  } catch {
    case e: Throwable ⇒
      logger.error(s"Can't create C* session", e)
  }

  private val indexManagerProps = IndexManager.props(hyperbus, db, tracker, maxWorkers)
  private val indexManagerRef = actorSystem.actorOf(
    indexManagerProps, "index-manager"
  )

  // worker actor todo: recovery job
  private val workerSettings = HyperstorageWorkerSettings(hyperbus, db, tracker, maxWorkers, maxWorkers,
    backgroundTaskTimeout, indexManagerRef, scheduler)

  // shard shard cluster transport
  private val shardTransportRef = actorSystem.actorOf(AkkaClusterShardingTransport.props("hyperstorage"))

  // shard processor actor
  private val shardProcessorRef = actorSystem.actorOf(
    ShardProcessor.props(shardTransportRef, workerSettings, tracker, shardSyncTimeout), "hyperstorage"
  )

  private val hyperbusAdapter = new HyperbusAdapter(hyperbus, shardProcessorRef, db, tracker, requestTimeout)

  private val hotPeriod = (hotRecovery.toMillis, failTimeout.toMillis)
  logger.info(s"Launching hot recovery $hotRecovery-$failTimeout")

  private val hotRecoveryRef = actorSystem.actorOf(HotRecoveryWorker.props(hotPeriod, db, shardProcessorRef, tracker, hotRecoveryRetry, backgroundTaskTimeout), "hot-recovery")
  shardProcessorRef ! SubscribeToShardStatus(hotRecoveryRef)

  private val stalePeriod = (staleRecovery.toMillis, hotRecovery.toMillis)
  logger.info(s"Launching stale recovery $staleRecovery-$hotRecovery")

  private val staleRecoveryRef = actorSystem.actorOf(StaleRecoveryWorker.props(stalePeriod, db, shardProcessorRef, tracker, staleRecoveryRetry, backgroundTaskTimeout), "stale-recovery")
  shardProcessorRef ! SubscribeToShardStatus(staleRecoveryRef)

  logger.info(s"Launching index manager")
  shardProcessorRef ! SubscribeToShardStatus(indexManagerRef)

  logger.info("Hyperstorage started!")

  // shutdown
  override def stopService(controlBreak: Boolean, timeout: FiniteDuration): Future[Unit] = {
    val step1 = Seq[Future[Any]](
      gracefulStop(staleRecoveryRef, timeout / 2, ShutdownRecoveryWorker),
      gracefulStop(hotRecoveryRef, timeout / 2, ShutdownRecoveryWorker),
      hyperbusAdapter.off().timeout(timeout / 2).runAsync
    )

    logger.info("Stopping Hyperstorage service...")
    Future.sequence(step1)
      .recover(logException("Service didn't stopped gracefully"))
      .flatMap { _ ⇒
        logger.info("Stopping processor actor...")
        gracefulStop(shardProcessorRef, shutdownTimeout * 4 / 5, ShutdownProcessor)
      }
      .recover(logException("ProcessorActor didn't stopped gracefully"))
      .flatMap { _ ⇒
        logger.info(s"Stopping ActorSystem: ${actorSystem.name}...")
        actorSystem.terminate()
      }
      .recover(logException("ActorSystem didn't stopped gracefully"))
      .map { _ ⇒
        db.close()
      }
      .recover(logException("ActorSystem didn't stopped gracefully"))
      .map { _ ⇒
        logger.info("Hyperstorage stopped.")
      }
  }

  private def logException(str: String): PartialFunction[Throwable, Unit] = {
    case e: Throwable ⇒
      logger.error(str, e)
  }
}
