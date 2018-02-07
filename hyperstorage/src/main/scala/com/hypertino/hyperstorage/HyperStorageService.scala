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
import com.hypertino.hyperstorage.sharding.akkacluster.{AkkaClusterTransport, AkkaClusterTransportActor}
import com.hypertino.hyperstorage.sharding.consulzmq.ZMQCClusterTransport
import com.hypertino.hyperstorage.sharding.{ShardProcessor, ShutdownProcessor, SubscribeToShardStatus}
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.metrics.MetricsTracker
import com.hypertino.service.control.api.Service
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import scaldi.{Injectable, Injector}

import scala.concurrent.Future
import scala.concurrent.duration._

case class HyperStorageConfig(
                               shutdownTimeout: FiniteDuration,
                               shardSyncTimeout: FiniteDuration,
                               maxWorkers: Int,
                               backgroundTaskTimeout: FiniteDuration,
                               maxIncompleteTransactions: Int,
                               maxBatchSizeInBytes: Int,
                               requestTimeout: FiniteDuration,
                               failTimeout: FiniteDuration,
                               hotRecovery: FiniteDuration,
                               hotRecoveryRetry: FiniteDuration,
                               staleRecovery: FiniteDuration,
                               staleRecoveryRetry: FiniteDuration,
                               transactionTtl: FiniteDuration,
                               clusterManager: String
                             )

class HyperStorageService(implicit val scheduler: Scheduler,
                          implicit val injector: Injector) extends Service with Injectable with StrictLogging {

  logger.info(s"Starting Hyperstorage service v${BuildInfo.version}...")

  // configuration
  private val config: Config = inject[Config]
  logger.info(s"Hyperstorage configuration: $config")

  import com.hypertino.binders.config.ConfigBinders._
  private val serviceConfig = config.getValue("hyperstorage").read[HyperStorageConfig]

  private val zmqClusterManager: Boolean = if (serviceConfig.clusterManager == "zmqc") true
  else if (serviceConfig.clusterManager == "akka-cluster") false
  else throw new RuntimeException(s"Invalid cluster-manager specified: ${serviceConfig.clusterManager}")

  private val connector: CassandraConnector = inject[CassandraConnector]

  // metrics tracker
  private val tracker = inject[MetricsTracker]
  MetricsReporter.startReporter(tracker)

  // initialize
  logger.info(s"Initializing hyperbus...")
  private val hyperbus = inject[Hyperbus]

  // currently we rely on the name of system
  private val actorSystem = if (zmqClusterManager) {
    ActorSystem("hyperstorage", config.getConfig("hyperstorage.regular-actor-system"))
  }
  else {
    val as = ActorSystem("hyperstorage", config.getConfig("hyperstorage.cluster-actor-system"))
    Cluster(as)
    as
  }

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

  private val indexManagerProps = IndexManager.props(hyperbus, db, tracker, serviceConfig.maxWorkers, scheduler)
  private val indexManagerRef = actorSystem.actorOf(
    indexManagerProps, "index-manager"
  )

  // worker actor todo: recovery job
  private val workerSettings = HyperstorageWorkerSettings(hyperbus, db, tracker, serviceConfig.maxWorkers,
    serviceConfig.maxWorkers, serviceConfig.backgroundTaskTimeout, serviceConfig.maxIncompleteTransactions,
    serviceConfig.maxBatchSizeInBytes, serviceConfig.transactionTtl, indexManagerRef, scheduler)

  // shard shard cluster transport
  private val shardTransport = if (zmqClusterManager) {
    new ZMQCClusterTransport(
      config.getConfig(s"hyperstorage.zmq-cluster-manager")
    )
  } else {
    val shardTransportRef = actorSystem.actorOf(AkkaClusterTransportActor.props("hyperstorage"))
    new AkkaClusterTransport(shardTransportRef)
  }

  // shard processor actor
  private val shardProcessorRef = actorSystem.actorOf(
    ShardProcessor.props(shardTransport, workerSettings, tracker, serviceConfig.shardSyncTimeout), "hyperstorage"
  )

  private val hyperbusAdapter = new HyperbusAdapter(hyperbus, shardProcessorRef, db, tracker, serviceConfig.requestTimeout)

  private val hotPeriod = (serviceConfig.hotRecovery.toMillis, serviceConfig.failTimeout.toMillis)
  logger.info(s"Launching hot recovery ${serviceConfig.hotRecovery}-${serviceConfig.failTimeout}")

  private val hotRecoveryRef = actorSystem.actorOf(HotRecoveryWorker.props(hotPeriod, db, shardProcessorRef, tracker,
    serviceConfig.hotRecoveryRetry, serviceConfig.backgroundTaskTimeout, serviceConfig.transactionTtl, scheduler), "hot-recovery")
  shardProcessorRef ! SubscribeToShardStatus(hotRecoveryRef)

  private val stalePeriod = (serviceConfig.staleRecovery.toMillis, serviceConfig.hotRecovery.toMillis)
  logger.info(s"Launching stale recovery ${serviceConfig.staleRecovery}-${serviceConfig.hotRecovery}")

  private val staleRecoveryRef = actorSystem.actorOf(StaleRecoveryWorker.props(stalePeriod, db, shardProcessorRef, tracker,
    serviceConfig.staleRecoveryRetry, serviceConfig.backgroundTaskTimeout, serviceConfig.transactionTtl, scheduler), "stale-recovery")
  shardProcessorRef ! SubscribeToShardStatus(staleRecoveryRef)

  logger.info(s"Launching index manager")
  shardProcessorRef ! SubscribeToShardStatus(indexManagerRef)

  logger.info("Hyperstorage is STARTED")

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
        gracefulStop(shardProcessorRef, serviceConfig.shutdownTimeout * 4 / 5, ShutdownProcessor)
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
        if (zmqClusterManager) {
          logger.info(s"Stopping ZMQCClusterTransport...")
          shardTransport.asInstanceOf[ZMQCClusterTransport].close()
        }
      }
      .recover(logException("ZMQCClusterTransport didn't stopped gracefully"))
      .map { _ ⇒
        logger.info("Hyperstorage is STOPPED")
      }
  }

  private def logException(str: String): PartialFunction[Throwable, Unit] = {
    case e: Throwable ⇒
      logger.error(str, e)
  }
}
