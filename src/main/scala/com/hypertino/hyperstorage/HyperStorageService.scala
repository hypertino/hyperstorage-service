package com.hypertino.hyperstorage

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.pattern.gracefulStop
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperstorage.db.Db
import com.hypertino.hyperstorage.indexing.IndexManager
import com.hypertino.hyperstorage.metrics.MetricsReporter
import com.hypertino.hyperstorage.recovery.{HotRecoveryWorker, ShutdownRecoveryWorker, StaleRecoveryWorker}
import com.hypertino.hyperstorage.sharding.{ShardProcessor, ShutdownProcessor, SubscribeToShardStatus}
import com.hypertino.hyperstorage.workers.primary.PrimaryWorker
import com.hypertino.hyperstorage.workers.secondary.SecondaryWorker
import com.hypertino.metrics.MetricsTracker
import com.hypertino.service.control.api.{Console, Service}
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler
import org.slf4j.LoggerFactory
import scaldi.{Injectable, Injector}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
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
                          implicit val injector: Injector) extends Service with Injectable {

  private var log = LoggerFactory.getLogger(getClass)
  log.info(s"Starting HyperStorage service v${BuildInfo.version}...")

  // configuration
  private val config: Config = inject[Config]
  private val connector: CassandraConnector = inject[CassandraConnector]

  import com.hypertino.binders.config.ConfigBinders._

  private val serviceConfig = config.getValue("hyper-storage").read[HyperStorageConfig]

  log.info(s"HyperStorage configuration: $config")

  // metrics tracker
  private val tracker = inject[MetricsTracker]
  MetricsReporter.startReporter(tracker)

  import serviceConfig._

  // initialize
  log.info(s"Initializing hyperbus...")
  private val hyperbus = inject[Hyperbus]

  // currently we rely on the name of system
  private val actorSystem = ActorSystem("hyper-storage")
  // ActorSystemRegistry.get("eu-inn").get
  private val cluster = Cluster(actorSystem)

  //
  private val db = new Db(connector)
  // trigger connect to c* but continue initialization
  try {
    log.info(s"Initializing database connection...")
    db.preStart()
  } catch {
    case NonFatal(e) ⇒
      log.error(s"Can't create C* session", e)
  }

  private val indexManagerProps = IndexManager.props(hyperbus, db, tracker, maxWorkers)
  private val indexManagerRef = actorSystem.actorOf(
    indexManagerProps, "index-manager"
  )

  // worker actor todo: recovery job
  private val primaryWorkerProps = PrimaryWorker.props(hyperbus, db, tracker, backgroundTaskTimeout)
  private val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, tracker, indexManagerRef, scheduler)
  private val workerSettings = Map(
    "hyper-storage-primary-worker" → (primaryWorkerProps, maxWorkers, "pgw-"),
    "hyper-storage-secondary-worker" → (secondaryWorkerProps, maxWorkers, "sgw-")
  )

  // shard processor actor
  private val shardProcessorRef = actorSystem.actorOf(
    ShardProcessor.props(workerSettings, "hyper-storage", tracker, shardSyncTimeout), "hyper-storage"
  )

  private val hyperbusAdapter = new HyperbusAdapter(hyperbus, shardProcessorRef, db, tracker, requestTimeout)

  private val hotPeriod = (hotRecovery.toMillis, failTimeout.toMillis)
  log.info(s"Launching hot recovery $hotRecovery-$failTimeout")

  private val hotRecoveryRef = actorSystem.actorOf(HotRecoveryWorker.props(hotPeriod, db, shardProcessorRef, tracker, hotRecoveryRetry, backgroundTaskTimeout), "hot-recovery")
  shardProcessorRef ! SubscribeToShardStatus(hotRecoveryRef)

  private val stalePeriod = (staleRecovery.toMillis, hotRecovery.toMillis)
  log.info(s"Launching stale recovery $staleRecovery-$hotRecovery")

  private val staleRecoveryRef = actorSystem.actorOf(StaleRecoveryWorker.props(stalePeriod, db, shardProcessorRef, tracker, staleRecoveryRetry, backgroundTaskTimeout), "stale-recovery")
  shardProcessorRef ! SubscribeToShardStatus(staleRecoveryRef)

  log.info(s"Launching index manager")
  shardProcessorRef ! SubscribeToShardStatus(indexManagerRef)

  log.info("HyperStorage started!")

  // shutdown
  override def stopService(controlBreak: Boolean, timeout: FiniteDuration): Future[Unit] = {
    val step1 = Seq[Future[Any]](
      gracefulStop(staleRecoveryRef, timeout / 2, ShutdownRecoveryWorker),
      gracefulStop(hotRecoveryRef, timeout / 2, ShutdownRecoveryWorker),
      hyperbusAdapter.off().timeout(timeout / 2).runAsync
    )

    log.info("Stopping HyperStorage service...")
    Future.sequence(step1)
      .recover(logException("Service didn't stopped gracefully"))
      .flatMap { _ ⇒
        log.info("Stopping processor actor...")
        gracefulStop(shardProcessorRef, shutdownTimeout * 4 / 5, ShutdownProcessor)
      }
      .recover(logException("ProcessorActor didn't stopped gracefully"))
      .flatMap { _ ⇒
        log.info(s"Stopping ActorSystem: ${actorSystem.name}...")
        actorSystem.terminate()
      }
      .recover(logException("ActorSystem didn't stopped gracefully"))
      .map { _ ⇒
        db.close()
      }
      .recover(logException("ActorSystem didn't stopped gracefully"))
      .map { _ ⇒
        log.info("HyperStorage stopped.")
      }
  }

  private def logException(str: String): PartialFunction[Throwable, Unit] = {
    case NonFatal(e) ⇒
      log.error(str, e)
  }
}
