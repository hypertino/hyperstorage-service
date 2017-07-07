import java.util.UUID

import akka.actor._
import akka.cluster.Cluster
import akka.testkit.{TestActorRef, _}
import akka.util.Timeout
import com.codahale.metrics.ScheduledReporter
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.ConfigFactory
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{DynamicBody, DynamicResponse, Response, StandardResponse}
import com.hypertino.hyperbus.serialization.MessageReader
import com.hypertino.hyperbus.transport.api.{TransportConfigurationLoader, TransportManager}
import com.hypertino.hyperstorage.db.{Db, Transaction}
import com.hypertino.hyperstorage.indexing.IndexManager
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.workers.primary.PrimaryWorker
import com.hypertino.hyperstorage.workers.secondary.SecondaryWorker
import com.hypertino.metrics.MetricsTracker
import com.hypertino.metrics.modules.ConsoleReporterModule
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, Matchers}
import org.slf4j.LoggerFactory
import scaldi.Injectable

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

trait TestHelpers extends Matchers with BeforeAndAfterEach with ScalaFutures with Injectable {
  this: org.scalatest.BeforeAndAfterEach with org.scalatest.Suite =>
  private[this] val log = LoggerFactory.getLogger(getClass)
  implicit val injector = new ConsoleReporterModule(Duration.Inf)
  val tracker = inject[MetricsTracker]
  val reporter = inject[ScheduledReporter]
  //val _actorSystems = TrieMap[Int, ActorSystem]()
  val _hyperbuses = TrieMap[Int, Hyperbus]()

  implicit def scheduler = {
    monix.execution.Scheduler.Implicits.global
  }

  def createShardProcessor(groupName: String, workerCount: Int = 1, waitWhileActivates: Boolean = true)(implicit actorSystem: ActorSystem) = {
    val workerSettings = Map(groupName → (Props[TestWorker], workerCount, "test-worker"))
    val fsm = new TestFSMRef[ShardMemberStatus, ShardedClusterData, ShardProcessor](actorSystem,
      ShardProcessor.props(workerSettings, "hyper-storage", tracker).withDispatcher("deque-dispatcher"),
      GuardianExtractor.guardian(actorSystem),
      "hyper-storage"
    )
    //val fsm = TestFSMRef(new ShardProcessor(Props[TestWorker], workerCount), "hyperstorage")
    //val ShardProcessor: TestActorRef[ShardProcessor] = fsm
    fsm.stateName should equal(ShardMemberStatus.Activating)
    if (waitWhileActivates) {
      val t = new TestKit(actorSystem)
      t.awaitCond(fsm.stateName == ShardMemberStatus.Active)
    }
    fsm
  }

  def integratedHyperbus(db: Db, waitWhileActivates: Boolean = true): Hyperbus = {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    val indexManager = TestActorRef(IndexManager.props(hyperbus, db, tracker, 1))
    val workerProps = PrimaryWorker.props(hyperbus, db, tracker, 10.seconds)
    val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, tracker, indexManager, scheduler)
    val workerSettings = Map(
      "hyper-storage-primary-worker" → (workerProps, 1, "pgw-"),
      "hyper-storage-secondary-worker" → (secondaryWorkerProps, 1, "sgw-")
    )

    val processor = new TestFSMRef[ShardMemberStatus, ShardedClusterData, ShardProcessor](system,
      ShardProcessor.props(workerSettings, "hyper-storage", tracker).withDispatcher("deque-dispatcher"),
      GuardianExtractor.guardian(system),
      "hyper-storage"
    )

    processor.stateName should equal(ShardMemberStatus.Activating)
    if (waitWhileActivates) {
      awaitCond(processor.stateName == ShardMemberStatus.Active)
    }

    processor ! SubscribeToShardStatus(indexManager)

    val adapter = new HyperbusAdapter(hyperbus, processor, db, tracker, 20.seconds)
    Thread.sleep(2000)
    hyperbus
  }

  def testKit(index: Int = 0) = new TestKit(testActorSystem(index)) with ImplicitSender

  def testActorSystem(index: Int = 0) = {
    testHyperbus(index)
    val config = ConfigFactory.load().getConfig(s"actor-system-registry.actor-system-$index")
    ActorSystem(s"actor-system-$index", config)
  }

  def testHyperbus(index: Int = 0) = {
    val hb = _hyperbuses.getOrElseUpdate(index, {
      val config = ConfigFactory.load().getConfig(s"hyperbus-$index")
      new Hyperbus(config)
    }
    )
    hb
  }


  def shutdownCluster(index: Int = 0): Unit = {
    _hyperbuses.get(index).foreach { hb ⇒
      hb.shutdown(5.seconds)
      Thread.sleep(1000)
    }
  }

  def selectTransactions(uuids: Seq[UUID], path: String, db: Db): Seq[Transaction] = {
    uuids flatMap { uuid ⇒
      val partition = TransactionLogic.partitionFromUri(path)
      val qt = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(uuid))
      whenReady(db.selectTransaction(qt, partition, path, uuid)) { mon ⇒
        mon
      }
    }
  }

  def shutdownShardProcessor(fsm: TestFSMRef[ShardMemberStatus, ShardedClusterData, ShardProcessor])(implicit actorSystem: ActorSystem) = {
    val probe = TestProbe()
    probe watch fsm
    fsm ! ShutdownProcessor
    new TestKit(actorSystem).awaitCond(fsm.stateName == ShardMemberStatus.Deactivating, 10.second)
    probe.expectTerminated(fsm, 10.second)
  }

  override def afterEach() {
    log.info("------- SHUTTING DOWN HYPERBUSES -------- ")
    _hyperbuses.foreach {
      case (index, hb) ⇒ {
        Await.result(hb.shutdown(10.second).runAsync, 11.second)
      }
    }
    _hyperbuses.clear()
    Thread.sleep(500)
    log.info("------- HYPERBUSES WERE SHUT DOWN -------- ")
    reporter.report()
  }

  implicit class TaskEx(t: ShardTask) {
    def isProcessingStarted = t.asInstanceOf[TestShardTask].isProcessingStarted

    def isProcessed = t.asInstanceOf[TestShardTask].isProcessed

    def processorPath = t.asInstanceOf[TestShardTask].processActorPath getOrElse ""
  }

  def response(content: String): DynamicResponse = MessageReader.fromString(content, StandardResponse.apply)
}

case class TestShardTask(key: String, value: String,
                         sleep: Int = 0,
                         ttl: Long = System.currentTimeMillis() + 60 * 1000,
                         id: UUID = UUID.randomUUID()) extends ShardTask {
  def group = "test-group"

  def isExpired = ttl < System.currentTimeMillis()

  def processingStarted(actorPath: String): Unit = {
    ProcessedRegistry.tasksStarted += id → (actorPath, this)
  }

  def processed(actorPath: String): Unit = {
    ProcessedRegistry.tasks += id → (actorPath, this)
  }

  def isProcessed = ProcessedRegistry.tasks.get(id).isDefined

  def isProcessingStarted = ProcessedRegistry.tasksStarted.get(id).isDefined

  override def toString = s"TestTask($key, $value, $sleep, $ttl, #${System.identityHashCode(this)}, actor: $processActorPath"

  def processActorPath: Option[String] = ProcessedRegistry.tasks.get(id) map { kv ⇒ kv._1 }
}

class TestWorker extends Actor with ActorLogging {
  def receive = {
    case task: TestShardTask => {
      if (task.isExpired) {
        log.error(s"Task is expired: $task")
      } else {
        val c = Cluster(context.system)
        val path = c.selfAddress + "/" + self.path.toString
        log.info(s"Processing task: $task")
        task.processingStarted(path)
        if (task.sleep > 0) {
          Thread.sleep(task.sleep)
        }
        task.processed(path)
        log.info(s"Task processed: $task")
      }
      sender() ! ShardTaskComplete(task, None)
    }
  }
}

object ProcessedRegistry {
  val tasks = TrieMap[UUID, (String, TestShardTask)]()
  val tasksStarted = TrieMap[UUID, (String, TestShardTask)]()
}
