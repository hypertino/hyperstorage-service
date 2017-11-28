/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import java.util.UUID

import akka.actor._
import akka.cluster.Cluster
import akka.testkit.{TestActorRef, _}
import com.codahale.metrics.ScheduledReporter
import com.datastax.driver.core.utils.UUIDs
import com.hypertino.binders.value.{Null, Obj, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.annotations.{body, request}
import com.hypertino.hyperbus.model.{BadGateway, Body, DefinedResponse, DynamicBody, DynamicResponse, MessagingContext, Method, Ok, Request, RequestBase, ServiceUnavailable, StandardResponse, Status}
import com.hypertino.hyperbus.serialization.MessageReader
import com.hypertino.hyperbus.util.IdGenerator
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.db.{Db, Transaction}
import com.hypertino.hyperstorage.indexing.IndexManager
import com.hypertino.hyperstorage.internal.api.NodeStatus
import com.hypertino.hyperstorage.modules.{HyperStorageServiceModule, SystemServicesModule}
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage.sharding.akkacluster.AkkaClusterShardingTransport
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.hyperstorage.workers.primary.PrimaryExtra
import com.hypertino.metrics.MetricsTracker
import com.hypertino.metrics.modules.{ConsoleReporterModule, MetricsModule}
import com.hypertino.service.config.ConfigModule
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, Matchers}
import scaldi.Injectable

import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

trait TestHelpers extends Matchers with BeforeAndAfterEach with ScalaFutures with Injectable with StrictLogging{
  this: org.scalatest.BeforeAndAfterEach with org.scalatest.Suite =>

  implicit val injector = new SystemServicesModule :: new HyperStorageServiceModule :: new MetricsModule ::
    new ConsoleReporterModule(Duration.Inf).injector :: ConfigModule()

  val tracker = inject[MetricsTracker]
  val reporter = inject[ScheduledReporter]
  val _actorSystems = TrieMap[Int, ActorSystem]()
  val _hyperbuses = TrieMap[Int, Hyperbus]()

  implicit def scheduler = inject [monix.execution.Scheduler]

  def createShardProcessor(groupName: String, workerCount: Int = 1, waitWhileActivates: Boolean = true)(implicit actorSystem: ActorSystem) = {
    val clusterTransport = TestActorRef(AkkaClusterShardingTransport.props("hyperstorage"))
    val workerSettings = Map(
      groupName → WorkerGroupSettings(Props[TestWorker], workerCount, "test-worker", Seq(TestShardTaskPost))
    )
    val fsm = new TestFSMRef[String, ShardedClusterData, ShardProcessor](actorSystem,
      ShardProcessor.props(clusterTransport, workerSettings, tracker).withDispatcher("deque-dispatcher"),
      GuardianExtractor.guardian(actorSystem),
      "hyperstorage"
    )
    //val fsm = TestFSMRef(new ShardProcessor(Props[TestWorker], workerCount), "hyperstorage")
    //val ShardProcessor: TestActorRef[ShardProcessor] = fsm
    fsm.stateName should equal(NodeStatus.ACTIVATING)
    if (waitWhileActivates) {
      val t = new TestKit(actorSystem)
      t.awaitCond(fsm.stateName == NodeStatus.ACTIVE)
    }
    fsm
  }

  def integratedHyperbus(db: Db, waitWhileActivates: Boolean = true): Hyperbus = {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    val indexManager = TestActorRef(IndexManager.props(hyperbus, db, tracker, 1))
    val workerSettings = HyperstorageWorkerSettings(hyperbus, db, tracker, 1, 1, 10.seconds, indexManager, scheduler)

    val clusterTransport = TestActorRef(AkkaClusterShardingTransport.props("hyperstorage"))
    val processor = new TestFSMRef[String, ShardedClusterData, ShardProcessor](system,
      ShardProcessor.props(clusterTransport, workerSettings, tracker).withDispatcher("deque-dispatcher"),
      GuardianExtractor.guardian(system),
      "hyperstorage"
    )

    processor.stateName should equal(NodeStatus.ACTIVATING)
    if (waitWhileActivates) {
      awaitCond(processor.stateName == NodeStatus.ACTIVE)
    }

    processor ! SubscribeToShardStatus(indexManager)

    val adapter = new HyperbusAdapter(hyperbus, processor, db, tracker, 20.seconds)
    Thread.sleep(2000)
    hyperbus
  }

  def testKit(index: Int = 0) = new TestKit(testActorSystem(index)) with ImplicitSender

  def testActorSystem(index: Int = 0) = {
    testHyperbus(index)
    _actorSystems.getOrElseUpdate(index, {
      val config = ConfigFactory.load().getConfig(s"actor-system-registry.hyperstorage-$index")
      ActorSystem(config.getString("actor-system-name"), config)
    })
  }

  def testHyperbus(index: Int = 0) = {
    val hb = _hyperbuses.getOrElseUpdate(index, {
      val config = ConfigFactory.load().getConfig(s"hyperbus-$index")
      new Hyperbus(config)
    }
    )
    hb
  }

  // todo: !!!!! implement graceful shutdown in main service !!!!!!
  def shutdownCluster(index: Int = 0): Unit = {
    _hyperbuses.get(index).foreach { hb ⇒
      hb.shutdown(5.seconds)
      Thread.sleep(1000)
      _hyperbuses.remove(index)
    }
    _actorSystems.get(index).foreach { as ⇒
      val cluster = Cluster(as)
      val me = cluster.selfUniqueAddress
      cluster.leave(me.address)
      Thread.sleep(1000)
      Await.result(as.terminate(), 1.seconds)
      _actorSystems.remove(index)
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

  def shutdownShardProcessor(fsm: TestFSMRef[String, ShardedClusterData, ShardProcessor])(implicit actorSystem: ActorSystem) = {
    val probe = TestProbe()
    probe watch fsm
    fsm ! ShutdownProcessor
    new TestKit(actorSystem).awaitCond(fsm.stateName == NodeStatus.DEACTIVATING, 10.second)
    probe.expectTerminated(fsm, 10.second)
  }

  override def afterEach() {
    logger.info("------- SHUTTING DOWN HYPERBUSES -------- ")
    _hyperbuses.foreach {
      case (index, hb) ⇒ {
        Await.result(hb.shutdown(10.second).runAsync, 11.second)
      }
    }
    _actorSystems.foreach { as ⇒
      Await.result(as._2.terminate(), 1.seconds);
    }
    _hyperbuses.clear()
    _actorSystems.clear()
    Thread.sleep(500)
    logger.info("------- HYPERBUSES WERE SHUT DOWN -------- ")
    reporter.report()
  }

//  implicit class TaskEx(t: TestShardTaskPost) {
//    def isProcessingStarted = t.asInstanceOf[TestShardTask].isProcessingStarted
//
//    def isProcessed = t.asInstanceOf[TestShardTask].isProcessed
//
//    def processorPath = t.asInstanceOf[TestShardTask].processActorPath getOrElse ""
//  }

  implicit class TestActorEx(t: TestKitBase)(implicit val as: ActorSystem) {
    def expectTaskR[T](max: FiniteDuration = 20.seconds)(implicit tag: ClassTag[T]): (LocalTask,T) = {
      t.expectMsgPF(max) {
        case l @ LocalTask(_, _, _, _, r, _) if tag.runtimeClass.isInstance(r) ⇒ (l, r.asInstanceOf[T])
      }
    }
  }

  //def response(content: String): DynamicResponse = MessageReader.fromString(content, StandardResponse.apply)

  def primaryTask(key: String, request: RequestBase,
                  ttl: Long = System.currentTimeMillis() + 10000l,
                  extra: Value = Obj.from(PrimaryExtra.INTERNAL_OPERATION → false),
                  expectsResult: Boolean = true) = LocalTask(key, HyperstorageWorkerSettings.PRIMARY, ttl, expectsResult, request, extra)

  def testTask(key: String, value: String, sleep: Int = 0): (LocalTask, TestShardTaskPost) = {
    implicit val mcx = MessagingContext.empty
    val t = TestShardTaskPost(
      TaskShardTaskBody(value, sleep)
    )
    (LocalTask(key, "test-group", System.currentTimeMillis() + 20000, true, t, Null), t)
  }
}

@body("test-shard-task-body")
case class TaskShardTaskBody(value: String,
                             sleep: Int = 0,
                             id: String = IdGenerator.create()) extends Body

@body("test-shard-task-result-body")
case class TaskShardTaskResultBody(value: String,
                                   id: String = IdGenerator.create()) extends Body

@request(Method.POST, "hb://test-shard-tasks")
case class TestShardTaskPost(body: TaskShardTaskBody) extends Request[TaskShardTaskBody]
  with DefinedResponse[Ok[TaskShardTaskResultBody]] {
  def processingStarted(actorPath: String): Unit = {
    ProcessedRegistry.tasksStarted += body.id → (actorPath, this)
  }

  def processed(actorPath: String): Unit = {
    ProcessedRegistry.tasks += body.id → (actorPath, this)
  }

  def isProcessed = ProcessedRegistry.tasks.get(body.id).isDefined

  def isProcessingStarted = ProcessedRegistry.tasksStarted.get(body.id).isDefined

  def processorPath: String = ProcessedRegistry.tasks.get(body.id) map { kv ⇒ kv._1 } getOrElse ""
}

class TestWorker extends Actor with StrictLogging {
  def receive = {
    case task: LocalTask => {
      if (task.isExpired) {
        logger.error(s"Task is expired: $task")
        sender() ! WorkerTaskResult(task, ServiceUnavailable()(MessagingContext.empty), Null)
      } else {
        implicit val request = task.request.asInstanceOf[TestShardTaskPost]

        val c = Cluster(context.system)
        val path = c.selfAddress + "/" + self.path.toString
        logger.info(s"Processing task: $task")
        request.processingStarted(path)
        if (request.body.sleep > 0) {
          Thread.sleep(request.body.sleep)
        }
        request.processed(path)
        logger.info(s"Task processed: $task")
        logger.info(s"Replying to ${sender()} that task $task is complete")
        sender() ! WorkerTaskResult(task, Ok(TaskShardTaskResultBody(request.body.value, request.body.id)))
      }
    }
  }
}

object ProcessedRegistry {
  val tasks = TrieMap[String, (String, TestShardTaskPost)]()
  val tasksStarted = TrieMap[String, (String, TestShardTaskPost)]()
}
