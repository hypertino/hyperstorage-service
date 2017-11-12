import java.util.UUID

import akka.actor.{ActorSelection, Address}
import akka.pattern.gracefulStop
import akka.testkit.{TestActorRef, TestProbe}
import com.datastax.driver.core.utils.UUIDs
import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.internal.api.NodeStatus
import com.hypertino.hyperstorage.recovery.{HotRecoveryWorker, ShutdownRecoveryWorker, StaleRecoveryWorker}
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage.workers.primary.{PrimaryContentTask, PrimaryWorker}
import com.hypertino.hyperstorage.workers.secondary.{BackgroundContentTask, BackgroundContentTaskFailedException, BackgroundContentTaskResult}
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, FreeSpec, Matchers}

import scala.concurrent.duration._

class RecoveryWorkersSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  import ContentLogic._

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  import MessagingContext.Implicits.emptyContext

  "HotRecoveryWorker" should "work" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))
    val path = "incomplete-" + UUID.randomUUID().toString
    val taskStr1 = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    ).serializeToString
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskStr1, expectsResult=true,isClientOperation=true)
    val backgroundWorkerTask = expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    val transactionUuids = whenReady(db.selectContent(path, "")) { result =>
      result.get.transactionList
    }

    val processorProbe = TestProbe("processor")
    val hotWorkerProps = HotRecoveryWorker.props(
      (60 * 1000l, -60 * 1000l), db, processorProbe.ref, tracker, 1.seconds, 10.seconds
    )

    val hotWorker = TestActorRef(hotWorkerProps)
    val selfAddress = Address("tcp", "127.0.0.1")
    val shardData = ShardedClusterData(Map(
      selfAddress.toString → ShardNode(ActorSelection(self, ""), NodeStatus.ACTIVE, NodeStatus.ACTIVE, selfAddress.toString)
    ), selfAddress.toString, NodeStatus.ACTIVE)

    // start recovery check
    hotWorker ! UpdateShardStatus(self, NodeStatus.ACTIVE, shardData)

    val backgroundWorkerTask2 = processorProbe.expectMsgType[BackgroundContentTask](max = 30.seconds)
    backgroundWorkerTask.documentUri should equal(backgroundWorkerTask2.documentUri)
    processorProbe.reply(BackgroundContentTaskFailedException(backgroundWorkerTask2.documentUri, "Testing worker behavior"))
    val backgroundWorkerTask3 = processorProbe.expectMsgType[BackgroundContentTask](max = 30.seconds)
    hotWorker ! processorProbe.reply(BackgroundContentTaskResult(backgroundWorkerTask2.documentUri, transactionUuids))
    gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
  }

  "StaleRecoveryWorker" should "work" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))
    val path = "incomplete-" + UUID.randomUUID().toString
    val taskStr1 = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    ).serializeToString
    val millis = System.currentTimeMillis()
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskStr1, expectsResult=true,isClientOperation=true)
    val backgroundWorkerTask = expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    val content = db.selectContent(path, "").futureValue.get
    val newTransactionUuid = UUIDs.startOf(millis - 5 * 60 * 1000l)
    val newContent = content.copy(
      transactionList = List(newTransactionUuid) // original transaction becomes abandoned
    )
    val transaction = selectTransactions(content.transactionList, content.uri, db).head
    val newTransaction = transaction.copy(
      dtQuantum = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(newTransactionUuid)),
      uuid = newTransactionUuid
    )
    db.insertContent(newContent).futureValue
    db.insertTransaction(newTransaction).futureValue

    db.updateCheckpoint(transaction.partition, transaction.dtQuantum - 10) // checkpoint to - 10 minutes

    val processorProbe = TestProbe("processor")
    val staleWorkerProps = StaleRecoveryWorker.props(
      (60 * 1000l, -60 * 1000l), db, processorProbe.ref, tracker, 1.seconds, 2.seconds
    )

    val hotWorker = TestActorRef(staleWorkerProps)
    val selfAddress = Address("tcp", "127.0.0.1")
    val shardData = ShardedClusterData(Map(
      selfAddress.toString → ShardNode(ActorSelection(self, ""), NodeStatus.ACTIVE, NodeStatus.ACTIVE, selfAddress.toString)
    ), selfAddress.toString, NodeStatus.ACTIVE)

    // start recovery check
    hotWorker ! UpdateShardStatus(self, NodeStatus.ACTIVE, shardData)

    val backgroundWorkerTask2 = processorProbe.expectMsgType[BackgroundContentTask](max = 30.seconds)
    backgroundWorkerTask.documentUri should equal(backgroundWorkerTask2.documentUri)
    processorProbe.reply(BackgroundContentTaskFailedException(backgroundWorkerTask2.documentUri, "Testing worker behavior"))

    eventually {
      db.selectCheckpoint(transaction.partition).futureValue shouldBe Some(newTransaction.dtQuantum - 1)
    }

    val backgroundWorkerTask3 = processorProbe.expectMsgType[BackgroundContentTask](max = 30.seconds)
    hotWorker ! processorProbe.reply(BackgroundContentTaskResult(backgroundWorkerTask2.documentUri, newContent.transactionList))

    eventually {
      db.selectCheckpoint(transaction.partition).futureValue.get shouldBe >(newTransaction.dtQuantum)
    }

    val backgroundWorkerTask4 = processorProbe.expectMsgType[BackgroundContentTask](max = 30.seconds) // this is abandoned
    hotWorker ! processorProbe.reply(BackgroundContentTaskResult(backgroundWorkerTask4.documentUri, List()))

    gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
  }
}

