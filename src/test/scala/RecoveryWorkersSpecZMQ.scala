/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import java.util.UUID

import akka.actor.{ActorSelection, Address}
import akka.pattern.gracefulStop
import akka.testkit.{TestActorRef, TestProbe}
import com.datastax.driver.core.utils.UUIDs
import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.internal.api.{BackgroundContentTaskResult, BackgroundContentTasksPost, NodeStatus}
import com.hypertino.hyperstorage.recovery.{HotRecoveryWorker, ShutdownRecoveryWorker, StaleRecoveryWorker}
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage.workers.primary.PrimaryWorker
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, FreeSpec, Matchers}

import scala.concurrent.duration._

class RecoveryWorkersSpecZMQ extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override def defaultClusterTransportIsZMQ: Boolean = true

  import ContentLogic._

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  import MessagingContext.Implicits.emptyContext

  "HotRecoveryWorker" should "work" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, scheduler))
    val path = "incomplete-" + UUID.randomUUID().toString
    val put = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    )
    worker ! primaryTask(path, put)
    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    val transactionUuids = whenReady(db.selectContent(path, "").runAsync) { result =>
      result.get.transactionList
    }

    val processorProbe = TestProbe("processor")
    val hotWorkerProps = HotRecoveryWorker.props(
      (60 * 1000l, -60 * 1000l), db, processorProbe.ref, tracker, 1.seconds, 10.seconds, scheduler
    )

    val hotWorker = TestActorRef(hotWorkerProps)
    val selfAddress = Address("tcp", "127.0.0.1")
    val shardData = ShardedClusterData(Map(
      selfAddress.toString → ShardNode(selfAddress.toString, NodeStatus.ACTIVE, NodeStatus.ACTIVE)
    ), selfAddress.toString, NodeStatus.ACTIVE)

    // start recovery check
    hotWorker ! UpdateShardStatus(self, NodeStatus.ACTIVE, shardData)

    val (backgroundWorkerTask2, br2) = processorProbe.expectTaskR[BackgroundContentTasksPost](max = 30.seconds)
    br.body.documentUri should equal(br2.body.documentUri)
    processorProbe.reply(Conflict(ErrorBody("test",Some("Testing worker behavior"))))
    val (bgTask3, br3) = processorProbe.expectTaskR[BackgroundContentTasksPost](max = 30.seconds)

    hotWorker ! processorProbe.reply(Ok(BackgroundContentTaskResult(br2.body.documentUri, transactionUuids.map(_.toString))))
    gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
  }

  "StaleRecoveryWorker" should "work" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, scheduler))
    val path = "incomplete-" + UUID.randomUUID().toString
    val put = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    )
    val millis = System.currentTimeMillis()
    worker ! primaryTask(path, put)
    val (backgroundWorkerTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    val content = db.selectContent(path, "").runAsync.futureValue.get
    val newTransactionUuid = UUIDs.startOf(millis - 5 * 60 * 1000l)
    val newContent = content.copy(
      transactionList = List(newTransactionUuid) // original transaction becomes abandoned
    )
    val transaction = selectTransactions(content.transactionList, content.uri, db).head
    val newTransaction = transaction.copy(
      dtQuantum = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(newTransactionUuid)),
      uuid = newTransactionUuid
    )
    db.insertContent(newContent).runAsync.futureValue
    db.insertTransaction(newTransaction).runAsync.futureValue

    db.updateCheckpoint(transaction.partition, transaction.dtQuantum - 10).runAsync.futureValue // checkpoint to - 10 minutes

    val processorProbe = TestProbe("processor")
    val staleWorkerProps = StaleRecoveryWorker.props(
      (60 * 1000l, -60 * 1000l), db, processorProbe.ref, tracker, 1.seconds, 2.seconds, scheduler
    )

    val hotWorker = TestActorRef(staleWorkerProps)
    val selfAddress = Address("tcp", "127.0.0.1")
    val shardData = ShardedClusterData(Map(
      selfAddress.toString → ShardNode(selfAddress.toString, NodeStatus.ACTIVE, NodeStatus.ACTIVE)
    ), selfAddress.toString, NodeStatus.ACTIVE)

    // start recovery check
    hotWorker ! UpdateShardStatus(self, NodeStatus.ACTIVE, shardData)

    val (backgroundWorkerTask2, br2) = processorProbe.expectTaskR[BackgroundContentTasksPost](max = 30.seconds)
    br.body.documentUri should equal(br2.body.documentUri)
    processorProbe.reply(Conflict(ErrorBody("test",Some("Testing worker behavior"))))

    eventually {
      db.selectCheckpoint(transaction.partition).runAsync.futureValue shouldBe Some(newTransaction.dtQuantum - 1)
    }

    val (backgroundWorkerTask3, br3) = processorProbe.expectTaskR[BackgroundContentTasksPost](max = 30.seconds)
    hotWorker ! processorProbe.reply(Ok(BackgroundContentTaskResult(br2.body.documentUri, newContent.transactionList.map(_.toString))))

    eventually {
      db.selectCheckpoint(transaction.partition).runAsync.futureValue.get shouldBe >(newTransaction.dtQuantum)
    }

    val (backgroundWorkerTask4, br4) = processorProbe.expectTaskR[BackgroundContentTasksPost](max = 30.seconds) // this is abandoned
    hotWorker ! processorProbe.reply(Ok(BackgroundContentTaskResult(br4.body.documentUri, List.empty)))

    gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
  }
}

class RecoveryWorkersSpecAkkaCluster extends RecoveryWorkersSpecZMQ {
  override def defaultClusterTransportIsZMQ: Boolean = false
}
