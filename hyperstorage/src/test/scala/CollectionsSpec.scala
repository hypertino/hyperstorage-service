/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import java.util.UUID

import akka.testkit.TestActorRef
import com.hypertino.binders.value._
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model._
import com.hypertino.hyperbus.serialization.SerializationOptions
import com.hypertino.hyperbus.util.SeqGenerator
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.internal.api.{BackgroundContentTaskResult, BackgroundContentTasksPost}
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage.workers.HyperstorageWorkerSettings
import com.hypertino.hyperstorage.workers.primary.{PrimaryExtra, PrimaryWorker}
import com.hypertino.hyperstorage.workers.secondary.SecondaryWorker
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, FreeSpec, Matchers}

import scala.concurrent.duration._

class CollectionsSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))
  import MessagingContext.Implicits.emptyContext

  "Collection" should "Put item" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, scheduler))

    val collection = SeqGenerator.create() + "~"
    val item1 = SeqGenerator.create()

    val task = ContentPut(
      path = s"$collection/$item1",
      DynamicBody(Obj.from("text" → "Test item value", "null" → Null))
    )

    db.selectContent(collection, item1).runAsync.futureValue shouldBe None

    worker ! primaryTask(collection, task)
    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    br.body.documentUri should equal(collection)
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(task.correlationId)

    val content = db.selectContent(collection, item1).runAsync.futureValue
    content shouldNot equal(None)
    content.get.body should equal(Some(s"""{"text":"Test item value","id":"$item1"}"""))
    content.get.transactionList.size should equal(1)
    content.get.revision should equal(1)
    content.get.count should equal(Some(1))
    val uuid = content.get.transactionList.head

    val item2 = SeqGenerator.create()
    val task2 = ContentPut(
      path = s"$collection/$item2",
      DynamicBody(Obj.from("text" → "Test item value 2"))
    )
    worker ! primaryTask(collection, task2)
    val (bgTask2, br2) = tk.expectTaskR[BackgroundContentTasksPost]()
    br2.body.documentUri should equal(collection)
    val workerResult2 = expectMsgType[WorkerTaskResult]
    val r2 = workerResult2.result.get
    r2.headers.statusCode should equal(Status.CREATED)
    r2.headers.correlationId should equal(task2.correlationId)

    val content2 = db.selectContent(collection, item2).runAsync.futureValue
    content2 shouldNot equal(None)
    content2.get.body should equal(Some(s"""{"text":"Test item value 2","id":"$item2"}"""))
    content2.get.transactionList.size should equal(2)
    content2.get.revision should equal(2)
    content2.get.count should equal(Some(2))

    val transactions = selectTransactions(content2.get.transactionList, collection, db)
    transactions.size should equal(2)
    transactions.foreach {
      _.completedAt shouldBe None
    }
    transactions.head.revision should equal(2)
    transactions.tail.head.revision should equal(1)

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! bgTask.copy(expectsResult=true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal(collection)
    rc.body.transactions should equal(content2.get.transactionList.reverse.map(_.toString))

    eventually {
      db.selectContentStatic(collection).runAsync.futureValue.get.transactionList shouldBe empty
    }
    selectTransactions(content2.get.transactionList, collection, db).foreach {
      _.completedAt shouldNot be(None)
    }
  }

  it should "Patch item" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, scheduler))

    val collection = SeqGenerator.create() + "~"
    val path = s"$collection/${SeqGenerator.create()}"
    val ResourcePath(documentUri, itemId) = ContentLogic.splitPath(path)

    implicit val so = SerializationOptions.forceOptionalFields
    val put = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    )

    worker ! primaryTask(documentUri, put)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    val patch = ContentPatch(path,
      DynamicBody(Obj.from("text1" → "efg", "text2" → Null, "text3" → "zzz"))
    )
    worker ! primaryTask(documentUri, patch)

    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgPF() {
      case WorkerTaskResult(_, _, result, _) if result.get.headers.statusCode == Status.OK &&
        result.get.headers.correlationId == patch.correlationId ⇒ true
    }

    whenReady(db.selectContent(documentUri, itemId).runAsync) { result =>
      result.get.body should equal(Some(s"""{"text1":"efg","id":"$itemId","text3":"zzz"}"""))
      result.get.modifiedAt shouldNot be(None)
      result.get.documentUri should equal(documentUri)
      result.get.itemId should equal(itemId)
      result.get.count should equal(Some(1))
    }

    // delete element
    val delete = ContentDelete(path)
    worker ! primaryTask(documentUri, delete)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    // now patch should create again
    worker ! primaryTask(documentUri, patch)

    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgPF() {
      case WorkerTaskResult(_, _, result, _) if result.get.headers.statusCode == Status.CREATED &&
        result.get.headers.correlationId == patch.correlationId ⇒ true
    }

    whenReady(db.selectContent(documentUri, itemId).runAsync) { result =>
      result.get.body should equal(Some(s"""{"id":"$itemId","text1":"efg","text3":"zzz"}"""))
      result.get.modifiedAt shouldBe None
      result.get.documentUri should equal(documentUri)
      result.get.itemId should equal(itemId)
      result.get.count should equal(Some(1))
    }
  }

  it should "Delete item" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, scheduler))

    val collection = SeqGenerator.create() + "~"
    val item1 = SeqGenerator.create()
    val path = s"$collection/$item1"
    val ResourcePath(documentUri, itemId) = ContentLogic.splitPath(path)

    val put = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    )

    worker ! primaryTask(documentUri, put)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    val delete = ContentDelete(path)
    worker ! primaryTask(documentUri, delete)

    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgPF() {
      case WorkerTaskResult(_, _, r, _) if r.get.headers.statusCode == Status.OK &&
        r.get.headers.correlationId == delete.headers.correlationId ⇒ true
    }

    db.selectContent(documentUri, itemId).runAsync.futureValue shouldBe None
    db.selectContentStatic(collection).runAsync.futureValue.get.count shouldBe Some(0)
  }

  it should "Delete collection" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, scheduler))

    val path = UUID.randomUUID().toString + "~/el1"
    val ResourcePath(documentUri, itemId) = ContentLogic.splitPath(path)

    val put = ContentPut(path,
      DynamicBody(Obj.from("str" → "abc"))
    )

    worker ! primaryTask(documentUri, put)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    val delete = ContentDelete(documentUri)
    worker ! primaryTask(documentUri, delete)

    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgPF() {
      case WorkerTaskResult(_, _, r, _) if r.get.headers.statusCode == Status.OK &&
        r.get.headers.correlationId == delete.headers.correlationId ⇒ true
    }

    db.selectContent(documentUri, itemId).runAsync.futureValue.get.isDeleted shouldBe Some(true)
    db.selectContent(documentUri, "").runAsync.futureValue.get.isDeleted shouldBe Some(true)

    val itemId2 = "el2"
    val path2 = documentUri + "/" + itemId2

    val put2 = ContentPut(path2,
      DynamicBody(Obj.from("str" → "klmn"))
    )

    worker ! primaryTask(documentUri, put2)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    db.selectContent(documentUri, itemId2).runAsync.futureValue.get.isDeleted shouldBe None
    db.selectContent(documentUri, itemId).runAsync.futureValue shouldBe None
  }

  it should "put empty collection" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, scheduler))

    val collection = SeqGenerator.create() + "~"

    implicit val so = SerializationOptions.forceOptionalFields
    val put = ContentPut(collection,
      DynamicBody(Null)
    )

    worker ! primaryTask(collection, put)
    tk.expectTaskR[BackgroundContentTasksPost]()

    expectMsgPF() {
      case WorkerTaskResult(_, _, r, _) if r.get.headers.statusCode == Status.CREATED &&
        r.get.headers.correlationId == put.correlationId ⇒ true
    }

    whenReady(db.selectContent(collection, "").runAsync) { result =>
      result.get.body shouldBe None
      result.get.documentUri should equal(collection)
      result.get.count should equal(Some(0))
    }
  }

  "Collection" should "Put item with smart id" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, scheduler))

    val collection = SeqGenerator.create() + "/users~"
    val item1 = SeqGenerator.create()

    val put = ContentPut(
      path = s"$collection/$item1",
      DynamicBody(Obj.from("text" → "Test item value", "null" → Null))
    )

    db.selectContent(collection, item1).runAsync.futureValue shouldBe None

    worker ! primaryTask(collection, put)
    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    br.body.documentUri should equal(collection)
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(put.correlationId)

    val content = db.selectContent(collection, item1).runAsync.futureValue
    content shouldNot equal(None)
    content.get.body should equal(Some(s"""{"text":"Test item value","user_id":"$item1"}"""))
    content.get.transactionList.size should equal(1)
    content.get.revision should equal(1)
    content.get.count should equal(Some(1))
    val uuid = content.get.transactionList.head

    val item2 = SeqGenerator.create()
    val put2 = ContentPut(
      path = s"$collection/$item2",
      DynamicBody(Obj.from("text" → "Test item value 2"))
    )
    worker ! primaryTask(collection, put2)
    val (bgTask2, br2) = tk.expectTaskR[BackgroundContentTasksPost]()
    br2.body.documentUri should equal(collection)
    val workerResult2 = expectMsgType[WorkerTaskResult]
    val r2 = workerResult2.result.get
    r2.headers.statusCode should equal(Status.CREATED)
    r2.headers.correlationId should equal(put2.correlationId)

    val content2 = db.selectContent(collection, item2).runAsync.futureValue
    content2 shouldNot equal(None)
    content2.get.body should equal(Some(s"""{"text":"Test item value 2","user_id":"$item2"}"""))
    content2.get.transactionList.size should equal(2)
    content2.get.revision should equal(2)
    content2.get.count should equal(Some(2))

    val transactions = selectTransactions(content2.get.transactionList, collection, db)
    transactions.size should equal(2)
    transactions.foreach {
      _.completedAt shouldBe None
    }
    transactions.head.revision should equal(2)
    transactions.tail.head.revision should equal(1)

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! bgTask.copy(expectsResult = true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal(collection)
    rc.body.transactions should equal(content2.get.transactionList.reverse.map(_.toString))

    eventually {
      db.selectContentStatic(collection).runAsync.futureValue.get.transactionList shouldBe empty
    }
    selectTransactions(content2.get.transactionList, collection, db).foreach {
      _.completedAt shouldNot be(None)
    }
  }
}
