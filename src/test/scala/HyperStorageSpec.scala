/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{Actor, ActorRef, Props}
import akka.testkit.TestActorRef
import com.datastax.driver.core.utils.UUIDs
import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperbus.serialization.SerializationOptions
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.db.Content
import com.hypertino.hyperstorage.internal.api.{BackgroundContentTaskResult, BackgroundContentTasksPost}
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage.workers.primary.{PrimaryWorker, PrimaryWorkerRequest}
import com.hypertino.hyperstorage.workers.secondary._
import mock.FaultClientTransport
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration._

class HyperStorageSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  import ContentLogic._

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))
  implicit val emptyContext = MessagingContext.empty

  "HyperStorage" should "Put" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))

    val task = ContentPut(
      path = "test-resource-1",
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    )

    whenReady(db.selectContent("test-resource-1", "")) { result =>
      result shouldBe None
    }

    val taskStr = task.serializeToString
    worker ! primaryTask(task.path, task)
    val backgroundTask = expectMsgType[BackgroundContentTasksPost]
    backgroundTask.body.documentUri should equal(task.path)
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(task.correlationId)

    val uuid = whenReady(db.selectContent("test-resource-1", "")) { result =>
      result.get.body should equal(Some("""{"text":"Test resource value"}"""))
      result.get.transactionList.size should equal(1)

      selectTransactions(result.get.transactionList, result.get.uri, db) foreach { transaction ⇒
        transaction.completedAt shouldBe None
        transaction.revision should equal(result.get.revision)
      }
      result.get.transactionList.head
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! backgroundTask
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal("test-resource-1")
    rc.body.transactions should contain(uuid)
    selectTransactions(rc.body.transactions.map(UUID.fromString), "test-resource-1", db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
      transaction.revision should equal(1)
    }
    db.selectContent("test-resource-1", "").futureValue.get.transactionList shouldBe empty
  }

  it should "Patch resource that doesn't exists" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))

    val patch = ContentPatch(
      path = "not-existing",
      DynamicBody(Obj.from("text" → "Test resource value"))
    )

    worker ! primaryTask(patch.path, patch)
    val backgroundTask = expectMsgType[BackgroundContentTasksPost]
    backgroundTask.body.documentUri should equal(patch.path)
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(patch.correlationId)

    val uuid = whenReady(db.selectContent("not-existing", "")) { result =>
      result.get.body should equal(Some("""{"text":"Test resource value"}"""))
      result.get.transactionList.size should equal(1)

      selectTransactions(result.get.transactionList, result.get.uri, db) foreach { transaction ⇒
        transaction.completedAt shouldBe None
        transaction.revision should equal(result.get.revision)
      }
      result.get.transactionList.head
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! backgroundTask
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.asInstanceOf[BackgroundContentTaskResult]
    rc.documentUri should equal("not-existing")
    rc.transactions should contain(uuid)
    selectTransactions(rc.transactions.map(UUID.fromString), "not-existing", db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
      transaction.revision should equal(1)
    }
    db.selectContent("not-existing", "").futureValue.get.transactionList shouldBe empty
  }

  it should "Patch existing and deleted resource" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))

    val path = "test-resource-" + UUID.randomUUID().toString
    val put = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    )

    worker ! primaryTask(path, put)
    expectMsgType[BackgroundContentTasksPost]
    expectMsgType[WorkerTaskResult]

    implicit val so = SerializationOptions.forceOptionalFields
    val patch = ContentPatch(path,
      DynamicBody(Obj.from("text1" → "efg", "text2" → Null, "text3" → "zzz"))
    )

    worker ! primaryTask(path, patch)
    expectMsgType[BackgroundContentTasksPost]
    expectMsgPF() {
      case WorkerTaskResult(_, _, r, _) if r.get.headers.statusCode == Status.OK &&
        r.get.headers.correlationId == patch.correlationId ⇒ true
    }

    whenReady(db.selectContent(path, "")) { result =>
      result.get.body should equal(Some("""{"text1":"efg","text3":"zzz"}"""))
    }

    // delete resource
    val delete = ContentDelete(path)
    worker ! primaryTask(path, delete)
    expectMsgType[BackgroundContentTasksPost]
    expectMsgType[WorkerTaskResult]

    // now patch should create new resource
    worker ! primaryTask(path, patch)

    val backgroundTask = expectMsgType[BackgroundContentTasksPost]
    backgroundTask.body.documentUri should equal(patch.path)
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(patch.correlationId)

    val uuid = whenReady(db.selectContent(path, "")) { result =>
      result.get.body should equal(Some("""{"text3":"zzz","text1":"efg"}"""))
      result.get.transactionList.size should equal(1)

      selectTransactions(result.get.transactionList, result.get.uri, db) foreach { transaction ⇒
        transaction.completedAt shouldBe None
        transaction.revision should equal(result.get.revision)
      }
      result.get.transactionList.head
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! backgroundTask
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal(path)
    rc.body.transactions should contain(uuid)
    selectTransactions(rc.body.transactions.map(UUID.fromString), path, db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
      transaction.revision should equal(4)
    }
    db.selectContent(path, "").futureValue.get.transactionList shouldBe empty
  }

  it should "Delete resource that doesn't exists" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))

    val delete = ContentDelete(path = "not-existing", body = EmptyBody)

    worker ! primaryTask(delete.path, delete)
    expectMsgPF() {
      case WorkerTaskResult(_, _, Some(r), _) if r.headers.statusCode == Status.NOT_FOUND &&
        r.headers.correlationId == delete.correlationId ⇒ true
    }

    whenReady(db.selectContent("not-existing", "")) { result =>
      result shouldBe None
    }
  }

  it should "Delete resource that exists" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))

    val path = "test-resource-" + UUID.randomUUID().toString
    val put = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    )

    worker ! primaryTask(path, put)
    expectMsgType[BackgroundContentTasksPost]
    expectMsgType[WorkerTaskResult]

    whenReady(db.selectContent(path, "")) { result =>
      result shouldNot be(None)
      result.get.isDeleted shouldBe None
    }

    val delete = ContentDelete(path)

    worker ! primaryTask(path, delete)
    expectMsgType[BackgroundContentTasksPost]
    expectMsgPF() {
      case WorkerTaskResult(_, _, Some(r), _) if r.headers.statusCode == Status.OK &&
        r.headers.correlationId == delete.headers.correlationId ⇒ true
    }

    whenReady(db.selectContent(path, "")) { result =>
      result.get.isDeleted shouldBe Some(true)
    }
  }

  it should "Put resource over the previously deleted" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))

    val path = "test-resource-" + UUID.randomUUID().toString
    val put = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    )

    worker ! primaryTask(path, put)
    expectMsgType[BackgroundContentTasksPost]
    expectMsgType[WorkerTaskResult]

    whenReady(db.selectContent(path, "")) { result =>
      result shouldNot be(None)
      result.get.isDeleted shouldBe None
    }

    val delete = ContentDelete(path)

    worker ! primaryTask(path, delete)
    expectMsgType[BackgroundContentTasksPost]
    expectMsgPF() {
      case WorkerTaskResult(_, _, Some(r), _) if r.headers.statusCode == Status.OK &&
        r.headers.correlationId == delete.headers.correlationId ⇒ true
    }

    whenReady(db.selectContent(path, "")) { result =>
      result.get.isDeleted shouldBe Some(true)
    }

    worker ! primaryTask(path, put)
    expectMsgType[BackgroundContentTasksPost]
    expectMsgType[WorkerTaskResult]

    whenReady(db.selectContent(path, "")) { result =>
      result shouldNot be(None)
      result.get.isDeleted shouldBe Some(false)
    }
  }

  it should "Test multiple transactions" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val transactionList = mutable.ListBuffer[String]()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))
    val path = "abcde"
    val put = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    )
    worker ! primaryTask(path, put)
    expectMsgType[BackgroundContentTasksPost]
    val result1 = expectMsgType[WorkerTaskResult]
    transactionList += result1.result.get.asInstanceOf[Ok[HyperStorageTransactionCreated]].body.transactionId

    val patch = ContentPatch(path,
      DynamicBody(Obj.from("text" → "abc", "text2" → "klmn"))
    )
    worker ! primaryTask(path, patch)
    expectMsgType[BackgroundContentTasksPost]
    val result2 = expectMsgType[WorkerTaskResult]
    transactionList += result2.result.get.asInstanceOf[Ok[HyperStorageTransactionCreated]].body.transactionId

    val delete = ContentDelete(path)
    worker ! primaryTask(path, delete)
    val backgroundWorkerTask = expectMsgType[BackgroundContentTasksPost]
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get.asInstanceOf[Ok[HyperStorageTransactionCreated]]
    r.headers.statusCode should equal(Status.OK)
    transactionList += r.body.transactionId
    // todo: list of transactions!s

    val transactionsC = whenReady(db.selectContent(path, "")) { result =>
      result.get.isDeleted shouldBe Some(true)
      result.get.transactionList
    }

    val transactions = transactionList.map(UUID.fromString)

    transactions should equal(transactionsC.reverse)

    selectTransactions(transactions, path, db) foreach { transaction ⇒
      transaction.completedAt should be(None)
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! backgroundWorkerTask
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.asInstanceOf[BackgroundContentTaskResult]
    rc.documentUri should equal(path)
    rc.transactions should equal(transactions)

    selectTransactions(rc.transactions.map(UUID.fromString), path, db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
    }
  }

  it should "Test faulty publish" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))
    val path = "faulty"
    val put = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    )
    worker ! primaryTask(path, put)
    expectMsgType[BackgroundContentTasksPost]
    expectMsgType[WorkerTaskResult]

    val patch = ContentPatch(path,
      DynamicBody(Obj.from("text" → "abc", "text2" → "klmn"))
    )
    worker ! primaryTask(path, patch)
    val backgroundWorkerTask = expectMsgType[BackgroundContentTasksPost]
    expectMsgType[WorkerTaskResult]

    val transactionUuids = whenReady(db.selectContent(path, "")) { result =>
      result.get.transactionList
    }

    selectTransactions(transactionUuids, path, db) foreach {
      _.completedAt shouldBe None
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))

    FaultClientTransport.checkers += {
      case request: DynamicRequest ⇒
        if (request.headers.method == Method.FEED_PUT) {
          true
        } else {
          fail("Unexpected publish")
          false
        }
    }

    backgroundWorker ! backgroundWorkerTask
    expectMsgType[WorkerTaskResult].result.get shouldBe a[InternalServerError[_]]
    selectTransactions(transactionUuids, path, db) foreach {
      _.completedAt shouldBe None
    }

    FaultClientTransport.checkers.clear()
    FaultClientTransport.checkers += {
      case request: DynamicRequest ⇒
        if (request.headers.method == Method.FEED_PATCH) {
          true
        } else {
          false
        }
    }

    backgroundWorker ! backgroundWorkerTask
    expectMsgType[WorkerTaskResult].result.get shouldBe a[InternalServerError[_]]
    val mons = selectTransactions(transactionUuids, path, db)

    mons.head.completedAt shouldBe None
    mons.tail.head.completedAt shouldNot be(None)

    FaultClientTransport.checkers.clear()
    backgroundWorker ! backgroundWorkerTask
    expectMsgType[WorkerTaskResult].result shouldBe a[BackgroundContentTaskResult]
    selectTransactions(transactionUuids, path, db) foreach {
      _.completedAt shouldNot be(None)
    }

    db.selectContent(path, "").futureValue.get.transactionList shouldBe empty
  }

  it should "wait for full operation if requested" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 20.seconds))
    val fakeProcessor = TestActorRef[FakeProcessor](Props(classOf[FakeProcessor], worker))
    val distributor = new HyperbusAdapter(hyperbus, fakeProcessor, db, tracker, 20.seconds)

    Thread.sleep(2000)

    val path = "abcde"
    val createTask = hyperbus.ask(
      ContentPut(path, DynamicBody(Obj.from("a" → 10, "x" → "hello")), Headers(HyperStorageHeader.HYPER_STORAGE_WAIT → "full")))
      .runAsync

    val r1: Content = eventually {
      val r = db.selectContent(path, "").futureValue.head
      r.documentUri shouldBe path
      r
    }
    val tr1 = r1.transactionList.head

    val backgroundWorkerTask = eventually {
      import scala.collection.JavaConverters._
      val h = fakeProcessor.underlyingActor.otherMessages.asScala.head
      h shouldBe a[LocalTask] // from primary worker
      h.asInstanceOf[LocalTask].request shouldBe BackgroundContentTasksPost
      h.asInstanceOf[LocalTask]
    }

    Thread.sleep(2000)
    createTask.isCompleted shouldBe false

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! backgroundWorkerTask
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    backgroundWorkerResult.result shouldBe a[BackgroundContentTaskResult]

    selectTransactions(Seq(tr1), path, db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
    }

    createTask.futureValue shouldBe a[Created[_]]
  }
}

class FakeProcessor(primaryWorker: ActorRef) extends Actor {
  var adapter: ActorRef = null
  var otherMessages = new ConcurrentLinkedQueue[Any]()

  def receive: Receive = {
    case r: LocalTask if r.request.isInstanceOf[PrimaryWorkerRequest] ⇒
      adapter = sender()
      //println("got from" + sender() + s", self: $self: " + r)
      primaryWorker ! r

    case WorkerTaskResult(_, _, r, _) if r.isDefined && r.get.body.isInstanceOf[HyperStorageTransactionCreated] ⇒
      //println("got from" + sender() + s", self: $self, forwarding to $adapter: " + r)
      adapter forward r

    case r ⇒
      //println("got unknown from" + sender() + s", self: $self: " + r)
      otherMessages.add(r)
  }
}
