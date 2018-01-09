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

class HyperStorageSpecZMQ extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  import ContentLogic._

  override def defaultClusterTransportIsZMQ: Boolean = true
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))
  implicit val emptyContext = MessagingContext.empty

  "HyperStorage" should "Put" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))

    val task = ContentPut(
      path = "test-resource-1",
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    )

    whenReady(db.selectContent("test-resource-1", "").runAsync) { result =>
      result shouldBe None
    }

    val taskStr = task.serializeToString
    worker ! primaryTask(task.path, task)
    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    br.body.documentUri should equal(task.path)
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(task.correlationId)

    val uuid = whenReady(db.selectContent("test-resource-1", "").runAsync) { result =>
      result.get.body should equal(Some("""{"text":"Test resource value"}"""))
      result.get.transactionList.size should equal(1)

      selectTransactions(result.get.transactionList, result.get.uri, db) foreach { transaction ⇒
        transaction.completedAt shouldBe None
        transaction.revision should equal(result.get.revision)
      }
      result.get.transactionList.head.toString
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! bgTask.copy(expectsResult=true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal("test-resource-1")
    rc.body.transactions should contain(uuid)
    selectTransactions(rc.body.transactions.map(UUID.fromString), "test-resource-1", db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
      transaction.revision should equal(1)
    }
    db.selectContent("test-resource-1", "").runAsync.futureValue.get.transactionList shouldBe empty
  }

  it should "Patch resource that doesn't exists" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))

    val patch = ContentPatch(
      path = "not-existing",
      DynamicBody(Obj.from("text" → "Test resource value"))
    )

    worker ! primaryTask(patch.path, patch)
    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    br.body.documentUri should equal(patch.path)
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(patch.correlationId)

    val uuid = whenReady(db.selectContent("not-existing", "").runAsync) { result =>
      result.get.body should equal(Some("""{"text":"Test resource value"}"""))
      result.get.transactionList.size should equal(1)

      selectTransactions(result.get.transactionList, result.get.uri, db) foreach { transaction ⇒
        transaction.completedAt shouldBe None
        transaction.revision should equal(result.get.revision)
      }
      result.get.transactionList.head.toString
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! bgTask.copy(expectsResult=true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal("not-existing")
    rc.body.transactions should contain(uuid)
    selectTransactions(rc.body.transactions.map(UUID.fromString), "not-existing", db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
      transaction.revision should equal(1)
    }
    db.selectContent("not-existing", "").runAsync.futureValue.get.transactionList shouldBe empty
  }

  it should "Patch existing and deleted resource" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))

    val path = "test-resource-" + UUID.randomUUID().toString
    val put = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    )

    worker ! primaryTask(path, put)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    implicit val so = SerializationOptions.forceOptionalFields
    val patch = ContentPatch(path,
      DynamicBody(Obj.from("text1" → "efg", "text2" → Null, "text3" → "zzz"))
    )

    worker ! primaryTask(path, patch)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgPF() {
      case WorkerTaskResult(_, _, r, _) if r.get.headers.statusCode == Status.OK &&
        r.get.headers.correlationId == patch.correlationId ⇒ true
    }

    whenReady(db.selectContent(path, "").runAsync) { result =>
      result.get.body should equal(Some("""{"text1":"efg","text3":"zzz"}"""))
    }

    // delete resource
    val delete = ContentDelete(path)
    worker ! primaryTask(path, delete)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    // now patch should create new resource
    worker ! primaryTask(path, patch)

    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    br.body.documentUri should equal(patch.path)
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(patch.correlationId)

    val uuid = whenReady(db.selectContent(path, "").runAsync) { result =>
      result.get.body should equal(Some("""{"text1":"efg","text3":"zzz"}"""))
      result.get.transactionList.size should equal(1)

      selectTransactions(result.get.transactionList, result.get.uri, db) foreach { transaction ⇒
        transaction.completedAt shouldBe None
        transaction.revision should equal(result.get.revision)
      }
      result.get.transactionList.head.toString
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! bgTask.copy(expectsResult = true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal(path)
    rc.body.transactions should contain(uuid)
    selectTransactions(rc.body.transactions.map(UUID.fromString), path, db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
      transaction.revision should equal(4)
    }
    db.selectContent(path, "").runAsync.futureValue.get.transactionList shouldBe empty
  }

  it should "Delete resource that doesn't exists" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))

    val delete = ContentDelete(path = "not-existing", body = EmptyBody)

    worker ! primaryTask(delete.path, delete)
    expectMsgPF() {
      case WorkerTaskResult(_, _, Some(r), _) if r.headers.statusCode == Status.NOT_FOUND &&
        r.headers.correlationId == delete.correlationId ⇒ true
    }

    whenReady(db.selectContent("not-existing", "").runAsync) { result =>
      result shouldBe None
    }
  }

  it should "Delete resource that exists" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))

    val path = "test-resource-" + UUID.randomUUID().toString
    val put = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    )

    worker ! primaryTask(path, put)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    whenReady(db.selectContent(path, "").runAsync) { result =>
      result shouldNot be(None)
      result.get.isDeleted shouldBe None
    }

    val delete = ContentDelete(path)

    worker ! primaryTask(path, delete)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgPF() {
      case WorkerTaskResult(_, _, Some(r), _) if r.headers.statusCode == Status.OK &&
        r.headers.correlationId == delete.headers.correlationId ⇒ true
    }

    whenReady(db.selectContent(path, "").runAsync) { result =>
      result.get.isDeleted shouldBe Some(true)
    }
  }

  it should "Put resource over the previously deleted" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))

    val path = "test-resource-" + UUID.randomUUID().toString
    val put = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    )

    worker ! primaryTask(path, put)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    whenReady(db.selectContent(path, "").runAsync) { result =>
      result shouldNot be(None)
      result.get.isDeleted shouldBe None
    }

    val delete = ContentDelete(path)

    worker ! primaryTask(path, delete)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgPF() {
      case WorkerTaskResult(_, _, Some(r), _) if r.headers.statusCode == Status.OK &&
        r.headers.correlationId == delete.headers.correlationId ⇒ true
    }

    whenReady(db.selectContent(path, "").runAsync) { result =>
      result.get.isDeleted shouldBe Some(true)
    }

    worker ! primaryTask(path, put)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    whenReady(db.selectContent(path, "").runAsync) { result =>
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

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))
    val path = "abcde"
    val put = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    )
    worker ! primaryTask(path, put)
    tk.expectTaskR[BackgroundContentTasksPost]()
    val result1 = expectMsgType[WorkerTaskResult]
    transactionList += result1.result.get.asInstanceOf[Created[HyperStorageTransactionCreated]].body.transactionId

    val patch = ContentPatch(path,
      DynamicBody(Obj.from("text" → "abc", "text2" → "klmn"))
    )
    worker ! primaryTask(path, patch)
    tk.expectTaskR[BackgroundContentTasksPost]()
    val result2 = expectMsgType[WorkerTaskResult]
    transactionList += result2.result.get.asInstanceOf[Ok[HyperStorageTransaction]].body.transactionId

    val delete = ContentDelete(path)
    worker ! primaryTask(path, delete)
    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get.asInstanceOf[Ok[HyperStorageTransaction]]
    r.headers.statusCode should equal(Status.OK)
    transactionList += r.body.transactionId
    // todo: list of transactions!s

    val transactionsC = whenReady(db.selectContent(path, "").runAsync) { result =>
      result.get.isDeleted shouldBe Some(true)
      result.get.transactionList
    }

    val transactionsUUIDs = transactionList.map(UUID.fromString)

    transactionsUUIDs should equal(transactionsC.reverse)

    selectTransactions(transactionsUUIDs, path, db) foreach { transaction ⇒
      transaction.completedAt should be(None)
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! bgTask.copy(expectsResult = true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal(path)
    rc.body.transactions should equal(transactionList)

    selectTransactions(rc.body.transactions.map(UUID.fromString), path, db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
    }
  }
  it should "Test faulty publish" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))
    val path = "faulty"
    val put = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    )
    worker ! primaryTask(path, put)
    tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    val patch = ContentPatch(path,
      DynamicBody(Obj.from("text" → "abc", "text2" → "klmn"))
    )
    worker ! primaryTask(path, patch)
    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    expectMsgType[WorkerTaskResult]

    val transactionUuids = whenReady(db.selectContent(path, "").runAsync) { result =>
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

    backgroundWorker ! bgTask.copy(expectsResult = true)
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

    backgroundWorker ! bgTask.copy(expectsResult = true)
    expectMsgType[WorkerTaskResult].result.get shouldBe a[InternalServerError[_]]
    val mons = selectTransactions(transactionUuids, path, db)

    mons.head.completedAt shouldBe None
    mons.tail.head.completedAt shouldNot be(None)

    FaultClientTransport.checkers.clear()
    backgroundWorker ! bgTask.copy(expectsResult = true)
    expectMsgType[WorkerTaskResult].result.get.body shouldBe a[BackgroundContentTaskResult]
    selectTransactions(transactionUuids, path, db) foreach {
      _.completedAt shouldNot be(None)
    }

    db.selectContent(path, "").runAsync.futureValue.get.transactionList shouldBe empty
  }

  it should "wait for full operation if requested" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 20.seconds, 10, 16777216, scheduler))
    val fakeProcessor = TestActorRef[FakeProcessor](Props(classOf[FakeProcessor], worker))
    val distributor = new HyperbusAdapter(hyperbus, fakeProcessor, db, tracker, 20.seconds)

    Thread.sleep(2000)

    val path = "abcde"
    val createTask = hyperbus.ask(
      ContentPut(path, DynamicBody(Obj.from("a" → 10, "x" → "hello")), Headers(HyperStorageHeader.HYPER_STORAGE_WAIT → "full")))
      .runAsync

    val r1: Content = eventually {
      val r = db.selectContent(path, "").runAsync.futureValue.head
      r.documentUri shouldBe path
      r
    }
    val tr1 = r1.transactionList.head

    val backgroundWorkerTask = eventually {
      import scala.collection.JavaConverters._
      val h = fakeProcessor.underlyingActor.otherMessages.asScala.head
      h shouldBe a[LocalTask] // from primary worker
      h.asInstanceOf[LocalTask].request shouldBe a[BackgroundContentTasksPost]
      h.asInstanceOf[LocalTask]
    }

    Thread.sleep(2000)
    createTask.isCompleted shouldBe false

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! backgroundWorkerTask.copy(expectsResult = true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    backgroundWorkerResult.result.get.body shouldBe a[BackgroundContentTaskResult]

    selectTransactions(Seq(tr1), path, db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
    }

    createTask.futureValue shouldBe a[Created[_]]
  }

  it should "return TOO_MANY_REQUESTS if incomplete transaction limit is reached" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 2, 16777216, scheduler))

    val task = ContentPut(
      path = "test-resource-1",
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    )

    db.selectContent("test-resource-1", "").runAsync.futureValue shouldBe None

    val taskStr = task.serializeToString
    worker ! primaryTask(task.path, task)
    val (_, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    br.body.documentUri should equal(task.path)
    val workerResult = expectMsgType[WorkerTaskResult]
    val r = workerResult.result.get
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(task.correlationId)

    worker ! primaryTask(task.path, task)
    val (_, br2) = tk.expectTaskR[BackgroundContentTasksPost]()
    br2.body.documentUri should equal(task.path)
    val workerResult2 = expectMsgType[WorkerTaskResult]
    val r2 = workerResult2.result.get
    r2.headers.statusCode should equal(Status.OK)

    worker ! primaryTask(task.path, task)
    val workerResult3 = expectMsgType[WorkerTaskResult]
    val r3 = workerResult3.result.get
    r3.headers.statusCode should equal(Status.TOO_MANY_REQUESTS)
  }

  "PrimaryWorker" should "execute batched tasks" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val transactionList = mutable.ListBuffer[String]()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))
    val path = "abcde"
    val put = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "text2" → "yey"))
    )
    val patch = ContentPatch(path,
      DynamicBody(Obj.from("text2" → "abc", "text3" → "klmn"))
    )
    worker ! BatchTask(Seq(
      LocalTaskWithId(1, primaryTask(path, put)),
      LocalTaskWithId(2, primaryTask(path, patch))
    ))

    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    val result1 = expectMsgType[WorkerBatchTaskResult]
    result1.results.size shouldBe 2
    val m = result1.results.map(i => (i._1, i._2.get)).toMap
    transactionList += m(1).asInstanceOf[Response[HyperStorageTransactionCreated]].body.transactionId
    transactionList += m(2).asInstanceOf[Response[HyperStorageTransaction]].body.transactionId

    val transactionsC = whenReady(db.selectContent(path, "").runAsync) { result =>
      result.get.body should equal(Some("""{"text":"Test resource value","text2":"abc","text3":"klmn"}"""))
      result.get.isDeleted shouldBe None
      result.get.transactionList
    }

    val transactionsUUIDs = transactionList.map(UUID.fromString)

    transactionsUUIDs should equal(transactionsC.reverse)

    selectTransactions(transactionsUUIDs, path, db) foreach { transaction ⇒
      transaction.completedAt should be(None)
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! bgTask.copy(expectsResult = true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal(path)
    rc.body.transactions should equal(transactionList)

    selectTransactions(rc.body.transactions.map(UUID.fromString), path, db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
    }
  }

  it should "execute all batched tasks even if some fails" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val transactionList = mutable.ListBuffer[String]()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, 10, 16777216, scheduler))
    val path = "abcde"
    val put = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "text2" → "yey"))
    )
    val wrongPatch = ContentPatch(path,
      DynamicBody(Text("abc"))
    )
    val patch = ContentPatch(path,
      DynamicBody(Obj.from("text2" → "abc", "text3" → "klmn"))
    )
    worker ! BatchTask(Seq(
      LocalTaskWithId(1, primaryTask(path, put)),
      LocalTaskWithId(2, primaryTask(path, wrongPatch)),
      LocalTaskWithId(3, primaryTask(path, patch))
    ))

    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    val result1 = expectMsgType[WorkerBatchTaskResult]
    result1.results.size shouldBe 3
    val m = result1.results.map(i => (i._1, i._2.get)).toMap
    transactionList += m(1).asInstanceOf[Response[HyperStorageTransactionCreated]].body.transactionId
    m(2) shouldBe a[InternalServerError[_]]
    transactionList += m(3).asInstanceOf[Response[HyperStorageTransaction]].body.transactionId

    val transactionsC = whenReady(db.selectContent(path, "").runAsync) { result =>
      result.get.body should equal(Some("""{"text":"Test resource value","text2":"abc","text3":"klmn"}"""))
      result.get.isDeleted shouldBe None
      result.get.transactionList
    }

    val transactionsUUIDs = transactionList.map(UUID.fromString)

    transactionsUUIDs should equal(transactionsC.reverse)

    selectTransactions(transactionsUUIDs, path, db) foreach { transaction ⇒
      transaction.completedAt should be(None)
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! bgTask.copy(expectsResult = true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal(path)
    rc.body.transactions should equal(transactionList)

    selectTransactions(rc.body.transactions.map(UUID.fromString), path, db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
    }
  }

  it should "execute batched tasks on collection/splitting by size" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val transactionList = mutable.ListBuffer[String]()

    val inserts = 1
    val deletes = 2
    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds, maxIncompleteTransaction=inserts+deletes+1,
      maxBatchSizeInBytes=1024, scheduler))
    val path = "abcde~"
    worker ! BatchTask(
      (0 until inserts).map { i =>
        LocalTaskWithId(i, primaryTask(path,
          ContentPut(path+ "/" + i, DynamicBody(Obj.from("text" → "yey", "n" -> i)))
        ))
      } ++ (inserts until (inserts + deletes)).map { i =>
        LocalTaskWithId(i, primaryTask(path,
          ContentDelete(path + "/" + i)
        ))
      }
    )

    val (bgTask, br) = tk.expectTaskR[BackgroundContentTasksPost]()
    val result1 = expectMsgType[WorkerBatchTaskResult]
    result1.results.size shouldBe inserts + deletes
    transactionList ++= result1.results.sortBy(_._1).take(inserts).map(_._2.get.asInstanceOf[Response[HyperStorageTransactionCreated]].body.transactionId)
    result1.results.sortBy(_._1).drop(inserts).map(_._2.get shouldBe a[Ok[_]])

    val transactionsC = whenReady(db.selectContentStatic(path).runAsync) { result =>
      result.get.count shouldBe Some(inserts)
      result.get.isDeleted shouldBe None
      result.get.transactionList
    }

    val transactionsUUIDs = transactionList.map(UUID.fromString)
    transactionsUUIDs should equal(transactionsC.reverse)

    selectTransactions(transactionsUUIDs, path, db) foreach { transaction ⇒
      transaction.completedAt should be(None)
    }

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! bgTask.copy(expectsResult = true)
    val backgroundWorkerResult = expectMsgType[WorkerTaskResult]
    val rc = backgroundWorkerResult.result.get.asInstanceOf[Ok[BackgroundContentTaskResult]]
    rc.body.documentUri should equal(path)
    rc.body.transactions should equal(transactionList)

    selectTransactions(rc.body.transactions.map(UUID.fromString), path, db) foreach { transaction ⇒
      transaction.completedAt shouldNot be(None)
    }
  }
}

class FakeProcessor(primaryWorker: ActorRef) extends Actor {
  var client: ActorRef = null
  var otherMessages = new ConcurrentLinkedQueue[Any]()

  def receive: Receive = {
    case r: LocalTask if r.request.isInstanceOf[PrimaryWorkerRequest] ⇒
      client = sender()
      println("got from" + sender() + s", self: $self: " + r)
      primaryWorker ! r

    case WorkerTaskResult(_, _, r, _) if r.isDefined && r.get.body.isInstanceOf[HyperStorageTransactionCreated] ⇒
      println("got from" + sender() + s", self: $self, forwarding to $client: " + r.get)
      client forward r.get

    case r ⇒
      println("got unknown from" + sender() + s", self: $self: " + r)
      otherMessages.add(r)
  }
}


class HyperStorageSpecAkkaCluster extends HyperStorageSpecZMQ {
  override def defaultClusterTransportIsZMQ: Boolean = false
}

