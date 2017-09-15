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
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage.workers.primary.{PrimaryContentTask, PrimaryWorker, PrimaryWorkerTaskResult}
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
    worker ! PrimaryContentTask(task.path, System.currentTimeMillis() + 10000, taskStr, expectsResult=true,isClientOperation=true)
    val backgroundTask = expectMsgType[BackgroundContentTask]
    backgroundTask.documentUri should equal(task.path)
    val workerResult = expectMsgType[ShardTaskComplete]
    val r = response(workerResult.result.asInstanceOf[PrimaryWorkerTaskResult].content)
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
    val backgroundWorkerResult = expectMsgType[ShardTaskComplete]
    val rc = backgroundWorkerResult.result.asInstanceOf[BackgroundContentTaskResult]
    rc.documentUri should equal("test-resource-1")
    rc.transactions should contain(uuid)
    selectTransactions(rc.transactions, "test-resource-1", db) foreach { transaction ⇒
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

    val task = ContentPatch(
      path = "not-existing",
      DynamicBody(Obj.from("text" → "Test resource value"))
    )

    val taskStr = task.serializeToString
    worker ! PrimaryContentTask(task.path, System.currentTimeMillis() + 10000, taskStr, expectsResult=true,isClientOperation=true)
    val backgroundTask = expectMsgType[BackgroundContentTask]
    backgroundTask.documentUri should equal(task.path)
    val workerResult = expectMsgType[ShardTaskComplete]
    val r = response(workerResult.result.asInstanceOf[PrimaryWorkerTaskResult].content)
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(task.correlationId)

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
    val backgroundWorkerResult = expectMsgType[ShardTaskComplete]
    val rc = backgroundWorkerResult.result.asInstanceOf[BackgroundContentTaskResult]
    rc.documentUri should equal("not-existing")
    rc.transactions should contain(uuid)
    selectTransactions(rc.transactions, "not-existing", db) foreach { transaction ⇒
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
    val taskPutStr = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    ).serializeToString

    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskPutStr, expectsResult=true,isClientOperation=true)
    expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    implicit val so = SerializationOptions.forceOptionalFields
    val task = ContentPatch(path,
      DynamicBody(Obj.from("text1" → "efg", "text2" → Null, "text3" → "zzz"))
    )
    val taskPatchStr = task.serializeToString

    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskPatchStr, expectsResult=true,isClientOperation=true)
    expectMsgType[BackgroundContentTask]
    expectMsgPF() {
      case ShardTaskComplete(_, result: PrimaryWorkerTaskResult) if response(result.content).headers.statusCode == Status.OK &&
        response(result.content).headers.correlationId == task.correlationId ⇒ {
        true
      }
    }

    whenReady(db.selectContent(path, "")) { result =>
      result.get.body should equal(Some("""{"text1":"efg","text3":"zzz"}"""))
    }

    // delete resource
    val deleteTask = ContentDelete(path)
    val deleteTaskStr = deleteTask.serializeToString
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, deleteTaskStr, expectsResult=true,isClientOperation=true)
    expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    // now patch should create new resource
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskPatchStr, expectsResult=true,isClientOperation=true)

    val backgroundTask = expectMsgType[BackgroundContentTask]
    backgroundTask.documentUri should equal(task.path)
    val workerResult = expectMsgType[ShardTaskComplete]
    val r = response(workerResult.result.asInstanceOf[PrimaryWorkerTaskResult].content)
    r.headers.statusCode should equal(Status.CREATED)
    r.headers.correlationId should equal(task.correlationId)

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
    val backgroundWorkerResult = expectMsgType[ShardTaskComplete]
    val rc = backgroundWorkerResult.result.asInstanceOf[BackgroundContentTaskResult]
    rc.documentUri should equal(path)
    rc.transactions should contain(uuid)
    selectTransactions(rc.transactions, path, db) foreach { transaction ⇒
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

    val task = ContentDelete(path = "not-existing", body = EmptyBody)

    val taskStr = task.serializeToString
    worker ! PrimaryContentTask(task.path, System.currentTimeMillis() + 10000, taskStr, expectsResult=true,isClientOperation=true)
    expectMsgPF() {
      case ShardTaskComplete(_, result: PrimaryWorkerTaskResult) if response(result.content).headers.statusCode == Status.NOT_FOUND &&
        response(result.content).headers.correlationId == task.correlationId ⇒ {
        true
      }
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
    val taskPutStr = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    ).serializeToString

    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskPutStr, expectsResult=true,isClientOperation=true)
    expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    whenReady(db.selectContent(path, "")) { result =>
      result shouldNot be(None)
      result.get.isDeleted shouldBe None
    }

    val task = ContentDelete(path)

    val taskStr = task.serializeToString
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskStr, expectsResult=true,isClientOperation=true)
    expectMsgType[BackgroundContentTask]
    expectMsgPF() {
      case ShardTaskComplete(_, result: PrimaryWorkerTaskResult) if response(result.content).headers.statusCode == Status.OK &&
        response(result.content).headers.correlationId == task.headers.correlationId ⇒ {
        true
      }
    }

    whenReady(db.selectContent(path, "")) { result =>
      result.get.isDeleted shouldBe Some(true)
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
    val taskStr1 = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    ).serializeToString
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskStr1, expectsResult=true,isClientOperation=true)
    expectMsgType[BackgroundContentTask]
    val result1 = expectMsgType[ShardTaskComplete]
    transactionList += response(result1.result.asInstanceOf[PrimaryWorkerTaskResult].content).body.content.transaction_id.toString

    val taskStr2 = ContentPatch(path,
      DynamicBody(Obj.from("text" → "abc", "text2" → "klmn"))
    ).serializeToString
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskStr2, expectsResult=true,isClientOperation=true)
    expectMsgType[BackgroundContentTask]
    val result2 = expectMsgType[ShardTaskComplete]
    transactionList += response(result2.result.asInstanceOf[PrimaryWorkerTaskResult].content).body.content.transaction_id.toString

    val taskStr3 = ContentDelete(path).serializeToString
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskStr3, expectsResult=true,isClientOperation=true)
    val backgroundWorkerTask = expectMsgType[BackgroundContentTask]
    val workerResult = expectMsgType[ShardTaskComplete]
    val r = response(workerResult.result.asInstanceOf[PrimaryWorkerTaskResult].content)
    r.headers.statusCode should equal(Status.OK)
    transactionList += r.body.content.transaction_id.toString
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
    val backgroundWorkerResult = expectMsgType[ShardTaskComplete]
    val rc = backgroundWorkerResult.result.asInstanceOf[BackgroundContentTaskResult]
    rc.documentUri should equal(path)
    rc.transactions should equal(transactions)

    selectTransactions(rc.transactions, path, db) foreach { transaction ⇒
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
    val taskStr1 = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    ).serializeToString
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskStr1, expectsResult=true,isClientOperation=true)
    expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    val taskStr2 = ContentPatch(path,
      DynamicBody(Obj.from("text" → "abc", "text2" → "klmn"))
    ).serializeToString
    worker ! PrimaryContentTask(path, System.currentTimeMillis() + 10000, taskStr2, expectsResult=true,isClientOperation=true)
    val backgroundWorkerTask = expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

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
    expectMsgType[ShardTaskComplete].result shouldBe a[BackgroundContentTaskFailedException]
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
    expectMsgType[ShardTaskComplete].result shouldBe a[BackgroundContentTaskFailedException]
    val mons = selectTransactions(transactionUuids, path, db)

    mons.head.completedAt shouldBe None
    mons.tail.head.completedAt shouldNot be(None)

    FaultClientTransport.checkers.clear()
    backgroundWorker ! backgroundWorkerTask
    expectMsgType[ShardTaskComplete].result shouldBe a[BackgroundContentTaskResult]
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
      ContentPut(path, DynamicBody(Obj.from("a" → 10, "x" → "hello")), HeadersMap(HyperStorageHeader.HYPER_STORAGE_WAIT → "full")))
      .runAsync

    val r1: Content = eventually {
      val r = db.selectContent(path, "").futureValue.head
      r.documentUri shouldBe path
      r
    }
    val tr1 = r1.transactionList.head

    val backgroundWorkerTask = eventually {
      import scala.collection.JavaConverters._
      fakeProcessor.underlyingActor.otherMessages.asScala.head shouldBe a[BackgroundContentTask] // from primary worker
      fakeProcessor.underlyingActor.otherMessages.asScala.head.asInstanceOf[BackgroundContentTask]
    }

    Thread.sleep(2000)
    createTask.isCompleted shouldBe false

    val backgroundWorker = TestActorRef(SecondaryWorker.props(hyperbus, db, tracker, self, scheduler))
    backgroundWorker ! backgroundWorkerTask
    val backgroundWorkerResult = expectMsgType[ShardTaskComplete]
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
    case r: PrimaryContentTask ⇒
      adapter = sender()
      //println("got from" + sender() + s", self: $self: " + r)
      primaryWorker ! r

    case ShardTaskComplete(_, r) if r.isInstanceOf[PrimaryWorkerTaskResult] ⇒
      //println("got from" + sender() + s", self: $self, forwarding to $adapter: " + r)
      adapter forward r

    case r ⇒
      //println("got unknown from" + sender() + s", self: $self: " + r)
      otherMessages.add(r)
  }
}
