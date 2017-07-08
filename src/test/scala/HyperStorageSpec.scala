import java.util.UUID

import akka.testkit.TestActorRef
import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage.workers.primary.{PrimaryTask, PrimaryWorker, PrimaryWorkerTaskResult}
import com.hypertino.hyperstorage.workers.secondary._
import mock.FaultClientTransport
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
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
    worker ! PrimaryTask(task.path, System.currentTimeMillis() + 10000, taskStr)
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
    worker ! PrimaryTask(task.path, System.currentTimeMillis() + 10000, taskStr)
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

  it should "Patch existing and deleted resource" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))

    val path = "test-resource-" + UUID.randomUUID().toString
    val taskPutStr = ContentPut(path,
      DynamicBody(Obj.from("text1" → "abc", "text2" → "klmn"))
    ).serializeToString

    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskPutStr)
    expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    val task = ContentPatch(path,
      DynamicBody(Obj.from("text1" → "efg", "text2" → Null, "text3" → "zzz"))
    )
    val taskPatchStr = task.serializeToString

    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskPatchStr)
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
    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, deleteTaskStr)
    expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    // now patch should return 404
    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskPatchStr)

    expectMsgPF() {
      case ShardTaskComplete(_, result: PrimaryWorkerTaskResult) if response(result.content).headers.statusCode == Status.NOT_FOUND &&
        response(result.content).headers.correlationId == task.correlationId ⇒ {
        true
      }
    }
  }

  it should "Delete resource that doesn't exists" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))

    val task = ContentDelete(path = "not-existing", body = EmptyBody)

    val taskStr = task.serializeToString
    worker ! PrimaryTask(task.path, System.currentTimeMillis() + 10000, taskStr)
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

    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskPutStr)
    expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    whenReady(db.selectContent(path, "")) { result =>
      result shouldNot be(None)
    }

    val task = ContentDelete(path)

    val taskStr = task.serializeToString
    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskStr)
    expectMsgType[BackgroundContentTask]
    expectMsgPF() {
      case ShardTaskComplete(_, result: PrimaryWorkerTaskResult) if response(result.content).headers.statusCode == Status.OK &&
        response(result.content).headers.correlationId == task.headers.correlationId ⇒ {
        true
      }
    }

    whenReady(db.selectContent(path, "")) { result =>
      result.get.isDeleted shouldBe true
    }
  }

  it should "Test multiple transactions" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val transactionList = mutable.ListBuffer[Value]()

    val worker = TestActorRef(PrimaryWorker.props(hyperbus, db, tracker, 10.seconds))
    val path = "abcde"
    val taskStr1 = ContentPut(path,
      DynamicBody(Obj.from("text" → "Test resource value", "null" → Null))
    ).serializeToString
    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskStr1)
    expectMsgType[BackgroundContentTask]
    val result1 = expectMsgType[ShardTaskComplete]
    transactionList += response(result1.result.asInstanceOf[PrimaryWorkerTaskResult].content).body.content.transactionId

    val taskStr2 = ContentPatch(path,
      DynamicBody(Obj.from("text" → "abc", "text2" → "klmn"))
    ).serializeToString
    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskStr2)
    expectMsgType[BackgroundContentTask]
    val result2 = expectMsgType[ShardTaskComplete]
    transactionList += response(result2.result.asInstanceOf[PrimaryWorkerTaskResult].content).body.content.transactionId

    val taskStr3 = ContentDelete(path).serializeToString
    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskStr3)
    val backgroundWorkerTask = expectMsgType[BackgroundContentTask]
    val workerResult = expectMsgType[ShardTaskComplete]
    val r = response(workerResult.result.asInstanceOf[PrimaryWorkerTaskResult].content)
    r.headers.statusCode should equal(Status.OK)
    transactionList += r.body.content.transactionId
    // todo: list of transactions!s

    val transactionsC = whenReady(db.selectContent(path, "")) { result =>
      result.get.isDeleted should equal(true)
      result.get.transactionList
    }

    val transactions = transactionList.map(s ⇒ UUID.fromString(s.toString.split(':')(1)))

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
    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskStr1)
    expectMsgType[BackgroundContentTask]
    expectMsgType[ShardTaskComplete]

    val taskStr2 = ContentPatch(path,
      DynamicBody(Obj.from("text" → "abc", "text2" → "klmn"))
    ).serializeToString
    worker ! PrimaryTask(path, System.currentTimeMillis() + 10000, taskStr2)
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
}
