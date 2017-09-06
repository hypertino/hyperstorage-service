import akka.testkit.TestActorRef
import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperstorage._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.sharding._
import com.hypertino.hyperstorage.workers.primary.PrimaryWorker
import com.hypertino.hyperstorage.workers.secondary.SecondaryWorker
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class IfMatchTest extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(20000, Millis)))
  implicit val emptyContext = MessagingContext.empty

  "if-match" should "work" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val workerProps = PrimaryWorker.props(hyperbus, db, tracker, 10.seconds)
    val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, tracker, self, scheduler)
    val workerSettings = Map(
      "hyperstorage-primary-worker" → (workerProps, 1, "pgw-"),
      "hyperstorage-secondary-worker" → (secondaryWorkerProps, 1, "sgw-")
    )

    val processor = TestActorRef(ShardProcessor.props(workerSettings, "hyperstorage", tracker))
    val distributor = new HyperbusAdapter(hyperbus, processor, db, tracker, 20.seconds)
    // wait while subscription is completes
    Thread.sleep(2000)

    val create = hyperbus.ask(ContentPut("abc", DynamicBody(Obj.from("a" → 10, "x" → "hello"))))
      .runAsync
      .futureValue

    create shouldBe a[Created[_]]

    val ok = hyperbus.ask(ContentGet("abc"))
      .runAsync
      .futureValue

    ok shouldBe a[Ok[_]]
    ok.body shouldBe DynamicBody(Obj.from("a" → 10, "x" → "hello"))

    val rev = ok.headers(Header.REVISION)
    val etag = "\"" + rev + "\""
    val wrongEtag = "\"" + rev+1 + "\""

    val deleteFail = hyperbus.ask(ContentDelete("abc", $headersMap=HeadersMap("if-match" → wrongEtag)))
      .runAsync
      .failed
      .futureValue

    deleteFail shouldBe a[PreconditionFailed[_]]

    val delete = hyperbus.ask(ContentDelete("abc", $headersMap=HeadersMap("if-match" → etag)))
      .runAsync
      .futureValue

    delete shouldBe a[Ok[_]]
  }

  it should "work on empty etag" in {
    val hyperbus = testHyperbus()
    val tk = testKit()
    import tk._

    cleanUpCassandra()

    val workerProps = PrimaryWorker.props(hyperbus, db, tracker, 10.seconds)
    val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, tracker, self, scheduler)
    val workerSettings = Map(
      "hyperstorage-primary-worker" → (workerProps, 1, "pgw-"),
      "hyperstorage-secondary-worker" → (secondaryWorkerProps, 1, "sgw-")
    )

    val processor = TestActorRef(ShardProcessor.props(workerSettings, "hyperstorage", tracker))
    val distributor = new HyperbusAdapter(hyperbus, processor, db, tracker, 20.seconds)
    // wait while subscription is completes
    Thread.sleep(2000)

    val createFail = hyperbus.ask(ContentPut("abc", DynamicBody(Obj.from("a" → 10, "x" → "hello")),$headersMap=HeadersMap("if-match" → "\"1\"")))
      .runAsync
      .failed
      .futureValue

    createFail shouldBe a[PreconditionFailed[_]]
  }
}

