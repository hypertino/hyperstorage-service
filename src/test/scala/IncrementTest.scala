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

class IncrementTest extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(20000, Millis)))
  implicit val emptyContext = MessagingContext.empty

  "increment" should "work" in {
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

    val create = hyperbus.ask(ContentPut("abc", DynamicBody(Obj.from("a" → 10, "b" → 8, "x" → "hello"))))
      .runAsync
      .futureValue

    create shouldBe a[Created[_]]

    val ok = hyperbus.ask(ContentGet("abc"))
      .runAsync
      .futureValue

    ok shouldBe a[Ok[_]]
    ok.body shouldBe DynamicBody(Obj.from("a" → 10, "b" → 8, "x" → "hello"))

    val patchIncrement = hyperbus.ask(ContentPatch("abc",
      DynamicBody(Obj.from("a" → 3, "b" → -2)),
      $headersMap = HeadersMap(Header.CONTENT_TYPE → "hyperstorage-content-increment"))
    )
      .runAsync
      .futureValue

    patchIncrement shouldBe a[Ok[_]]
    val ok2 = hyperbus.ask(ContentGet("abc"))
      .runAsync
      .futureValue

    ok2 shouldBe a[Ok[_]]
    ok2.body shouldBe DynamicBody(Obj.from("a" → 13, "b" → 6, "x" → "hello"))
  }
}

