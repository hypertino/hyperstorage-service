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

class IncrementPatchTest extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(20000, Millis)))
  implicit val emptyContext = MessagingContext.empty

  "increment" should "work" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

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
      headers=Headers(Header.CONTENT_TYPE → HyperStoragePatchType.HYPERSTORAGE_CONTENT_INCREMENT))
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

