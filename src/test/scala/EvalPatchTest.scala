import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperstorage.api._
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}

class EvalPatchTest extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(20000, Millis)))
  implicit val emptyContext = MessagingContext.empty

  "eval patch" should "work" in {
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

    val patch = ContentPatch("abc",
      DynamicBody(Obj.from("a" → "original.a * 2", "b" → "original.b + 1", "x" → "original.x + ' john'"),
        Some(HyperStoragePatchType.HYPERSTORAGE_CONTENT_EVALUATE))
      )

    val patchEval = hyperbus.ask(
      patch
    )
      .runAsync
      .futureValue

    patchEval shouldBe a[Ok[_]]
    val ok2 = hyperbus.ask(ContentGet("abc"))
      .runAsync
      .futureValue

    ok2 shouldBe a[Ok[_]]
    ok2.body shouldBe DynamicBody(Obj.from("a" → 20, "b" → 9, "x" → "hello john"))
  }
}

