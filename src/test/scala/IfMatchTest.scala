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
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val created = hyperbus.ask(ContentPut("abc", DynamicBody(Obj.from("a" → 10, "x" → "hello"))))
      .runAsync
      .futureValue

    created shouldBe a[Created[_]]

    val ok = hyperbus.ask(ContentGet("abc"))
      .runAsync
      .futureValue

    ok shouldBe a[Ok[_]]
    ok.body shouldBe DynamicBody(Obj.from("a" → 10, "x" → "hello"))

    val rev = ok.headers(Header.REVISION)
    val etag = ok.headers(HyperStorageHeader.ETAG)
    etag shouldBe Text("\"1\"")
    val wrongEtag = "\"" + etag.toString + "x" + "\""

    val notModified = hyperbus.ask(ContentGet("abc", $headersMap=HeadersMap(HyperStorageHeader.IF_MATCH → etag)))
      .runAsync
      .futureValue

    notModified shouldBe a[NotModified[_]]

    val deleteFail = hyperbus.ask(ContentDelete("abc", $headersMap=HeadersMap(HyperStorageHeader.IF_MATCH → wrongEtag)))
      .runAsync
      .failed
      .futureValue

    deleteFail shouldBe a[PreconditionFailed[_]]

    val deleteFail2 = hyperbus.ask(ContentDelete("abc", $headersMap=HeadersMap(HyperStorageHeader.IF_NONE_MATCH → "*")))
      .runAsync
      .failed
      .futureValue

    deleteFail2 shouldBe a[PreconditionFailed[_]]

    val delete = hyperbus.ask(ContentDelete("abc", $headersMap=HeadersMap(HyperStorageHeader.IF_MATCH → etag)))
      .runAsync
      .futureValue

    delete shouldBe a[Ok[_]]
  }

  it should "fail if resource with etag not match" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val createFail = hyperbus.ask(ContentPut("abc", DynamicBody(Obj.from("a" → 10, "x" → "hello")), $headersMap=HeadersMap(HyperStorageHeader.IF_MATCH → "\"1\"")))
      .runAsync
      .failed
      .futureValue

    createFail shouldBe a[PreconditionFailed[_]]
  }

  it should "work on empty etag for a collection" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val create = hyperbus.ask(ContentPut("abc~/1", DynamicBody(Obj.from("a" → 1))))
      .runAsync
      .futureValue

    create shouldBe a[Created[_]]

    val create2 = hyperbus.ask(ContentPut("abc~/2", DynamicBody(Obj.from("a" → 2)), $headersMap=HeadersMap(HyperStorageHeader.IF_NONE_MATCH → "*")))
      .runAsync
      .futureValue

    create2 shouldBe a[Created[_]]

    val createFail1 = hyperbus.ask(ContentPut("abc~/2", DynamicBody(Obj.from("a" → 2)), $headersMap=HeadersMap(HyperStorageHeader.IF_NONE_MATCH → "*")))
      .runAsync
      .failed
      .futureValue

    createFail1 shouldBe a[PreconditionFailed[_]]

    val createFail2 = hyperbus.ask(ContentPut("abc~/3", DynamicBody(Obj.from("a" → 10, "x" → "hello")), $headersMap=HeadersMap(HyperStorageHeader.IF_MATCH → "\"1\"")))
      .runAsync
      .failed
      .futureValue

    createFail2 shouldBe a[PreconditionFailed[_]]
  }
}

