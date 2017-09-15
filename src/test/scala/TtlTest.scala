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
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class TtlTest extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(15, Seconds)))
  implicit val emptyContext = MessagingContext.empty

  "ttl" should "work on documents" in {
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

    val docCreated = hyperbus.ask(ContentPut("abc", DynamicBody(Obj.from("a" → 10)),
      $headersMap = HeadersMap(HyperStorageHeader.HYPER_STORAGE_TTL → 1)))
      .runAsync
      .futureValue

    docCreated shouldBe a[Created[_]]

    val docOk = hyperbus.ask(ContentGet("abc"))
      .runAsync
      .futureValue

    docOk shouldBe a[Ok[_]]
    docOk.body shouldBe DynamicBody(Obj.from("a" → 10))

    eventually {
      val notFound = hyperbus.ask(ContentGet("abc"))
        .runAsync
        .failed
        .futureValue

      notFound shouldBe a[NotFound[_]]
    }
  }

  it should "work on views" in {
    val hyperbus = integratedHyperbus(db)

    hyperbus.ask(ViewPut("abcs~", HyperStorageView("abc/{*}")))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val h = db.selectViewDefs().futureValue.toSeq.head
      h.documentUri shouldBe "abcs~"
      h.templateUri shouldBe "abc/{*}"
    }

    val docCreated = hyperbus.ask(ContentPut("abc/1", DynamicBody(Obj.from("a" → 10)),
      $headersMap = HeadersMap(HyperStorageHeader.HYPER_STORAGE_TTL → 3)))
      .runAsync
      .futureValue

    docCreated shouldBe a[Created[_]]

    val docOk = hyperbus.ask(ContentGet("abc/1"))
      .runAsync
      .futureValue

    docOk shouldBe a[Ok[_]]
    docOk.body shouldBe DynamicBody(Obj.from("a" → 10))

    eventually {
      val ok = hyperbus.ask(ContentGet("abcs~/1"))
        .runAsync
        .futureValue

      ok shouldBe a[Ok[_]]
    }

    eventually {
      hyperbus.ask(ContentGet("abc"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }

    eventually {
      hyperbus.ask(ContentGet("abcs~/1"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }
  }

  it should "work on collection items and indexes" in {
    val hyperbus = integratedHyperbus(db)

    val path = "abc~"
    val f2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None, materialize=Some(true)))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.CREATED)

    val itemCreated = hyperbus.ask(ContentPut("abc~/1", DynamicBody(Obj.from("a" → 10)),
      $headersMap=HeadersMap(HyperStorageHeader.HYPER_STORAGE_TTL → 3)))
      .runAsync
      .futureValue
    itemCreated shouldBe a[Created[_]]
    val itemCreated2 = hyperbus.ask(ContentPut("abc~/2", DynamicBody(Obj.from("a" → 10))))
      .runAsync
      .futureValue
    itemCreated2 shouldBe a[Created[_]]

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "abc~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
      indexContent.size shouldBe 2
    }

    eventually {
      val notFound = hyperbus.ask(ContentGet("abc~/1"))
        .runAsync
        .failed
        .futureValue

      notFound shouldBe a [NotFound[_]]
    }

    val itemOk2 = hyperbus.ask(ContentGet("abc~/2"))
      .runAsync
      .futureValue

    itemOk2 shouldBe a[Ok[_]]

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "abc~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
      indexContent.size shouldBe 1
    }
  }
}

