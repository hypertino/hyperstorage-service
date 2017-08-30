import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.{Conflict, Created, DynamicBody, MessagingContext, NotFound, Ok, Status}
import com.hypertino.hyperstorage.api._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}

class ViewSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(11000, Millis)))

  import MessagingContext.Implicits.emptyContext

  "Views" should "work on documents" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    hyperbus.ask(ViewPut("abcs~", HyperStorageView("abc/{*}")))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val h = db.selectViewDefs().futureValue.toSeq.head
      h.documentUri shouldBe "abcs~"
      h.templateUri shouldBe "abc/{*}"
    }

    hyperbus.ask(ContentPut("def/456", DynamicBody(Obj.from("b" → 10, "y" → "hello"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    hyperbus.ask(ContentPut("abc/123", DynamicBody(Obj.from("a" → 10, "x" → "hello"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val ok = hyperbus.ask(ContentGet("abcs~/123"))
        .runAsync
        .futureValue

      ok shouldBe a[Ok[_]]
      ok.body shouldBe DynamicBody(Obj.from("a" → 10, "x" → "hello", "abc_id" → "123"))

      hyperbus.ask(ContentGet("abcs~/456"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }

    hyperbus.ask(ContentDelete("abc/123"))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    eventually {
      hyperbus.ask(ContentGet("abcs~/123"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }
  }

  it should "work on documents with filter" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    hyperbus.ask(ViewPut("abcs~", HyperStorageView("abc/{*}", Some("a > 5"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val h = db.selectViewDefs().futureValue.toSeq.head
      h.documentUri shouldBe "abcs~"
      h.templateUri shouldBe "abc/{*}"
    }

    hyperbus.ask(ContentPut("abc/123", DynamicBody(Obj.from("a" → 1, "x" → "hello"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    hyperbus.ask(ContentPut("abc/456", DynamicBody(Obj.from("a" → 10, "x" → "bye"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val ok = hyperbus.ask(ContentGet("abcs~/456"))
        .runAsync
        .futureValue

      ok shouldBe a[Ok[_]]
      ok.body shouldBe DynamicBody(Obj.from("a" → 10, "x" → "bye", "abc_id" → "456"))

      hyperbus.ask(ContentGet("abcs~/123"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }

    hyperbus.ask(ContentPatch("abc/456", DynamicBody(Obj.from("a" → 2))))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    eventually {
      hyperbus.ask(ContentGet("abcs~/456"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }

    hyperbus.ask(ContentPatch("abc/123", DynamicBody(Obj.from("a" → 10))))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    eventually {
      val ok = hyperbus.ask(ContentGet("abcs~/123"))
        .runAsync
        .futureValue

      ok shouldBe a[Ok[_]]
      ok.body shouldBe DynamicBody(Obj.from("a" → 10, "x" → "hello", "abc_id" → "123"))
    }
  }

  it should "work on collections" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    hyperbus.ask(ViewPut("abcs~", HyperStorageView("collection~/{*}")))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val h = db.selectViewDefs().futureValue.toSeq.head
      h.documentUri shouldBe "abcs~"
      h.templateUri shouldBe "collection~/{*}"
    }

    hyperbus.ask(ContentPut("othercol~/456", DynamicBody(Obj.from("b" → 10, "y" → "hello"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    hyperbus.ask(ContentPut("collection~/123", DynamicBody(Obj.from("a" → 10, "x" → "hello"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val ok = hyperbus.ask(ContentGet("abcs~/123"))
        .runAsync
        .futureValue

      ok shouldBe a[Ok[_]]
      ok.body shouldBe DynamicBody(Obj.from("a" → 10, "x" → "hello", "abc_id" → "123", "collection_id" → "123"))

      hyperbus.ask(ContentGet("abcs~/456"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }

    hyperbus.ask(ContentDelete("collection~/123"))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    eventually {
      hyperbus.ask(ContentGet("abcs~/123"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }
  }

  it should "work on collections with filter" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    hyperbus.ask(ViewPut("abcs~", HyperStorageView("collection~/{*}", Some("a > 5"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val h = db.selectViewDefs().futureValue.toSeq.head
      h.documentUri shouldBe "abcs~"
      h.templateUri shouldBe "collection~/{*}"
    }

    hyperbus.ask(ContentPut("collection~/123", DynamicBody(Obj.from("a" → 1, "x" → "hello"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    hyperbus.ask(ContentPut("collection~/456", DynamicBody(Obj.from("a" → 10, "x" → "bye"))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val ok = hyperbus.ask(ContentGet("abcs~/456"))
        .runAsync
        .futureValue

      ok shouldBe a[Ok[_]]
      ok.body shouldBe DynamicBody(Obj.from("a" → 10, "x" → "bye", "collection_id" → "456", "abc_id" → "456"))

      hyperbus.ask(ContentGet("abcs~/123"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }

    hyperbus.ask(ContentPatch("collection~/456", DynamicBody(Obj.from("a" → 2))))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    eventually {
      hyperbus.ask(ContentGet("abcs~/456"))
        .runAsync
        .failed
        .futureValue shouldBe a[NotFound[_]]
    }

    hyperbus.ask(ContentPatch("collection~/123", DynamicBody(Obj.from("a" → 10))))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    eventually {
      val ok = hyperbus.ask(ContentGet("abcs~/123"))
        .runAsync
        .futureValue

      ok shouldBe a[Ok[_]]
      ok.body shouldBe DynamicBody(Obj.from("a" → 10, "x" → "hello", "collection_id" → "123", "abc_id" → "123"))
    }
  }

  it should "disallow direct modification of view" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    hyperbus.ask(ViewPut("abcs~", HyperStorageView("abc/{*}")))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val h = db.selectViewDefs().futureValue.toSeq.head
      h.documentUri shouldBe "abcs~"
      h.templateUri shouldBe "abc/{*}"
    }

    hyperbus.ask(ContentPut("abc/123", DynamicBody(Obj.from("a" → 1))))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val ok = hyperbus.ask(ContentGet("abcs~/123"))
        .runAsync
        .futureValue

      ok shouldBe a[Ok[_]]
    }

    hyperbus.ask(ContentPatch("abcs~/123", DynamicBody(Obj.from("a" → 2))))
      .runAsync
      .failed
      .futureValue shouldBe a[Conflict[_]]

    hyperbus.ask(ContentPost("abcs~", DynamicBody(Obj.from("a" → 3))))
      .runAsync
      .failed
      .futureValue shouldBe a[Conflict[_]]
  }
}
