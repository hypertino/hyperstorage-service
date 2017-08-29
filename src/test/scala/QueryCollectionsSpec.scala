import com.hypertino.binders.value._
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.utils.{Sort, SortBy}
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, FreeSpec, Matchers}
import org.mockito.Mockito._

class QueryCollectionsSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  import MessagingContext.Implicits.emptyContext

  val c1 = Obj.from("a" → "hello", "b" → 100500)
  val c1x = c1 + Obj.from("id" → "item1")
  val c2 = Obj.from("a" → "goodbye", "b" → 1)
  val c2x = c2 + Obj.from("id" → "item2")
  val c3 = Obj.from("a" → "way way", "b" → 12)
  val c3x = c3 + Obj.from("id" → "item3")

  import com.hypertino.hyperstorage.utils.Sort._

  def setup(): Hyperbus = {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode shouldBe Status.CREATED

    val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
    f2.futureValue.headers.statusCode shouldBe Status.CREATED

    val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode shouldBe Status.CREATED

    reset(db)
    hyperbus
  }

  def setupIndexes(hyperbus: Hyperbus): Unit = {
    hyperbus.ask(IndexPost("collection-1~", HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10"))))
      .runAsync
      .futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexDefUp1 = db.selectIndexDef("collection-1~", "index1").futureValue
      indexDefUp1 shouldBe defined
      indexDefUp1.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    hyperbus.ask(IndexPost("collection-1~",
      HyperStorageIndexNew(Some("index2"), Seq(HyperStorageIndexSortItem("a", order = Some("asc"), fieldType = Some("text"))), Some("b > 10"))))
      .runAsync
      .futureValue.statusCode should equal(Status.CREATED)

    eventually {
      val indexDefUp2 = db.selectIndexDef("collection-1~", "index2").futureValue
      indexDefUp2 shouldBe defined
      indexDefUp2.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    hyperbus.ask(IndexPost("collection-1~",
      HyperStorageIndexNew(Some("index3"), Seq(HyperStorageIndexSortItem("a", order = Some("asc"), fieldType = Some("text"))), None)))
      .runAsync
      .futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexDefUp3 = db.selectIndexDef("collection-1~", "index3").futureValue
      indexDefUp3 shouldBe defined
      indexDefUp3.get.status shouldBe IndexDef.STATUS_NORMAL
    }
    reset(db)
  }

  "QueryCollections" should "Query without sorting and indexes with fiter by id (+check correlationId)" in {
    val hyperbus = setup()
    // setupIndexes(hyperbus)
    // query by id asc
    implicit val mcx = MessagingContext("abc123")
    val get = ContentGet("collection-1~", perPage = Some(5), filter = Some("id =\"item3\""))(mcx)
    val res = hyperbus
      .ask(get)
      .runAsync
      .futureValue
    res.headers.statusCode shouldBe Status.OK
    res.headers.correlationId shouldBe "abc123"
    res.body.content shouldBe Lst.from(c3x)
    verify(db).selectContentCollection("collection-1~", 5, Some(("item3", FilterEq)), true)
  }

  it should "Query without sorting and indexes with fiter by other field" in {
    val hyperbus = setup()
    // setupIndexes(hyperbus)
    // query by id asc
    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(1), filter = Some("a =\"way way\"")))
      .runAsync
      .futureValue
    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c3x)
    verify(db).selectContentCollection("collection-1~", 1, None, true)
    verify(db).selectContentCollection("collection-1~", 500, Some(("item1", FilterGt)), true)
  }

  it should "Query by id asc" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)
    // query by id asc
    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("id")))
      .runAsync
      .futureValue
    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c1x, c2x, c3x)
    verify(db).selectContentCollection("collection-1~", 50, None, true)
  }

  it should "Query by id asc and return Link header if there are more" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)
    // query by id asc
    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(1), sortBy = Some("id")))
      .runAsync
      .futureValue
    res.headers.statusCode shouldBe Status.OK
    res.headers.link shouldBe Map("next_page_url" → HRL(
      location = "hb://hyperstorage/content/{path}",
      query =
        Obj.from(
          "path" → "collection-1~",
          "skip_max" → Null,
          "per_page" → 1,
          "filter" → "id > \"item1\"",
          "sort_by" → "id"
        )
    ))

    res.body.content shouldBe Lst.from(c1x)
    verify(db).selectContentCollection("collection-1~", 1, None, true)

    val res2 = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(1), sortBy = Some("id"), filter = Some("id > \"item3\"")))
      .runAsync
      .futureValue
    res2.headers.statusCode shouldBe Status.OK
    res2.headers.link shouldBe Map.empty
    res2.body.content shouldBe Lst.empty
  }

  it should "Query by id desc" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("-id")))
      .runAsync
      .futureValue
    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c3x, c2x, c1x)
    verify(db).selectContentCollection("collection-1~", 50, None, false)
  }

  it should "Query by id asc and filter by id" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("id"), filter = Some("id >\"item1\"")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c2x, c3x)
    verify(db).selectContentCollection("collection-1~", 50, Some(("item1", FilterGt)), true)
  }

  it should "Query by id desc and filter by id" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("-id"), filter = Some("id <\"item3\"")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c2x, c1x)
    verify(db).selectContentCollection("collection-1~", 50, Some(("item3", FilterLt)), false)
  }

  it should "Query with filter by some non-index field" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(2), filter = Some("a =\"way way\"")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c3x)
    verify(db).selectContentCollection("collection-1~", 2, None, true)
    verify(db).selectContentCollection("collection-1~", 501, Some(("item2", FilterGt)), true)
  }

  it should "Query with filter by some non-index field and descending sorting" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(2), sortBy = Some("-id"), filter = Some("a =\"hello\"")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c1x)
    verify(db).selectContentCollection("collection-1~", 2, None, false)
    verify(db).selectContentCollection("collection-1~", 501, Some(("item2", FilterLt)), false)
  }

  it should "Query with filter and sorting by some non-index field (full table scan)" in {
    val hyperbus = setup()

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(2), sortBy = Some("a"), filter = Some("a >\"goodbye\"")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c1x, c3x)
    verify(db).selectContentCollection("collection-1~", 10002, None, true)
  }

  it should "Query with filter and sorting descending by some non-index field (full table scan)" in {
    val hyperbus = setup()

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(2), sortBy = Some("-a"), filter = Some("a >\"goodbye\"")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c3x, c1x)
    verify(db).selectContentCollection("collection-1~", 10002, None, true)
  }

  it should "Query when filter matches index filter and sort order" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("id"), filter = Some("b > 10")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c1x, c3x)
    verify(db).selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq(CkField("item_id", true)), 50)

    val res2 = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("a"), filter = Some("b > 10")))
      .runAsync
      .futureValue

    res2.statusCode shouldBe Status.OK
    res2.body.content shouldBe Lst.from(c1x, c3x)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq.empty, Seq(CkField("t0", true)), 50)
  }

  it should "Query when filter matches index filter and reversed sort order" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("-a"), filter = Some("b > 10")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c3x, c1x)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq.empty, Seq(CkField("t0", false)), 50)

    val res2 = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("-a,-id"), filter = Some("b > 10")))
      .runAsync
      .futureValue

    res2.statusCode shouldBe Status.OK
    res2.body.content shouldBe Lst.from(c3x, c1x)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq.empty, Seq(CkField("t0", false), CkField("item_id", false)), 50)
  }

  it should "Query when filter partially matches index filter and sort order" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("id"), filter = Some("b > 12")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c1x)
    verify(db).selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq(CkField("item_id", true)), 50)
  }

  it should "Query when filter partially matches index filter and sort order with CK field filter" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("a"), filter = Some("b > 10 and a > \"hello\"")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c3x)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter("t0", Text("hello"), FilterGt)), Seq(CkField("t0", true)), 50)

    val res2 = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(50), sortBy = Some("a,id"), filter = Some("b > 10 and a = \"hello\" and id > \"item2\"")))
      .runAsync
      .futureValue

    res2.statusCode shouldBe Status.OK
    res2.body.content shouldBe Lst.empty
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter("t0", Text("hello"), FilterEq), FieldFilter("item_id", Text("item2"), FilterGt)), Seq(CkField("t0", true), CkField("item_id", true)), 50)
  }

  it should "Query when sort order matches with CK fields (skipping unmatched to the filter)" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(2), sortBy = Some("a"), filter = Some("b < 50")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c2x, c3x)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq.empty, Seq(CkField("t0", true)), 2)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("hello"), FilterEq), FieldFilter("item_id", Text("item1"), FilterGt)), Seq(CkField("t0", true)), 501)
  }

  it should "Query when sort order matches with CK fields (skipping unmatched to the filter) with query-filter-ck-fields" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(2), sortBy = Some("a"), filter = Some("b < 50 and a < \"zzz\"")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c2x, c3x)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("zzz"), FilterLt)), Seq(CkField("t0", true)), 2)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("hello"), FilterEq), FieldFilter("item_id", Text("item1"), FilterGt)), Seq(CkField("t0", true)), 501)
  }

  it should "Query when sort order matches with CK fields (skipping unmatched to the filter) with query-filter-ck-fields / reversed" in {
    val hyperbus = setup()
    setupIndexes(hyperbus)

    val res = hyperbus
      .ask(ContentGet("collection-1~", perPage = Some(2), sortBy = Some("-a"), filter = Some("b < 50 and a > \"aaa\"")))
      .runAsync
      .futureValue

    res.headers.statusCode shouldBe Status.OK
    res.body.content shouldBe Lst.from(c3x, c2x)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("aaa"), FilterGt)), Seq(CkField("t0", false)), 2)
    verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("hello"), FilterEq), FieldFilter("item_id", Text("item1"), FilterLt)), Seq(CkField("t0", false)), 501)
  }
}

