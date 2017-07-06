import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.db.{FieldFilter, FilterGt, IndexDef}
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FreeSpec, Matchers}

class IndexingSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(11000, Millis)))
  import MessagingContext.Implicits.emptyContext

  "IndexingSpec" in {
    "Create index without sorting or filtering" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val path = "collection-1~"
      val f2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None))).runAsync
      f2.futureValue.headers.statusCode should equal(Status.CREATED)

      val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
      indexDef shouldBe defined
      indexDef.get.documentUri shouldBe "collection-1~"
      indexDef.get.indexId shouldBe "index1"

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      val c3 = Obj.from("a" → "goodbye", "b" → 123456)
      val f3 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c3))).runAsync
      f3.futureValue.headers.statusCode should equal(Status.CREATED)

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 2
        indexContent(1).documentUri shouldBe "collection-1~"
        indexContent(1).itemId shouldBe "item2"
        indexContent(1).body.get should include("\"item2\"")
      }
    }

    "Create index with filter" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val c1x = c1 + Obj.from("id" → "item1")
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val path = "collection-1~"
      val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10")))).runAsync
      fi.futureValue.headers.statusCode should equal(Status.CREATED)

      val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
      indexDef shouldBe defined
      indexDef.get.documentUri shouldBe "collection-1~"
      indexDef.get.indexId shouldBe "index1"

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      val c2 = Obj.from("a" → "goodbye", "b" → 1)
      val c2x = Obj(c2.toMap + "id" → "item2")
      val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
      f2.futureValue.headers.statusCode should equal(Status.CREATED)

      val c3 = Obj.from("a" → "way way", "b" → 12)
      val c3x = Obj(c3.toMap + "id" → "item3")
      val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
      f3.futureValue.headers.statusCode should equal(Status.CREATED)

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 2
        indexContent(1).documentUri shouldBe "collection-1~"
        indexContent(1).itemId shouldBe "item3"
        indexContent(1).body.get should include("\"item3\"")
      }

      removeContent("collection-1~","item1").futureValue
      removeContent("collection-1~","item2").futureValue
      removeContent("collection-1~","item3").futureValue

      import com.hypertino.hyperstorage.utils.Sort._
      val f4 = hyperbus.ask(ContentGet(
        path = "collection-1~",
        filter = Some("b > 10"),
        size = Some(50)
      )).runAsync
      val rc4 = f4.futureValue

      rc4.headers.statusCode shouldBe Status.OK
      rc4.body.content shouldBe Obj.from("_embedded" -> Obj.from("els" → Lst.from(c1x,c3x)))

      val f5 = hyperbus.ask(ContentGet("collection-1~",
        filter = Some("b < 10"),
        size = Some(50)
      )).runAsync
      val rc5 = f5.futureValue
      rc5.headers.statusCode shouldBe Status.OK
      rc5.body.content shouldBe Obj.from("_embedded" -> Obj.from("els" → Lst.empty))
    }

    "Deleting item should remove it from index" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val c1x = c1 + Obj.from("id" → "item1")
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val path = "collection-1~"
      val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10")))).runAsync
      fi.futureValue.headers.statusCode should equal(Status.CREATED)

      val fi2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index2"),
        Seq(HyperStorageIndexSortItem("a", order = None, fieldType = None)),
        Some("b > 10")))).runAsync
      fi2.futureValue.headers.statusCode should equal(Status.CREATED)

      eventually {
        val indexDefUp1 = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp1 shouldBe defined
        indexDefUp1.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexDefUp2 = db.selectIndexDef("collection-1~", "index2").futureValue
        indexDefUp2 shouldBe defined
        indexDefUp2.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexContent1 = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
          "item_id", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent1.size shouldBe 1
        indexContent1.head.documentUri shouldBe "collection-1~"
        indexContent1.head.itemId shouldBe "item1"
        indexContent1.head.body.get should include("\"item1\"")
      }

      eventually {
        val indexContent2 = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
          "t0", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent2.size shouldBe 1
        indexContent2.head.documentUri shouldBe "collection-1~"
        indexContent2.head.itemId shouldBe "item1"
        indexContent2.head.body.get should include("\"item1\"")
      }

      val f2 = hyperbus.ask(ContentDelete("collection-1~/item1", EmptyBody)).runAsync
      f2.futureValue.statusCode should equal(Status.OK)

      eventually {
        val indexContent1 = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
          "item_id", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent1 shouldBe empty
      }

      eventually {
        val indexContent2 = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
          "t0", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent2 shouldBe empty
      }
    }

    "Patching item should update index" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val c1x = c1 + Obj.from("id" → "item1")
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val path = "collection-1~"
      val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10")))).runAsync
      fi.futureValue.headers.statusCode should equal(Status.CREATED)

      val fi2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index2"),
        Seq(HyperStorageIndexSortItem("a", order = None, fieldType = None)),
        Some("b > 10")))).runAsync
      fi2.futureValue.headers.statusCode should equal(Status.CREATED)

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexDefUp2 = db.selectIndexDef("collection-1~", "index2").futureValue
        indexDefUp2 shouldBe defined
        indexDefUp2.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
          "item_id", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.revision shouldBe 1
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
          "t0", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.revision shouldBe 1
      }

      val c2 = Obj.from("a" → "goodbye")
      val c2x = c2 + Obj.from("id" → "item1")
      val f2 = hyperbus.ask(ContentPatch("collection-1~/item1", DynamicBody(c2))).runAsync
      f2.futureValue.statusCode should equal(Status.OK)

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
          "item_id", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.body.get should include("\"goodbye\"")
        indexContent.head.revision shouldBe 2
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
          "t0", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.body.get should include("\"goodbye\"")
        indexContent.head.revision shouldBe 2
      }

      val c3 = Obj.from("b" → 5)
      val f3 = hyperbus.ask(ContentPatch("collection-1~/item1", DynamicBody(c3))).runAsync
      f3.futureValue.statusCode should equal(Status.OK)

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
          "item_id", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent shouldBe empty
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index1", Seq(FieldFilter(
          "t0", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent shouldBe empty
      }
    }

    "Putting over existing item should update index" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val c1x = c1 + Obj.from("id" → "item1")
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val path = "collection-1~"
      val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10")))).runAsync
      fi.futureValue.statusCode should equal(Status.CREATED)

      val fi2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index2"),
        Seq(HyperStorageIndexSortItem("a", order = None, fieldType = None)),
        Some("b > 10")))).runAsync
      fi2.futureValue.statusCode should equal(Status.CREATED)

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index2").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
          "item_id", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
          "t0", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      val c2 = Obj.from("a" → "goodbye", "b" → 30)
      val c2x = c2 + Obj.from("id" → "item1")
      val f2 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c2))).runAsync
      f2.futureValue.headers.statusCode should equal(Status.OK)

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
          "item_id", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.body.get should include("\"goodbye\"")
        indexContent.head.revision shouldBe 2
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
          "t0", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.body.get should include("\"goodbye\"")
        indexContent.head.revision shouldBe 2
      }

      val c3 = Obj.from("a" → "hello", "b" → 5)
      val f3 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c3))).runAsync
      f3.futureValue.headers.statusCode should equal(Status.OK)

      eventually {
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
          "item_id", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent shouldBe empty
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
          "t0", "", FilterGt
        )), Seq.empty, 10).futureValue.toSeq
        indexContent shouldBe empty
      }
    }

    "Create index with filter and decimal sorting" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val path = "collection-1~"
      val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"),
        Seq(HyperStorageIndexSortItem("b", order = Some("asc"), fieldType = Some("decimal"))), Some("b > 10")))).runAsync
      fi.futureValue.statusCode should equal(Status.CREATED)

      val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
      indexDef shouldBe defined
      indexDef.get.documentUri shouldBe "collection-1~"
      indexDef.get.indexId shouldBe "index1"

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_da0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      val c2 = Obj.from("a" → "goodbye", "b" → 1)
      val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
      f2.futureValue.headers.statusCode should equal(Status.CREATED)

      val c3 = Obj.from("a" → "way way", "b" → 12)
      val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
      f3.futureValue.headers.statusCode should equal(Status.CREATED)

      eventually {
        val indexContent = db.selectIndexCollection("index_content_da0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 2
        indexContent(0).documentUri shouldBe "collection-1~"
        indexContent(0).itemId shouldBe "item3"
        indexContent(0).body.get should include("\"item3\"")

        indexContent(1).documentUri shouldBe "collection-1~"
        indexContent(1).itemId shouldBe "item1"
        indexContent(1).body.get should include("\"item1\"")
      }
    }

    "Create index with filter and decimal desc sorting" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val path = "collection-1~"
      val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"),
        Seq(HyperStorageIndexSortItem("b", order = Some("desc"), fieldType = Some("decimal"))), Some("b > 10")))).runAsync
      fi.futureValue.statusCode should equal(Status.CREATED)

      val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
      indexDef shouldBe defined
      indexDef.get.documentUri shouldBe "collection-1~"
      indexDef.get.indexId shouldBe "index1"

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_dd0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      val c2 = Obj.from("a" → "goodbye", "b" → 1)
      val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
      f2.futureValue.headers.statusCode should equal(Status.CREATED)

      val c3 = Obj.from("a" → "way way", "b" → 12)
      val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
      f3.futureValue.headers.statusCode should equal(Status.CREATED)

      eventually {
        val indexContent = db.selectIndexCollection("index_content_dd0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 2
        indexContent(0).documentUri shouldBe "collection-1~"
        indexContent(0).itemId shouldBe "item1"
        indexContent(0).body.get should include("\"item1\"")

        indexContent(1).documentUri shouldBe "collection-1~"
        indexContent(1).itemId shouldBe "item3"
        indexContent(1).body.get should include("\"item3\"")
      }
    }

    "Create index with filter and text sorting" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val path = "collection-1~"
      val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"),
        Seq(HyperStorageIndexSortItem("a", order = Some("asc"), fieldType = Some("text"))), Some("b > 10")))).runAsync
      fi.futureValue.statusCode should equal(Status.CREATED)

      val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
      indexDef shouldBe defined
      indexDef.get.documentUri shouldBe "collection-1~"
      indexDef.get.indexId shouldBe "index1"

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      val c2 = Obj.from("a" → "goodbye", "b" → 1)
      val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
      f2.futureValue.headers.statusCode should equal(Status.CREATED)

      val c3 = Obj.from("a" → "way way", "b" → 12)
      val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
      f3.futureValue.headers.statusCode should equal(Status.CREATED)

      eventually {
        val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        //println(indexContent)
        indexContent.size shouldBe 2
        indexContent(0).documentUri shouldBe "collection-1~"
        indexContent(0).itemId shouldBe "item1"
        indexContent(0).body.get should include("\"item1\"")

        indexContent(1).documentUri shouldBe "collection-1~"
        indexContent(1).itemId shouldBe "item3"
        indexContent(1).body.get should include("\"item3\"")
      }
    }

    "Create index with filter and text desc sorting" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val path = "collection-1~"
      val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"),
        Seq(HyperStorageIndexSortItem("a", order = Some("desc"), fieldType = Some("text"))), Some("b > 10")))).runAsync
      fi.futureValue.statusCode should equal(Status.CREATED)

      val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
      indexDef shouldBe defined
      indexDef.get.documentUri shouldBe "collection-1~"
      indexDef.get.indexId shouldBe "index1"

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      eventually {
        val indexContent = db.selectIndexCollection("index_content_td0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      val c2 = Obj.from("a" → "goodbye", "b" → 1)
      val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
      f2.futureValue.headers.statusCode should equal(Status.CREATED)

      val c3 = Obj.from("a" → "way way", "b" → 12)
      val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
      f3.futureValue.headers.statusCode should equal(Status.CREATED)

      eventually {
        val indexContent = db.selectIndexCollection("index_content_td0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 2
        indexContent(0).documentUri shouldBe "collection-1~"
        indexContent(0).itemId shouldBe "item3"
        indexContent(0).body.get should include("\"item3\"")

        indexContent(1).documentUri shouldBe "collection-1~"
        indexContent(1).itemId shouldBe "item1"
        indexContent(1).body.get should include("\"item1\"")
      }
    }

    "Create and delete index" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val path = "collection-1~"
      val f2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None))).runAsync
      f2.futureValue.statusCode should equal(Status.CREATED)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
      indexDef shouldBe defined
      indexDef.get.documentUri shouldBe "collection-1~"
      indexDef.get.indexId shouldBe "index1"

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      val f3 = hyperbus.ask(IndexDelete(path, "index1")).runAsync
      f3.futureValue.headers.statusCode should equal(Status.NO_CONTENT)

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe None
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent shouldBe empty
      }
    }

    "Collection removal should remove index" in {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)

      val path = "collection-1~"
      val f2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None))).runAsync
      f2.futureValue.statusCode should equal(Status.CREATED)

      val c1 = Obj.from("a" → "hello", "b" → 100500)
      val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
      f1.futureValue.headers.statusCode should equal(Status.CREATED)

      val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
      indexDef shouldBe defined
      indexDef.get.documentUri shouldBe "collection-1~"
      indexDef.get.indexId shouldBe "index1"

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe defined
        indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent.size shouldBe 1
        indexContent.head.documentUri shouldBe "collection-1~"
        indexContent.head.itemId shouldBe "item1"
        indexContent.head.body.get should include("\"item1\"")
      }

      val f3 = hyperbus.ask(ContentDelete(path)).runAsync
      f3.futureValue.statusCode should equal(Status.OK)

      eventually {
        val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp shouldBe None
        val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
        indexContent shouldBe empty
      }
    }
  }
}
