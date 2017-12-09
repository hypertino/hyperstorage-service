/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.binders.value._
import com.hypertino.hyperbus.model._
import com.hypertino.hyperstorage.api._
import com.hypertino.hyperstorage.db.{FieldFilter, FilterGt, IndexDef}
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, FreeSpec, Matchers}

class IndexingSpecMaterialized extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  def materialize: Boolean = true

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(30000, Millis)))

  import MessagingContext.Implicits.emptyContext

  "Indexing" should "Create index without sorting or filtering" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val c1x = c1 + Obj.from("id" → "item1")
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val f2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None, materialize=Some(materialize)))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.CREATED)

    val indexDef = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
    indexDef shouldBe defined
    indexDef.get.documentUri shouldBe "collection-1~"
    indexDef.get.indexId shouldBe "index1"
    indexDef.get.materialize shouldBe materialize

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
      indexContent.head.count shouldBe Some(1l)
    }

    val c3 = Obj.from("a" → "goodbye", "b" → 123456)
    val c3x = c3 + Obj.from("id" → "item2")
    val f3 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 2
      indexContent(1).documentUri shouldBe "collection-1~"
      indexContent(1).itemId shouldBe "item2"
      if (materialize) {
        indexContent(1).body.get should include("\"item2\"")
      }
      else {
        indexContent(1).body shouldBe None
      }
      indexContent.head.count shouldBe Some(2l)
    }

    val f4 = hyperbus.ask(ContentGet(
      path = "collection-1~",
      filter = None,
      perPage = Some(50)
    )).runAsync
    val rc4 = f4.futureValue

    rc4.headers.statusCode shouldBe Status.OK
    rc4.headers.get(Header.COUNT) shouldBe Some(Number(2))
    rc4.body.content shouldBe Lst.from(c1x, c3x)
  }

  it should "Create index with filter" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val c1x = c1 + Obj.from("id" → "item1")
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(
      Some("index1"),
      Seq.empty,
      Some("b > 10"),
      materialize=Some(materialize)
    ))).runAsync
    fi.futureValue.headers.statusCode should equal(Status.CREATED)

    val indexDef = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
    indexDef shouldBe defined
    indexDef.get.documentUri shouldBe "collection-1~"
    indexDef.get.indexId shouldBe "index1"
    indexDef.get.materialize shouldBe materialize

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.count shouldBe Some(1l)
      if (materialize)
        indexContent.head.body.get should include("\"item1\"")
      else
        indexContent.head.body shouldBe None
    }

    val c2 = Obj.from("a" → "goodbye", "b" → 1)
    val c2x = c2 + Obj.from("id" → "item2")
    val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.CREATED)

    val c3 = Obj.from("a" → "way way", "b" → 12)
    val c3x = c3 + Obj.from("id" → "item3")
    val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 2
      indexContent(1).documentUri shouldBe "collection-1~"
      indexContent(1).itemId shouldBe "item3"
      indexContent(1).count shouldBe Some(2l)
      if (materialize)
        indexContent(1).body.get should include("\"item3\"")
      else
        indexContent(1).body shouldBe None
    }

//    removeContent("collection-1~", "item1").runAsync.futureValue
//    removeContent("collection-1~", "item2").runAsync.futureValue
//    removeContent("collection-1~", "item3").runAsync.futureValue

    import com.hypertino.hyperstorage.utils.Sort._
    val f4 = hyperbus.ask(ContentGet(
      path = "collection-1~",
      filter = Some("b > 10"),
      perPage = Some(50)
    )).runAsync
    val rc4 = f4.futureValue

    rc4.headers.statusCode shouldBe Status.OK
    rc4.headers.get(Header.COUNT) shouldBe Some(Number(2))
    rc4.body.content shouldBe Lst.from(c1x, c3x)

    val f5 = hyperbus.ask(ContentGet("collection-1~",
      filter = Some("b < 10"),
      perPage = Some(50)
    )).runAsync
    val rc5 = f5.futureValue
    rc5.headers.statusCode shouldBe Status.OK
    rc5.body.content shouldBe Lst.from(c2x)
  }

  it should "Deleting item should remove it from index" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val c1x = c1 + Obj.from("id" → "item1")
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10"),materialize=Some(materialize)))).runAsync
    fi.futureValue.headers.statusCode should equal(Status.CREATED)

    val fi2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index2"),
      Seq(HyperStorageIndexSortItem("a", order = None, fieldType = None)),
      Some("b > 10"),
      materialize=Some(materialize)
    ))).runAsync
    fi2.futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexDefUp1 = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp1 shouldBe defined
      indexDefUp1.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexDefUp2 = db.selectIndexDef("collection-1~", "index2").runAsync.futureValue
      indexDefUp2 shouldBe defined
      indexDefUp2.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent1 = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent1.size shouldBe 1
      indexContent1.head.documentUri shouldBe "collection-1~"
      indexContent1.head.itemId shouldBe "item1"
      indexContent1.head.count shouldBe Some(1l)

      if (materialize)
        indexContent1.head.body.get should include("\"item1\"")
      else
        indexContent1.head.body shouldBe None
    }

    eventually {
      val indexContent2 = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
        "t0", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent2.size shouldBe 1
      indexContent2.head.documentUri shouldBe "collection-1~"
      indexContent2.head.itemId shouldBe "item1"
      indexContent2.head.count shouldBe Some(1l)

      if (materialize)
        indexContent2.head.body.get should include("\"item1\"")
      else
        indexContent2.head.body shouldBe None
    }

    val f2 = hyperbus.ask(ContentDelete("collection-1~/item1", EmptyBody)).runAsync
    f2.futureValue.statusCode should equal(Status.OK)

    eventually {
      val indexContent1 = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent1 shouldBe empty
    }

    eventually {
      val indexContent2 = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
        "t0", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent2 shouldBe empty
    }
  }

  it should "Patching item should update index" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val c1x = c1 + Obj.from("id" → "item1")
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10"),materialize=Some(materialize)))).runAsync
    fi.futureValue.headers.statusCode should equal(Status.CREATED)

    val fi2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index2"),
      Seq(HyperStorageIndexSortItem("a", order = None, fieldType = None)),
      Some("b > 10"),
      materialize=Some(materialize)))).runAsync
    fi2.futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexDefUp2 = db.selectIndexDef("collection-1~", "index2").runAsync.futureValue
      indexDefUp2 shouldBe defined
      indexDefUp2.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.revision shouldBe 1
      indexContent.head.count shouldBe Some(1l)

      if (materialize)
        indexContent.head.body.get should include("\"item1\"")
      else
        indexContent.head.body shouldBe None
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
        "t0", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.revision shouldBe 1
      indexContent.head.count shouldBe Some(1l)

      if (materialize)
        indexContent.head.body.get should include("\"item1\"")
      else
        indexContent.head.body shouldBe None
    }

    val c2 = Obj.from("a" → "goodbye")
    val c2x = c2 + Obj.from("id" → "item1")
    val f2 = hyperbus.ask(ContentPatch("collection-1~/item1", DynamicBody(c2))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.OK)

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.revision shouldBe 2
      indexContent.head.count shouldBe Some(1l)

      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.body.get should include("\"goodbye\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
        "t0", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.revision shouldBe 2
      indexContent.head.count shouldBe Some(1l)
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.body.get should include("\"goodbye\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    val c3 = Obj.from("b" → 5)
    val f3 = hyperbus.ask(ContentPatch("collection-1~/item1", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode should equal(Status.OK)

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent shouldBe empty
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index1", Seq(FieldFilter(
        "t0", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent shouldBe empty
    }

    val f4 = hyperbus.ask(ContentGet(
      path = "collection-1~",
      filter = Some("b > 10"),
      perPage = Some(50)
    )).runAsync
    val rc4 = f4.futureValue
    rc4.body.content shouldBe Lst.empty

    val f5 = hyperbus.ask(ContentGet(
      path = "collection-1~",
      filter = Some("b > 10"),
      sortBy = Some("a"),
      perPage = Some(50)
    )).runAsync
    val rc5 = f5.futureValue
    rc5.body.content shouldBe Lst.empty
  }

  it should "update count when patching removes then adds item to index" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → 10)
    val c1x = c1 + Obj.from("id" → "item1")
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, Some("a > 5"),materialize=Some(materialize)))).runAsync
    fi.futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.revision shouldBe 1
      indexContent.head.count shouldBe Some(1l)

      if (materialize)
        indexContent.head.body.get should include("\"item1\"")
      else
        indexContent.head.body shouldBe None
    }

    val c2 = Obj.from("a" → 1)
    val c2x = c2 + Obj.from("id" → "item1")
    val f2 = hyperbus.ask(ContentPatch("collection-1~/item1", DynamicBody(c2))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.OK)

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent shouldBe empty
    }

    val c3 = Obj.from("a" → 10)
    val f3 = hyperbus.ask(ContentPatch("collection-1~/item1", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode should equal(Status.OK)

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.revision shouldBe 3
      indexContent.head.count shouldBe Some(1l)

      if (materialize)
        indexContent.head.body.get should include("\"item1\"")
      else
        indexContent.head.body shouldBe None
    }
  }

  it should "Putting over existing item should update index" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val c1x = c1 + Obj.from("id" → "item1")
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10"),materialize=Some(materialize)))).runAsync
    fi.futureValue.statusCode should equal(Status.CREATED)

    val fi2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index2"),
      Seq(HyperStorageIndexSortItem("a", order = None, fieldType = None)),
      Some("b > 10"),
      materialize=Some(materialize)))).runAsync
    fi2.futureValue.statusCode should equal(Status.CREATED)

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index2").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.count shouldBe Some(1l)
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
        "t0", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.count shouldBe Some(1l)
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    val c2 = Obj.from("a" → "goodbye", "b" → 30)
    val c2x = c2 + Obj.from("id" → "item1")
    val f2 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c2))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.OK)

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.revision shouldBe 2
      indexContent.head.count shouldBe Some(1l)
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.body.get should include("\"goodbye\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
        "t0", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.revision shouldBe 2
      indexContent.head.count shouldBe Some(1l)
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
        indexContent.head.body.get should include("\"goodbye\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    val c3 = Obj.from("a" → "hello", "b" → 5)
    val f3 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode should equal(Status.OK)

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq(FieldFilter(
        "item_id", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent shouldBe empty
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter(
        "t0", "", FilterGt
      )), Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent shouldBe empty
    }
  }

  it should "Create index with filter and decimal sorting" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"),
      Seq(HyperStorageIndexSortItem("b", order = Some("asc"), fieldType = Some("decimal"))), Some("b > 10"),materialize=Some(materialize)))).runAsync
    fi.futureValue.statusCode should equal(Status.CREATED)

    val indexDef = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
    indexDef shouldBe defined
    indexDef.get.documentUri shouldBe "collection-1~"
    indexDef.get.indexId shouldBe "index1"
    indexDef.get.materialize shouldBe materialize

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_da0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.count shouldBe Some(1l)
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }

    }

    val c2 = Obj.from("a" → "goodbye", "b" → 1)
    val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.CREATED)

    val c3 = Obj.from("a" → "way way", "b" → 12)
    val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexContent = db.selectIndexCollection("index_content_da0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 2
      indexContent(0).documentUri shouldBe "collection-1~"
      indexContent(0).itemId shouldBe "item3"
      indexContent(0).count shouldBe Some(2l)
      if (materialize) {
        indexContent(0).body.get should include("\"item3\"")
      }
      else {
        indexContent(0).body shouldBe None
      }

      indexContent(1).documentUri shouldBe "collection-1~"
      indexContent(1).itemId shouldBe "item1"
      if (materialize) {
        indexContent(1).body.get should include("\"item1\"")
      }
      else {
        indexContent(1).body shouldBe None
      }
    }
  }

  it should "Create index with filter and decimal desc sorting" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"),
      Seq(HyperStorageIndexSortItem("b", order = Some("desc"), fieldType = Some("decimal"))), Some("b > 10"),
      materialize=Some(materialize)))).runAsync
    fi.futureValue.statusCode should equal(Status.CREATED)

    val indexDef = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
    indexDef shouldBe defined
    indexDef.get.documentUri shouldBe "collection-1~"
    indexDef.get.indexId shouldBe "index1"
    indexDef.get.materialize shouldBe materialize

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_dd0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    val c2 = Obj.from("a" → "goodbye", "b" → 1)
    val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.CREATED)

    val c3 = Obj.from("a" → "way way", "b" → 12)
    val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexContent = db.selectIndexCollection("index_content_dd0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 2
      indexContent(0).documentUri shouldBe "collection-1~"
      indexContent(0).itemId shouldBe "item1"
      if (materialize) {
        indexContent(0).body.get should include("\"item1\"")
      }
      else {
        indexContent(0).body shouldBe None
      }

      indexContent(1).documentUri shouldBe "collection-1~"
      indexContent(1).itemId shouldBe "item3"
      if (materialize) {
        indexContent(1).body.get should include("\"item3\"")
      }
      else {
        indexContent(1).body shouldBe None
      }
    }
  }

  it should "Create index with filter and text sorting" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"),
      Seq(HyperStorageIndexSortItem("a", order = Some("asc"), fieldType = Some("text"))), Some("b > 10"),
      materialize=Some(materialize)))).runAsync
    fi.futureValue.statusCode should equal(Status.CREATED)

    val indexDef = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
    indexDef shouldBe defined
    indexDef.get.documentUri shouldBe "collection-1~"
    indexDef.get.indexId shouldBe "index1"
    indexDef.get.materialize shouldBe materialize

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    val c2 = Obj.from("a" → "goodbye", "b" → 1)
    val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.CREATED)

    val c3 = Obj.from("a" → "way way", "b" → 12)
    val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      //println(indexContent)
      indexContent.size shouldBe 2
      indexContent(0).documentUri shouldBe "collection-1~"
      indexContent(0).itemId shouldBe "item1"
      if (materialize) {
        indexContent(0).body.get should include("\"item1\"")
      }
      else {
        indexContent(0).body shouldBe None
      }

      indexContent(1).documentUri shouldBe "collection-1~"
      indexContent(1).itemId shouldBe "item3"
      if (materialize) {
        indexContent(1).body.get should include("\"item3\"")
      }
      else {
        indexContent(1).body shouldBe None
      }
    }
  }

  it should "Create index with filter and text desc sorting" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val path = "collection-1~"
    val fi = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"),
      Seq(HyperStorageIndexSortItem("a", order = Some("desc"), fieldType = Some("text"))), Some("b > 10"),
      materialize=Some(materialize)
    ))).runAsync
    fi.futureValue.statusCode should equal(Status.CREATED)

    val indexDef = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
    indexDef shouldBe defined
    indexDef.get.documentUri shouldBe "collection-1~"
    indexDef.get.indexId shouldBe "index1"
    indexDef.get.materialize shouldBe materialize

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
    }

    eventually {
      val indexContent = db.selectIndexCollection("index_content_td0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    val c2 = Obj.from("a" → "goodbye", "b" → 1)
    val f2 = hyperbus.ask(ContentPut("collection-1~/item2", DynamicBody(c2))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.CREATED)

    val c3 = Obj.from("a" → "way way", "b" → 12)
    val f3 = hyperbus.ask(ContentPut("collection-1~/item3", DynamicBody(c3))).runAsync
    f3.futureValue.headers.statusCode should equal(Status.CREATED)

    eventually {
      val indexContent = db.selectIndexCollection("index_content_td0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 2
      indexContent(0).documentUri shouldBe "collection-1~"
      indexContent(0).itemId shouldBe "item3"
      if (materialize) {
        indexContent(0).body.get should include("\"item3\"")
      }
      else {
        indexContent(0).body shouldBe None
      }

      indexContent(1).documentUri shouldBe "collection-1~"
      indexContent(1).itemId shouldBe "item1"
      if (materialize) {
        indexContent(1).body.get should include("\"item1\"")
      }
      else {
        indexContent(1).body shouldBe None
      }
    }
  }

  it should "Create and delete index" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val path = "collection-1~"
    val f2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None, materialize=Some(materialize)))).runAsync
    f2.futureValue.statusCode should equal(Status.CREATED)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val indexDef = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
    indexDef shouldBe defined
    indexDef.get.documentUri shouldBe "collection-1~"
    indexDef.get.indexId shouldBe "index1"
    indexDef.get.materialize shouldBe materialize

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.count shouldBe Some(1l)
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    val f3 = hyperbus.ask(IndexDelete(path, "index1")).runAsync
    f3.futureValue.headers.statusCode should equal(Status.NO_CONTENT)

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe None
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent shouldBe empty
    }
  }

  it should "Collection removal should remove index" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val path = "collection-1~"
    val f2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None, materialize=Some(materialize)))).runAsync
    f2.futureValue.statusCode should equal(Status.CREATED)

    val c1 = Obj.from("a" → "hello", "b" → 100500)
    val f1 = hyperbus.ask(ContentPut("collection-1~/item1", DynamicBody(c1))).runAsync
    f1.futureValue.headers.statusCode should equal(Status.CREATED)

    val indexDef = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
    indexDef shouldBe defined
    indexDef.get.documentUri shouldBe "collection-1~"
    indexDef.get.indexId shouldBe "index1"
    indexDef.get.materialize shouldBe materialize

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe defined
      indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "collection-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.count shouldBe Some(1l)

      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    val f3 = hyperbus.ask(ContentDelete(path)).runAsync
    f3.futureValue.statusCode should equal(Status.OK)

    eventually {
      val indexDefUp = db.selectIndexDef("collection-1~", "index1").runAsync.futureValue
      indexDefUp shouldBe None
      val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent shouldBe empty
    }
  }

  it should "create index by template" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val c1 = Obj.from("a" → "hello", "b" → 100500)

    val tf = hyperbus.ask(TemplateIndexPut("t1", HyperStorageTemplateIndex("a-1~", Seq.empty, None, materialize=Some(materialize)))).runAsync
    tf.futureValue.statusCode should equal(Status.CREATED)

    val fa1 = hyperbus.ask(ContentPut("a-1~/item1", DynamicBody(c1))).runAsync
    fa1.futureValue.headers.statusCode should equal(Status.CREATED)

//    val fb1 = hyperbus.ask(ContentPut("b-1~/item1", DynamicBody(c1))).runAsync
//    fb1.futureValue.headers.statusCode should equal(Status.CREATED)


    val templateIndexDefs = db.selectTemplateIndexDefs("*").runAsync.futureValue.toList
    templateIndexDefs.size shouldBe 1

    templateIndexDefs.head.templateUri shouldBe "a-1~"
    templateIndexDefs.head.indexId shouldBe "t1"
    templateIndexDefs.head.materialize shouldBe materialize

    eventually {
      val indexDef = db.selectIndexDef("a-1~", "t1").runAsync.futureValue
      indexDef shouldBe defined
      indexDef.get.documentUri shouldBe "a-1~"
      indexDef.get.indexId shouldBe "t1"
      indexDef.get.materialize shouldBe materialize
      indexDef.get.status shouldBe IndexDef.STATUS_NORMAL

      val indexContent = db.selectIndexCollection("index_content", "a-1~", "t1", Seq.empty, Seq.empty, 10).runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
      indexContent.head.documentUri shouldBe "a-1~"
      indexContent.head.itemId shouldBe "item1"
      indexContent.head.count shouldBe Some(1l)
      if (materialize) {
        indexContent.head.body.get should include("\"item1\"")
      }
      else {
        indexContent.head.body shouldBe None
      }
    }

    db.selectIndexDef("b-1~", "t1").runAsync.futureValue.toList shouldBe empty
  }
}

class IndexingSpecNonMaterialized extends IndexingSpecMaterialized {
  override def materialize: Boolean = false
}
