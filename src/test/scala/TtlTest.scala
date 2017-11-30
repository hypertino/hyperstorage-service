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
import com.hypertino.hyperstorage.db.{FieldFilter, FilterGt}
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

class TtlTest extends FlatSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(15, Seconds)))
  implicit val emptyContext = MessagingContext.empty

  "ttl" should "work on documents" in {
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val docCreated = hyperbus.ask(ContentPut("abc", DynamicBody(Obj.from("a" → 10)),
      headers = Headers(HyperStorageHeader.HYPER_STORAGE_TTL → 3)))
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
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    hyperbus.ask(ViewPut("abcs~", HyperStorageView("abc/{*}")))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    eventually {
      val h = db.selectViewDefs().runAsync.futureValue.toSeq.head
      h.documentUri shouldBe "abcs~"
      h.templateUri shouldBe "abc/{*}"
    }

    val docCreated = hyperbus.ask(ContentPut("abc/1", DynamicBody(Obj.from("a" → 10)),
      headers = Headers(HyperStorageHeader.HYPER_STORAGE_TTL → 3)))
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
    cleanUpCassandra()
    val hyperbus = integratedHyperbus(db)

    val path = "abc~"
    val f2 = hyperbus.ask(IndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None, materialize=Some(true)))).runAsync
    f2.futureValue.headers.statusCode should equal(Status.CREATED)

    val itemCreated = hyperbus.ask(ContentPut("abc~/1", DynamicBody(Obj.from("a" → 10)),
      headers=Headers(HyperStorageHeader.HYPER_STORAGE_TTL → 3)))
      .runAsync
      .futureValue
    itemCreated shouldBe a[Created[_]]
    val itemCreated2 = hyperbus.ask(ContentPut("abc~/2", DynamicBody(Obj.from("a" → 10))))
      .runAsync
      .futureValue
    itemCreated2 shouldBe a[Created[_]]

    eventually {
      val indexContent = db.selectIndexCollection("index_content", "abc~", "index1", Seq(FieldFilter("item_id","",FilterGt)), Seq.empty, 10)
        .runAsync.futureValue.toSeq
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
      val indexContent = db.selectIndexCollection("index_content", "abc~", "index1", Seq(FieldFilter("item_id","",FilterGt)), Seq.empty, 10)
        .runAsync.futureValue.toSeq
      indexContent.size shouldBe 1
    }
  }
}

