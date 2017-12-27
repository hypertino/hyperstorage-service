package com.hypertino.mock.hyperstorage

import com.hypertino.binders.value.{Lst, Obj, Text}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{Created, DynamicBody, MessagingContext, NotFound, Ok}
import com.hypertino.hyperbus.transport.api.{NoTransportRouteException, ServiceRegistrator}
import com.hypertino.hyperbus.transport.registrators.DummyRegistrator
import com.hypertino.hyperstorage.api._
import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.Scheduler
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import scaldi.Module

class HyperStorageMockSpec extends FlatSpec
                              with Matchers
                              with ScalaFutures
                              with Eventually
                              with BeforeAndAfterEach
                              with Module {
  implicit val patience: PatienceConfig = PatienceConfig(scaled(Span(20, Seconds)))

  protected implicit val _scheduler: Scheduler = monix.execution.Scheduler.Implicits.global
  protected implicit val mc: AnyRef with MessagingContext = MessagingContext.empty
  bind [Config] to ConfigFactory.load("hyperstorage-mock")
  bind [Scheduler] to _scheduler
  bind [Hyperbus] to injected[Hyperbus]
  bind [ServiceRegistrator] to DummyRegistrator

  val hyperbus: Hyperbus = inject[Hyperbus]
  val hyperStorageMock = new HyperStorageMock(hyperbus, _scheduler)

  override def afterEach(): Unit = {
    hyperStorageMock.reset()
  }

  "HyperStorageMock - documents" should "create document" in {
    val postResponse = hyperbus.ask(ContentPost("entry-x", DynamicBody(Obj.from("key" -> "val"))))
      .runAsync
      .futureValue

    postResponse shouldBe a[Created[_]]

    val id = postResponse.body.target.toMap("entry_x_id").toString()

    postResponse.body.path shouldBe s"entry-x/$id"

    val getResponse = hyperbus.ask(ContentGet(s"entry-x/$id"))
      .runAsync
      .futureValue

    getResponse shouldBe a[Ok[_]]

    getResponse.body.content shouldBe Obj.from("entry_x_id" -> id, "key" -> "val")
  }

  it should "insert document" in {
    val newEntry = Obj.from("key" -> "val")

    hyperbus.ask(ContentPut("entry-x/new-id", DynamicBody(newEntry)))
      .runAsync
      .futureValue shouldBe a[Created[_]]

    hyperbus.ask(ContentGet("entry-x/new-id"))
      .map(_.body.content)
      .runAsync
      .futureValue shouldBe newEntry
  }

  it should "update created document" in {
    val postResponse = hyperbus.ask(ContentPost("entry-x", DynamicBody(Obj.from("key" -> "val1"))))
      .runAsync
      .futureValue

    postResponse shouldBe a[Created[_]]

    val entryX = hyperbus.ask(ContentGet(postResponse.body.path))
      .map(_.body.content)
      .runAsync
      .futureValue

    val newEntryX = entryX % Obj.from("key" -> "val2")

    hyperbus.ask(ContentPut(postResponse.body.path, DynamicBody(newEntryX)))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    hyperbus.ask(ContentGet(s"entry-x/${entryX.toMap("entry_x_id")}"))
      .map(_.body.content)
      .runAsync
      .futureValue shouldBe newEntryX
  }

  it should "patch created document" in {
    val postResponse = hyperbus.ask(ContentPost("entry-x", DynamicBody(Obj.from("key1" -> "val1"))))
      .runAsync
      .futureValue

    postResponse shouldBe a[Created[_]]

    hyperbus.ask(ContentPatch(postResponse.body.path, DynamicBody(Obj.from("key2" -> "val2", "key1" -> "val1new"))))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    val getResponse = hyperbus.ask(ContentGet(postResponse.body.path))
      .map(_.body.content)
      .runAsync
      .futureValue

    getResponse shouldBe Obj.from(
      "key2" -> "val2",
      "entry_x_id" -> postResponse.body.target.toMap("entry_x_id"),
      "key1" -> "val1new")
  }

  it should "delete created document" in {
    val postResponse = hyperbus.ask(ContentPost("entry-x", DynamicBody(Obj.from("key1" -> "val1"))))
      .runAsync
      .futureValue

    postResponse shouldBe a[Created[_]]

    hyperbus.ask(ContentDelete(postResponse.body.path))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    hyperbus.ask(ContentGet(postResponse.body.path))
      .failed
      .runAsync
      .futureValue shouldBe a[NotFound[_]]
  }

  "HyperStorageMock - collections" should "create collection item" in {
    val postResponse = hyperbus.ask(ContentPost("entries~", DynamicBody(Obj.from("key" -> "val"))))
      .runAsync
      .futureValue

    postResponse shouldBe a[Created[_]]

    val id = postResponse.body.target.toMap("entry_id").toString()

    postResponse.body.path shouldBe s"entries~/$id"

    val getCollectionResponse = hyperbus.ask(ContentGet("entries~"))
      .runAsync
      .futureValue

    getCollectionResponse shouldBe a[Ok[_]]

    getCollectionResponse.body.content shouldBe Lst.from(Obj.from("entry_id" -> id, "key" -> "val"))

    val getCollectionItemResponse = hyperbus.ask(ContentGet(postResponse.body.path))
      .runAsync
      .futureValue

    getCollectionItemResponse shouldBe a[Ok[_]]

    getCollectionItemResponse.body.content shouldBe Obj.from("entry_id" -> id, "key" -> "val")
  }

  it should "update created collection item" in {
    val postResponse = hyperbus.ask(ContentPost("entries~", DynamicBody(Obj.from("key" -> "val"))))
      .runAsync
      .futureValue

    postResponse shouldBe a[Created[_]]

    val entry = hyperbus.ask(ContentGet(postResponse.body.path))
      .map(_.body.content)
      .runAsync
      .futureValue

    val newEntry = entry % Obj.from("key" -> "newVal", "new_key" -> "newValToo")

    hyperbus.ask(ContentPut(postResponse.body.path, DynamicBody(newEntry)))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    hyperbus.ask(ContentGet(postResponse.body.path))
        .map(_.body.content)
        .runAsync
        .futureValue shouldBe newEntry
  }

  it should "patch created collection item" in {
    val postResponse = hyperbus.ask(ContentPost("entries~", DynamicBody(Obj.from("key" -> "val"))))
      .runAsync
      .futureValue

    postResponse shouldBe a[Created[_]]

    val id = postResponse.body.target.toMap("entry_id").toString()

    hyperbus.ask(ContentPatch(postResponse.body.path, DynamicBody(Obj.from("key" -> "newVal", "new_key" -> "newValToo"))))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    hyperbus.ask(ContentGet(postResponse.body.path))
      .map(_.body.content)
      .runAsync
      .futureValue shouldBe Obj.from("key" -> "newVal", "entry_id" -> id, "new_key" -> "newValToo")
  }

  it should "delete created collection item" in {
    val postResponse = hyperbus.ask(ContentPost("entries~", DynamicBody(Obj.from("key" -> "val"))))
      .runAsync
      .futureValue

    postResponse shouldBe a[Created[_]]

    hyperbus.ask(ContentDelete(postResponse.body.path))
      .runAsync
      .futureValue shouldBe a[Ok[_]]

    hyperbus.ask(ContentGet(postResponse.body.path))
      .failed
      .runAsync
      .futureValue shouldBe a[NotFound[_]]
  }
}
