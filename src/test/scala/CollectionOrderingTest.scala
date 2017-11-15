/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.binders.value.{Obj, Text, Value}
import com.hypertino.hyperstorage.CollectionOrdering
import com.hypertino.hyperstorage.utils.SortBy
import org.scalatest.{FlatSpec, FreeSpec, Matchers}


class CollectionOrderingTest extends FlatSpec with Matchers {
  val c1 = Obj.from("a" → "hello", "b" → 100500, "c" → 10)
  val c1x = Obj.from(c1.toMap.toSeq ++ Seq("id" → Text("item1")): _*)
  val c2 = Obj.from("a" → "goodbye", "b" → 1, "c" → 20)
  val c2x = Obj.from(c2.toMap.toSeq ++ Seq("id" → Text("item2")): _*)
  val c3 = Obj.from("a" → "way way", "b" → 12, "c" → 10)
  val c3x = Obj.from(c3.toMap.toSeq ++ Seq("id" → Text("item3")): _*)

  "CollectionOrdering" should "sort" in {
    val list = List(c1x, c2x, c3x)
    val ordering = new CollectionOrdering(Seq(SortBy("a")))
    list.sorted(ordering) shouldBe List(c2x, c1x, c3x)
  }

  it should "sort descending" in {
    val list = List(c1x, c2x, c3x)
    val ordering = new CollectionOrdering(Seq(SortBy("a", descending = true)))
    list.sorted(ordering) shouldBe List(c3x, c1x, c2x)
  }

  it should "sort two fields" in {
    val list = List(c1x, c2x, c3x)
    val ordering = new CollectionOrdering(Seq(SortBy("c"), SortBy("a")))
    list.sorted(ordering) shouldBe List(c1x, c3x, c2x)
  }

  it should "sort descending two fields" in {
    val list: List[Value] = List(c1x, c2x, c3x)
    implicit val ordering = new CollectionOrdering(Seq(SortBy("c", descending = true), SortBy("a")))
    list.sorted shouldBe List(c2x, c1x, c3x)
  }
}
