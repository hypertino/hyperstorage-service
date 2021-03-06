/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.binders.value.{Number, Obj}
import com.hypertino.hyperstorage.api.{HyperStorageIndexSortFieldType, HyperStorageIndexSortItem, HyperStorageIndexSortOrder}
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.{IndexLogic, OrderFieldsLogic}
import com.hypertino.hyperstorage.utils.SortBy
import org.scalatest.{FreeSpec, Matchers}



class OrderFieldsLogicTest extends FreeSpec with Matchers {
  "OrderFieldsLogic" - {
    "weighOrdering" - {

      "equal orders should be 10" in {
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a", None, None))) shouldBe 10
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 10
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, None), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 10
      }

      "empty query orders should be 0" in {
        OrderFieldsLogic.weighOrdering(Seq.empty, Seq(HyperStorageIndexSortItem("a", None, None))) shouldBe 0
      }

      "reverse index order should be 5" in {
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a", None, None))) shouldBe 5
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 5
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a", None, None), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 5
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a")), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 5
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 5
      }

      "partially equal order should be 3" in {
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a", None, None), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 3
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 3
        OrderFieldsLogic.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 3
      }

      "unequal order should be -10" in {
        OrderFieldsLogic.weighOrdering(Seq(SortBy("z", descending = false)), Seq(HyperStorageIndexSortItem("a", None, None))) shouldBe -10
        OrderFieldsLogic.weighOrdering(Seq(SortBy("x", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a", None, None), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe -10
        OrderFieldsLogic.weighOrdering(Seq(SortBy("y"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe -10
      }
    }

    "extractIndexSortFields" - {
      "equal orders should be extracted totally" in {
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a", None, None))) shouldBe (Seq(CkField("t0", ascending = true)), false)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe (Seq(CkField("t0", ascending = true)), false)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, None), HyperStorageIndexSortItem("b", Some(HyperStorageIndexSortFieldType.DECIMAL), Some(HyperStorageIndexSortOrder.DESC)))) shouldBe (Seq(CkField("t0", ascending = true), CkField("d1", ascending = false)), false)
      }

      "empty query orders should be Seq.empty" in {
        OrderFieldsLogic.extractIndexSortFields("id", Seq.empty, Seq(HyperStorageIndexSortItem("a", None, None))) shouldBe (Seq.empty, false)
      }

      "reverse index order be extracted totally" in {
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a", None, None))) shouldBe (Seq(CkField("t0", ascending = false)), true)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe (Seq(CkField("t0", ascending = false)), true)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a", None, None), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe (Seq(CkField("t0", ascending = false), CkField("t1", ascending = true)), true)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a")), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe (Seq(CkField("t0", ascending = true)), true)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe (Seq(CkField("t0", ascending = true), CkField("t1", ascending = false)), true)
      }

      "partially equal order should be extracted partially" in {
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a", None, None), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe (Seq(CkField("t0", ascending = false)),true)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe (Seq(CkField("t0", ascending = true)), true)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe (Seq(CkField("t0", ascending = true)), true)
      }

      "unequal order should extract Seq.empty" in {
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("z", descending = false)), Seq(HyperStorageIndexSortItem("a", None, None))) shouldBe (Seq.empty, false)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("x", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a", None, None), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.ASC)))) shouldBe (Seq.empty, false)
        OrderFieldsLogic.extractIndexSortFields("id", Seq(SortBy("y"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a", None, Some(HyperStorageIndexSortOrder.DESC)), HyperStorageIndexSortItem("b", None, Some(HyperStorageIndexSortOrder.DESC)))) shouldBe (Seq.empty, false)
      }
    }

    "leastRowsFilterFields" - {

      /*a,b,c,d
      * 5,1,2,2 -> a=5 & b=1 & c=2 & d > 2
      * 5,1,2,3 (a=5 & b=1 & c=2 & d > 2) -> a=5 & b=1 & c>2
      * 5,1,3,0
      * 5,1,3,1 (a=5 & b=1 & c>2) -> a=5 & b>1
      * 5,2,4,4
      * 5,2,4,5
      * */

      "Simple least rows filter" in {
        val indexSortedBy = Seq(
          HyperStorageIndexSortItem("a", None, None),
          HyperStorageIndexSortItem("b", None, None),
          HyperStorageIndexSortItem("c", None, None),
          HyperStorageIndexSortItem("d", None, None)
        )
        val filterFields = Seq.empty[FieldFilter]
        val previousValue = None
        val currentValue = Obj.from("a" → 5, "b" → 1, "c" → 2, "d" → 2)
        val res = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 0, false, currentValue, false)
        res shouldBe Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(1), FilterEq),
          FieldFilter("t2", Number(2), FilterEq),
          FieldFilter("t3", Number(2), FilterGt)
        )
      }

      "Simple least rows filter (reverse order)" in {
        val indexSortedBy = Seq(
          HyperStorageIndexSortItem("a", None, None),
          HyperStorageIndexSortItem("b", None, None),
          HyperStorageIndexSortItem("c", None, None),
          HyperStorageIndexSortItem("d", None, None)
        )
        val filterFields = Seq.empty[FieldFilter]
        val previousValue = None
        val currentValue = Obj.from("a" → 5, "b" → 1, "c" → 2, "d" → 2)
        val res = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 0, false, currentValue, true)
        res shouldBe Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(1), FilterEq),
          FieldFilter("t2", Number(2), FilterEq),
          FieldFilter("t3", Number(2), FilterLt)
        )
      }

      "Least rows filter with existing filter" in {
        val indexSortedBy = Seq(
          HyperStorageIndexSortItem("a", None, None),
          HyperStorageIndexSortItem("b", None, None),
          HyperStorageIndexSortItem("c", None, None),
          HyperStorageIndexSortItem("d", None, None)
        )
        val filterFields = Seq(
          FieldFilter("t0", Number(18), FilterEq)
        )
        val previousValue = None
        val currentValue = Obj.from("a" → 5, "b" → 1, "c" → 2, "d" → 2)
        val res = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 0, false, currentValue, false)
        res shouldBe Seq(
          FieldFilter("t1", Number(1), FilterEq),
          FieldFilter("t2", Number(2), FilterEq),
          FieldFilter("t3", Number(2), FilterGt)
        )
      }

      "Least rows filter with existing+previous filters" in {
        val indexSortedBy = Seq(
          HyperStorageIndexSortItem("a", None, None),
          HyperStorageIndexSortItem("b", None, None),
          HyperStorageIndexSortItem("c", None, None),
          HyperStorageIndexSortItem("d", None, None)
        )
        val filterFields = Seq(
          FieldFilter("t0", Number(18), FilterEq)
        )
        val previousValue = None
        val currentValue = Obj.from("a" → 18, "b" → 1, "c" → 2, "d" → 2)
        val res = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 4, true, currentValue, false)
        res shouldBe Seq(
          FieldFilter("t1", Number(1), FilterEq),
          FieldFilter("t2", Number(2), FilterGt)
        )
      }

      "Least rows filter with existing+previous filters (not reached end)" in {
        val indexSortedBy = Seq(
          HyperStorageIndexSortItem("a", None, None),
          HyperStorageIndexSortItem("b", None, None),
          HyperStorageIndexSortItem("c", None, None),
          HyperStorageIndexSortItem("d", None, None)
        )
        val filterFields = Seq(
          FieldFilter("t0", Number(18), FilterEq)
        )
        val previousValue = None
        val currentValue = Obj.from("a" → 18, "b" → 1, "c" → 2, "d" → 2)
        val res = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 4, false, currentValue, false)
        res shouldBe Seq(
          FieldFilter("t1", Number(1), FilterEq),
          FieldFilter("t2", Number(2), FilterEq),
          FieldFilter("t3", Number(2), FilterGt)
        )
      }

      "Least rows filter with existing filter(gt)" in {
        val indexSortedBy = Seq(
          HyperStorageIndexSortItem("a", None, None),
          HyperStorageIndexSortItem("b", None, None),
          HyperStorageIndexSortItem("c", None, None),
          HyperStorageIndexSortItem("d", None, None)
        )
        val filterFields = Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(0), FilterGt)
        )
        val previousValue = None
        val currentValue = Obj.from("a" → 5, "b" → 1, "c" → 2, "d" → 2)
        val res = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 0, false, currentValue, false)
        res shouldBe Seq(
          FieldFilter("t1", Number(1), FilterEq),
          FieldFilter("t2", Number(2), FilterEq),
          FieldFilter("t3", Number(2), FilterGt)
        )

        IndexLogic.mergeLeastQueryFilterFields(filterFields, res) shouldBe Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(1), FilterEq),
          FieldFilter("t2", Number(2), FilterEq),
          FieldFilter("t3", Number(2), FilterGt)
        )
      }

      "Least rows filter with existing filter(gt) + previous" in {
        val indexSortedBy = Seq(
          HyperStorageIndexSortItem("a", None, None),
          HyperStorageIndexSortItem("b", None, None),
          HyperStorageIndexSortItem("c", None, None),
          HyperStorageIndexSortItem("d", None, None)
        )
        val filterFields = Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(0), FilterGt)
        )
        val previousValue = None
        val currentValue = Obj.from("a" → 5, "b" → 1, "c" → 2, "d" → 2)
        val res = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 4, true, currentValue, false)
        res shouldBe Seq(
          FieldFilter("t1", Number(1), FilterEq),
          FieldFilter("t2", Number(2), FilterGt)
        )

        IndexLogic.mergeLeastQueryFilterFields(filterFields, res) shouldBe Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(1), FilterEq),
          FieldFilter("t2", Number(2), FilterGt)
        )
      }

      "Least rows filter with existing filter(gt) + previous reversed" in {
        val indexSortedBy = Seq(
          HyperStorageIndexSortItem("a", None, None),
          HyperStorageIndexSortItem("b", None, None),
          HyperStorageIndexSortItem("c", None, None),
          HyperStorageIndexSortItem("d", None, None)
        )
        val filterFields = Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(3), FilterLt)
        )
        val previousValue = None
        val currentValue = Obj.from("a" → 5, "b" → 2, "c" → 2, "d" → 2)
        val res = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 4, true, currentValue, true)
        res shouldBe Seq(
          FieldFilter("t1", Number(2), FilterEq),
          FieldFilter("t2", Number(2), FilterLt)
        )

        IndexLogic.mergeLeastQueryFilterFields(filterFields, res) shouldBe Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(2), FilterEq),
          FieldFilter("t2", Number(2), FilterLt)
        )
      }

      "Least rows filter with existing filter(gt) should-be empty for non-range matching value" in {
        val indexSortedBy = Seq(
          HyperStorageIndexSortItem("a", None, None),
          HyperStorageIndexSortItem("b", None, None),
          HyperStorageIndexSortItem("c", None, None),
          HyperStorageIndexSortItem("d", None, None)
        )
        val filterFields = Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(1), FilterLt)
        )
        val currentValue = Obj.from("a" → 5, "b" → 2, "c" → 1, "d" → 1)
        val res = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 0, false, currentValue, false)
        res shouldBe Seq.empty

        val filterFields2 = Seq(
          FieldFilter("t0", Number(5), FilterEq),
          FieldFilter("t1", Number(3), FilterLt)
        )
        val currentValue2 = Obj.from("a" → 5, "b" → 2, "c" → 1, "d" → 1)
        val res2 = IndexLogic.leastRowsFilterFields("id", indexSortedBy, filterFields, 0, false, currentValue, true)
        res2 shouldBe Seq.empty
      }
    }
  }
}
