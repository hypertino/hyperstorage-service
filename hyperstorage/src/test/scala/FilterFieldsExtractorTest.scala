/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.binders.value.{Number, Text}
import com.hypertino.hyperstorage.api.{HyperStorageIndexSortFieldType, HyperStorageIndexSortItem, HyperStorageIndexSortOrder}
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.FieldFiltersExtractor
import com.hypertino.parser.HParser
import org.scalatest.{FlatSpec, FreeSpec, Matchers}



class FilterFieldsExtractorTest extends FlatSpec with Matchers {
  "FilterFieldsExtractor" should "single gt filter field" in {
    val expression = HParser(s""" id > "10" """)
    val sortByFields = Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterGt))
  }

  it should "single lt filter field" in {
    val expression = HParser(s""" id < "10" """)
    val sortByFields = Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterLt))
  }

  it should "single gteq filter field" in {
    val expression = HParser(s""" id >= "10" """)
    val sortByFields = Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterGtEq))
  }

  it should "single lteq filter field" in {
    val expression = HParser(s""" id <= "10" """)
    val sortByFields = Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterLtEq))
  }

  it should "single eq filter field" in {
    val expression = HParser(s""" id = "10" """)
    val sortByFields = Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterEq))
  }

  it should "single gt reversed filter field" in {
    val expression = HParser(s""" "10" < id """)
    val sortByFields = Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterGt))
  }

  it should "gt filter field with some other field" in {
    val expression = HParser(s""" id > "10" and x < 5 """)
    val sortByFields = Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterGt))
  }

  it should "eq filter field with some other fields" in {
    val expression = HParser(s""" id = "10" and x < 5 and z*3 > 24""")
    val sortByFields = Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterEq))
  }

  it should "eq filter multiple fields with some other fields" in {
    val expression = HParser(s""" id = "10" and x < 5 and z*3 > 24 and y = 12""")
    val sortByFields = Seq(
      HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)),
      HyperStorageIndexSortItem("x", Some(HyperStorageIndexSortFieldType.DECIMAL), Some(HyperStorageIndexSortOrder.ASC))
    )
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterEq), FieldFilter("d1", Number(5), FilterLt))
  }

  it should "gt filter field with or expression shouldn't match" in {
    val expression = HParser(s""" id > "10" or x < 5 """)
    val sortByFields = Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    val ff = new FieldFiltersExtractor("id", sortByFields).extract(expression)
    ff shouldBe Seq.empty
  }
}
