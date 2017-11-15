/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.hyperstorage.indexing.AstComparation._
import com.hypertino.hyperstorage.indexing.AstComparator
import com.hypertino.parser.HParser
import org.scalatest.{FlatSpec, Matchers}

class AstComparationTest extends FlatSpec with Matchers {


  "AstComparation" should "compare equal expressions" in {
    AstComparator.compare(HParser("x + 5"), HParser("x + 5")) shouldBe Equal
    AstComparator.compare(HParser("x > 4"), HParser("x > 4")) shouldBe Equal
  }

  it should "compare non-equal expressions" in {
    AstComparator.compare(HParser("x + y"), HParser("x + 5")) shouldBe NotEqual
    AstComparator.compare(HParser("x > 4"), HParser("x > 3")) shouldBe NotEqual
  }

  it should "return Wider for expressions with `> <`" in {
    AstComparator.compare(HParser("x > 4"), HParser("x > 5")) shouldBe Wider
    AstComparator.compare(HParser("x >= 4"), HParser("x >= 5")) shouldBe Wider
    AstComparator.compare(HParser("x < 3"), HParser("x < 2")) shouldBe Wider
    AstComparator.compare(HParser("x <= 4"), HParser("x <= 1")) shouldBe Wider
    AstComparator.compare(HParser("x*4 <= 4"), HParser("x*4 <= 1")) shouldBe Wider
  }

  it should "return Wider for expressions with `has / not`" in {
    AstComparator.compare(HParser("x has [1,2,3]"), HParser("x has [1,2]")) shouldBe Wider
    AstComparator.compare(HParser("x has not [1,2,3]"), HParser("x has not [1,2,3,4]")) shouldBe Wider
  }

  it should "return Wider for expressions with `or`" in {
    AstComparator.compare(HParser("x > 5 or y < 2"), HParser("x > 5")) shouldBe Wider
    AstComparator.compare(HParser("x has [5,1,3,4] or y < 2"), HParser("x has [5,1,3]")) shouldBe Wider
    AstComparator.compare(HParser("x or y < 2"), HParser("y < 2")) shouldBe Wider
    AstComparator.compare(HParser("x or y"), HParser("y")) shouldBe Wider
  }

  it should "return Wider for expressions with `and`" in {
    AstComparator.compare(HParser("x"), HParser("x and y = 20")) shouldBe Wider
    AstComparator.compare(HParser("x > 5"), HParser("x > 5 and y = 20")) shouldBe Wider
    AstComparator.compare(HParser("x"), HParser("x and y")) shouldBe Wider
    AstComparator.compare(HParser("x"), HParser("x+1 and y")) shouldBe NotEqual
  }
}
