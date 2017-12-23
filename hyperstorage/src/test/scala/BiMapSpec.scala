/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.hyperstorage.utils.BiMap
import org.scalatest.{FlatSpec, Matchers}

class BiMapSpec extends FlatSpec with Matchers {
  "BiMap" should "map in both ways" in {
    val m = BiMap("a" → 1, "b" → 2)
    m("a") shouldBe 1
    m("b") shouldBe 2
    m.inverse(1) shouldBe "a"
    m.inverse(2) shouldBe "b"
  }

  it should "remove in both" in {
    val m = BiMap("a" → 1, "b" → 2)
    (m - "b").get("b") shouldBe None
  }

  it should "add both" in {
    val m = BiMap("a" → 1, "b" → 2)
    val m2 = m + ("c" → 3)
    m2("c") shouldBe 3
  }
}
