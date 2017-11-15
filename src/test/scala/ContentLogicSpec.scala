/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.hyperstorage.{ContentLogic, ResourcePath}
import org.scalatest.{FlatSpec, FreeSpec, Matchers}

class ContentLogicSpec extends FlatSpec with Matchers {
  "ContentLogic.splitPath" should " parse document" in {
    ContentLogic.splitPath("document") should equal(ResourcePath("document", ""))
    ContentLogic.splitPath("some/other/document") should equal(ResourcePath("some/other/document", ""))
  }

  it should "parse collection item" in {
    ContentLogic.splitPath("document~/item") should equal(ResourcePath("document~", "item"))
    ContentLogic.splitPath("some/other/document~/item") should equal(ResourcePath("some/other/document~", "item"))
  }

  it should "fail when invalid chars are used" in {
    intercept[IllegalArgumentException] {
      ContentLogic.splitPath(" ")
    }
    intercept[IllegalArgumentException] {
      ContentLogic.splitPath("\t")
    }
  }

  it should "fail when invalid URI is used" in {
    intercept[IllegalArgumentException] {
      ContentLogic.splitPath("/")
    }
    intercept[IllegalArgumentException] {
      ContentLogic.splitPath("//")
    }
    intercept[IllegalArgumentException] {
      ContentLogic.splitPath("/a")
    }
    intercept[IllegalArgumentException] {
      ContentLogic.splitPath("a/")
    }
    intercept[IllegalArgumentException] {
      ContentLogic.splitPath("a//b")
    }
  }

  it should "generate id field name" in {
    ContentLogic.getIdFieldName("users") shouldBe "user_id"
    ContentLogic.getIdFieldName("users~") shouldBe "user_id"
    ContentLogic.getIdFieldName("some/users") shouldBe "user_id"
    ContentLogic.getIdFieldName("some/users~") shouldBe "user_id"
  }

  it should "match pathAndTemplateToId" in {
    ContentLogic.pathAndTemplateToId("abc/def", "abc/{*}") shouldBe Some("def")
    ContentLogic.pathAndTemplateToId("abc/def", "abc/def") shouldBe Some("")
    ContentLogic.pathAndTemplateToId("abc/123", "abc/def") shouldBe None
    ContentLogic.pathAndTemplateToId("abc/123", "{*}/123") shouldBe Some("abc")
  }
}
