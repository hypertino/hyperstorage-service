/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.indexing

import com.hypertino.binders.value.{Text, Value}
import com.hypertino.hyperstorage.db._

object FieldFiltersExpression {
  def translate(name: String, idFieldName: String): String = if (name == "item_id") idFieldName else name
}
