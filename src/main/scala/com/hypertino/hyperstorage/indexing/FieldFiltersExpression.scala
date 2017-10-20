package com.hypertino.hyperstorage.indexing

import com.hypertino.binders.value.{Text, Value}
import com.hypertino.hyperstorage.db._

object FieldFiltersExpression {
  def translate(name: String, idFieldName: String): String = if (name == "item_id") idFieldName else name
}
