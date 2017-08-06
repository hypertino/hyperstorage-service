package com.hypertino.hyperstorage.indexing

import com.hypertino.binders.value.{Text, Value}
import com.hypertino.hyperstorage.db._

object FieldFiltersExpression {
  def toExpresion(seq: Seq[FieldFilter]): String = {
    seq.map {
      case FieldFilter(name, v, FilterEq) ⇒ s"${translate(name)} = ${quote(v)}"
      case FieldFilter(name, v, FilterGt) ⇒ s"${translate(name)} > ${quote(v)}"
      case FieldFilter(name, v, FilterGtEq) ⇒ s"${translate(name)} >= ${quote(v)}"
      case FieldFilter(name, v, FilterLt) ⇒ s"${translate(name)} < ${quote(v)}"
      case FieldFilter(name, v, FilterLtEq) ⇒ s"${translate(name)} <= ${quote(v)}"
    } mkString " and "
  }

  private def quote(v: Value): String = v match {
    case Text(s) ⇒ '"' + s + '"'
    case _ ⇒ v.toString
  }

  private def translate(name: String): String = if (name == "item_id") "id" else name
}
