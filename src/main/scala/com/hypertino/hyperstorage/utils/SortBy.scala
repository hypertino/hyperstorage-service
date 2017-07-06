package com.hypertino.hyperstorage.utils

import com.hypertino.binders.value.Text
import com.hypertino.hyperbus.model.{HRL, RequestHeaders}

case class SortBy(fieldName: String, descending: Boolean = false)

object Sort {
  val SORT_FIELD_NAME = "sort"

  def parseQueryParam(param: Option[String]): Seq[SortBy] = param.map { value ⇒
    value.split(',').map(_.trim).flatMap {
      case s if s.startsWith("+") && s.length > 1 ⇒
        Some(SortBy(s.substring(1), descending = false))
      case s if s.startsWith("-") && s.length > 1 ⇒
        Some(SortBy(s.substring(1), descending = true))
      case s if s.nonEmpty ⇒
        Some(SortBy(s, descending = false))
      case _ ⇒
        None
    }
  }.toSeq.flatten

  def generateQueryParam(seq: Seq[SortBy]): String = {
    seq.map { s ⇒
      if (s.descending) "-" + s.fieldName else s.fieldName
    } mkString ","
  }
}
