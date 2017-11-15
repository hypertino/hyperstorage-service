/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.indexing

import com.hypertino.binders.value.Value
import com.hypertino.hyperstorage.db._
import com.hypertino.parser.{HEval, HFormatter}
import com.hypertino.parser.ast.{BinaryOperation, Constant, Expression, Identifier}
import scala.collection.mutable

object FieldFilterMerger extends FieldFiltersTools {
  def merge(expression: Option[Expression], extraFilter: Seq[FieldFilter]): Option[Expression] = {
    if (expression.isDefined || extraFilter.nonEmpty) {
      val remainingFilterMap = mutable.Map(extraFilter.groupBy(_.name).toSeq: _*)
      val modifiedExpression = expression.map(_merge(_, remainingFilterMap))

      val opSeq = remainingFilterMap
        .values
        .flatten
        .map { kv ⇒
          BinaryOperation(Identifier(kv.name), Identifier(kv.op.stringOp), Constant(kv.value))
        } ++ modifiedExpression

      Some(opSeq.tail.foldLeft(opSeq.head) { case (all, i) ⇒
        BinaryOperation(all, andOp, i)
      })
    }
    else {
      None
    }
  }

  private def _merge(expression: Expression, remainingFilterMap: mutable.Map[String, Seq[FieldFilter]]): Expression = {
    expression match {
      case BinaryOperation(left: Identifier, op, right) if compareOps.contains(op) && AstComparator.isConstantExpression(right) ⇒
        _merge(left, op.segments.head, right, remainingFilterMap)

      case BinaryOperation(left, op, right: Identifier) if compareOps.contains(op) && AstComparator.isConstantExpression(left) ⇒
        _merge(right, FilterOperator(op.segments.head).swap.stringOp, left, remainingFilterMap)

      case BinaryOperation(left, op, right) if op == andOp ⇒
        val l = _merge(left, remainingFilterMap)
        val r = _merge(right, remainingFilterMap)
        BinaryOperation(l, op, r)

      case _ ⇒ expression
    }
  }

  private def _merge(field: Identifier, op: String, argument: Expression, remainingFilterMap: mutable.Map[String, Seq[FieldFilter]]): Expression = {
    val fieldName = HFormatter(field)
    remainingFilterMap.get(fieldName) match {
      case Some(filters) ⇒
        val v = new HEval().eval(argument)

        filters
          .find(_filterMerges(v,op))
          .map { filter ⇒
            if (filters.tail.isEmpty) {
              remainingFilterMap.remove(fieldName)
            }
            else {
              remainingFilterMap.put(fieldName, filters.filterNot(_ == filter))
            }

            BinaryOperation(Identifier(filter.name), Identifier(op), Constant(filter.value))
          }
          .getOrElse {
            BinaryOperation(field, Identifier(op), argument)
          }

      case None ⇒
        BinaryOperation(field,Identifier(op),argument)
    }
  }

  private def _filterMerges(v: Value, op: String)(ff: FieldFilter): Boolean = {
    if (op == ff.op.stringOp) {
      ff.op match {
        case FilterEq ⇒ true
        case FilterGt ⇒ ff.value > v
        case FilterGtEq ⇒ ff.value >= v
        case FilterLt ⇒ ff.value < v
        case FilterLtEq ⇒ ff.value <= v
      }
    }
    else {
      false
    }
  }
}
