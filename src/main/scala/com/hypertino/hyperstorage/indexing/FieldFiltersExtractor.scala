package com.hypertino.hyperstorage.indexing

import com.hypertino.hyperstorage.api.HyperStorageIndexSortItem
import com.hypertino.hyperstorage.db._
import com.hypertino.parser.ast.{BinaryOperation, Expression, Identifier}
import com.hypertino.parser.{HEval, HParser}

class FieldFiltersExtractor(idFieldName: String, sortByFields: Seq[HyperStorageIndexSortItem]) extends FieldFiltersTools {
  private final val size = sortByFields.size
  private final val sortByFieldsMap = sortByFields.zipWithIndex.map{
    case (s,index) ⇒ (parseIdentifier(s.fieldName), (s,index,
        IndexLogic.tableFieldName(idFieldName, s, size, index)
      ))
  }.toMap

  def extract(expression: Expression): Seq[FieldFilter] = {
    expression match {
      case bop @ BinaryOperation(left: Identifier, op, right) if compareOps.contains(op) && AstComparator.isConstantExpression(right) ⇒
        fieldFilterSeq(left, op.segments.head, right)

      case bop @ BinaryOperation(left, op, right: Identifier) if compareOps.contains(op) && AstComparator.isConstantExpression(left) ⇒
        fieldFilterSeq(right, FilterOperator(op.segments.head).swap.stringOp, left)

      case bop @ BinaryOperation(left, op, right) if op == andOp ⇒
        extract(left) ++ extract(right)

      case _ ⇒ Seq.empty
    }
  }

  private def fieldFilterSeq(varIdent: Identifier, op: String, constExpr: Expression): Seq[FieldFilter] = {
    sortByFieldsMap.get(varIdent).map { sf ⇒
      FieldFilter(
        sf._3,
        new HEval().eval(constExpr),
        FilterOperator(op)
      )
    }.toSeq
  }
}
