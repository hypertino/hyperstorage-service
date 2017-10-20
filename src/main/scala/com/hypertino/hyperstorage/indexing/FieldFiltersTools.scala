package com.hypertino.hyperstorage.indexing

import com.hypertino.parser.HParser
import com.hypertino.parser.ast.Identifier

trait FieldFiltersTools {
  protected final val compareOps = Set(">", ">=", "<", "<=", "=").map(Identifier(_))
  protected final val andOp = parseIdentifier("and")

  protected def parseIdentifier(s: String): Identifier = {
    new HParser(s).Ident.run().get
  }
}
