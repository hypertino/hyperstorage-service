/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

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
