/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.utils

import java.util.concurrent.atomic.AtomicLong

object AkkaNaming {
  private val id = new AtomicLong()

  def next(prefix: String): String = {
    prefix + id.incrementAndGet().toHexString
  }
}
