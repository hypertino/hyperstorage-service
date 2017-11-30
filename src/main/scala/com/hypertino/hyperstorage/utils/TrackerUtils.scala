/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.utils

import com.hypertino.metrics.MetricsTracker
import monix.eval.Task

object TrackerUtils {
  private def measureTask[T](name: String, tracker: MetricsTracker, t: Task[T]): Task[T] = {
    Task.eval {
      val timer = tracker.timer(name).time()
      t
        .doOnFinish(_ ⇒ Task.now(timer.stop))
        .doOnCancel(Task.now(timer.stop()))
    }.flatten
  }

  implicit class TrackerUtilsExt(val tracker: MetricsTracker) extends AnyVal {
    def timeOfTask[T](name: String)(t: Task[T]): Task[T] = measureTask(name, tracker, t)
  }
}
