/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.metrics

import com.hypertino.metrics.loaders.MetricsReporterLoader
import com.hypertino.metrics.{MetricsTracker, ProcessMetrics}
import com.typesafe.scalalogging.StrictLogging
import scaldi.{Injectable, Injector, TypeTagIdentifier}

object MetricsReporter extends Injectable with StrictLogging {
  def startReporter(tracker: MetricsTracker)(implicit injector: Injector): Unit = {
    import scala.reflect.runtime.universe._
    injector.getBinding(List(TypeTagIdentifier(typeOf[MetricsReporterLoader]))) match {
      case Some(_) ⇒
        inject[MetricsReporterLoader].run()
        ProcessMetrics.startReporting(tracker)

      case None ⇒
        logger.warn("Metric reporter is not configured.")
    }
  }
}
