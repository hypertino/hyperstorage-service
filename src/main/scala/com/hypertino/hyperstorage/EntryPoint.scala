/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage

import akka.actor.ActorSystem
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperstorage.modules.{HyperStorageServiceModule, SystemServicesModule}
import com.hypertino.metrics.modules.MetricsModule
import com.hypertino.service.config.ConfigModule
import com.hypertino.service.control.ConsoleModule
import com.hypertino.service.control.api.{Service, ServiceController}
import monix.execution.Scheduler
import org.slf4j.LoggerFactory
import scaldi.{Injectable, Module}

import scala.concurrent.Await
import scala.util.Try
import scala.util.control.NonFatal
import scala.concurrent.duration._

class MainServiceModule extends Module {
  bind [Service]          identifiedBy 'mainService        to inject[HyperStorageService]
}

object EntryPoint extends Injectable {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    implicit val injector =
      new SystemServicesModule ::
      new MetricsModule ::
      new ConsoleModule ::
      new HyperStorageServiceModule ::
      ConfigModule()
    implicit val scheduler = inject[Scheduler]
    inject[ServiceController].run().andThen {
      case _ ⇒
        val timeout = 10.seconds
        Try{Await.result(inject[Hyperbus].shutdown(timeout).runAsync, timeout + 0.5.seconds)}.recover(logException)
        Try{Await.result(inject[ActorSystem].terminate(), timeout + 0.5.seconds)}.recover(logException)
    }
  }

  private def logException: PartialFunction[Throwable, Unit] = {
    case NonFatal(e) ⇒ log.error("Unhandled exception", e)
  }
}
