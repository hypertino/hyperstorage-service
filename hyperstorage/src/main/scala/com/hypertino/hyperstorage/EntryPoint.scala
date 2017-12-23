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
import com.hypertino.hyperbus.transport.api.{ServiceRegistrator, ServiceResolver}
import com.hypertino.hyperstorage.modules.{HyperStorageServiceModule, SystemServicesModule}
import com.hypertino.metrics.modules.MetricsModule
import com.hypertino.service.config.ConfigModule
import com.hypertino.service.control.ConsoleModule
import com.hypertino.service.control.api.{Service, ServiceController}
import com.hypertino.transport.registrators.consul.ConsulServiceRegistrator
import com.hypertino.transport.resolvers.consul.ConsulServiceResolver
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import scaldi.{Injectable, Module}

import scala.concurrent.Await
import scala.util.Try
import scala.concurrent.duration._

class EntryPointModule extends Module {
  bind [Service] to inject[HyperStorageService]
  bind [ServiceRegistrator] identifiedBy "consul-registrator" to new ConsulServiceRegistrator(inject[Config].getConfig("service-registrator"))(inject [monix.execution.Scheduler])
  bind [ServiceResolver] identifiedBy "consul-resolver" to new ConsulServiceResolver(inject[Config].getConfig("service-resolver"))(inject [Scheduler])
}

object EntryPoint extends Injectable with StrictLogging {

  def main(args: Array[String]): Unit = {
    implicit val injector =
      new SystemServicesModule ::
      new MetricsModule ::
      new ConsoleModule ::
      new HyperStorageServiceModule ::
      new EntryPointModule ::
      ConfigModule()
    implicit val scheduler = inject[Scheduler]
    inject[Hyperbus].startServices()
    inject[ServiceController].run().andThen {
      case _ ⇒
        val timeout = 10.seconds
        Try{Await.result(inject[Hyperbus].shutdown(timeout).runAsync, timeout + 0.5.seconds)}.recover(logException)
        Try{Await.result(inject[ActorSystem].terminate(), timeout + 0.5.seconds)}.recover(logException)
    }
  }

  private def logException: PartialFunction[Throwable, Unit] = {
    case e: Throwable ⇒ logger.error("Unhandled exception", e)
  }
}
