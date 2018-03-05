/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.modules

import com.datastax.driver.core.Session
import com.hypertino.hyperbus.transport.api.{ServiceRegistrator, ServiceResolver}
import com.hypertino.hyperstorage.{CassandraConnector, HyperStorageService}
import com.hypertino.transport.registrators.consul.ConsulServiceRegistrator
import com.hypertino.transport.resolvers.consul.ConsulServiceResolver
import com.typesafe.config.Config
import monix.execution.Scheduler
import scaldi.Module

class HyperStorageServiceModule extends Module {
  final val INTERNAL_SCHEDULER = "hyperstorage-internal-scheduler"
  private val internalScheduler = monix.execution.Scheduler.io(INTERNAL_SCHEDULER)
  bind[Scheduler] identifiedBy INTERNAL_SCHEDULER to internalScheduler
  bind[CassandraConnector] to new CassandraConnector {
    override def connect(): Session = {
      CassandraConnector.createCassandraSession(inject[Config].getConfig("hyperstorage.cassandra"))
    }
  }
  bind[HyperStorageService] to injected[HyperStorageService]
  bind[ServiceResolver] identifiedBy "hyperstorage-cluster-resolver" to new ConsulServiceResolver(
    inject[Config].getConfig("hyperstorage.zmq-cluster-manager.service-resolver")
  )(internalScheduler)
  bind[ServiceRegistrator] identifiedBy "hyperstorage-cluster-registrator" to new ConsulServiceRegistrator(
    inject[Config].getConfig("hyperstorage.zmq-cluster-manager.service-registrator")
  )(internalScheduler)
}
