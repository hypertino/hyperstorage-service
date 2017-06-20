package com.hypertino.hyperstorage.modules

import com.datastax.driver.core.Session
import com.hypertino.hyperstorage.{CassandraConnector, HyperStorageService}
import com.hypertino.service.control.ConsoleModule
import com.hypertino.service.control.api.Service
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext

class ServiceModule(config: Config) extends ConsoleModule {
  bind[Config] to config
  bind[ExecutionContext] to scala.concurrent.ExecutionContext.Implicits.global
  bind[CassandraConnector] to new CassandraConnector {
    override def connect(): Session = {
      CassandraConnector.createCassandraSession(config.getConfig("cassandra"))
    }
  }
  bind[Service] to injected[HyperStorageService]
}
