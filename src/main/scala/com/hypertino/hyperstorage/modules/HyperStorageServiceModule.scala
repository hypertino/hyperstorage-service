package com.hypertino.hyperstorage.modules

import com.datastax.driver.core.Session
import com.hypertino.hyperstorage.{CassandraConnector, HyperStorageService}
import com.typesafe.config.Config
import scaldi.Module

class HyperStorageServiceModule extends Module {
  bind[CassandraConnector] to new CassandraConnector {
    override def connect(): Session = {
      CassandraConnector.createCassandraSession(inject[Config].getConfig("hyperstorage.cassandra"))
    }
  }
  bind[HyperStorageService] to injected[HyperStorageService]
}
