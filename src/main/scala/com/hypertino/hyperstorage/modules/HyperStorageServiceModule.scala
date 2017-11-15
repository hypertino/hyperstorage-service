/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

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
