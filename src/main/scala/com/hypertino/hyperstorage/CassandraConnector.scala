/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, LatencyAwarePolicy, TokenAwarePolicy}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

trait CassandraConnector {
  def connect(): Session
}

object CassandraConnector extends StrictLogging{
  def createCassandraSession(hosts: Seq[String], datacenter: String, keyspace: String, connectTimeoutMillis: Int = 3000, readTimeoutMillis: Int = 500) =
    CassandraSessionBuilder.build(hosts, datacenter, keyspace, connectTimeoutMillis, readTimeoutMillis)

  def createCassandraSession(config: Config) =
    CassandraSessionBuilder.build(config)

  private class HostListener(connectTimeoutMillis: Long) extends Host.StateListener {
    private val latch = new CountDownLatch(1)

    def waitForConnection() {
      logger.info("Waiting for connection. Latch count " + latch.getCount)

      latch.await(connectTimeoutMillis, TimeUnit.MILLISECONDS)

      logger.info("Connection waited. Latch count " + latch.getCount)

      if (latch.getCount > 0) {
        throw new RuntimeException("No cassandra host in up state")
      }
    }

    def onAdd(p1: Host) {
      logger.info("Cassandra host add: " + p1)
      latch.countDown()
    }

    def onSuspected(p1: Host) {
      logger.info("Cassandra host suspected: " + p1)
    }

    def onRemove(p1: Host) {
      logger.info("Cassandra host remove: " + p1)
    }

    def onUp(p1: Host) {
      logger.info("Cassandra host up: " + p1)
    }

    def onDown(p1: Host) {
      logger.info("Cassandra host down: " + p1)
    }
  }


  private object CassandraSessionBuilder {
    def build(config: Config) = {
      val (cluster, listener) = defaultCluster(config)
      val keyspace = config.getString("keyspace")
      try {
        session(cluster, listener, keyspace)
      }
      catch {
        case NonFatal(e) ⇒
          cluster.close()
          throw e
      }
    }

    def defaultCluster(conf: Config) = newCluster(
      hosts = conf.getStringList("hosts"),
      datacenter = conf.getString("datacenter"),
      connectTimeoutMillis = conf.getDuration("connect-timeout", TimeUnit.MILLISECONDS).toInt,
      readTimeoutMillis = conf.getDuration("read-timeout", TimeUnit.MILLISECONDS).toInt
    )

    private def newCluster(hosts: Seq[String], datacenter: String, connectTimeoutMillis: Int, readTimeoutMillis: Int):
    (Cluster, HostListener) = {
      logger.info(s"Create cassandra cluster: $hosts, dc=$datacenter, $connectTimeoutMillis, $readTimeoutMillis")

      val cluster: Cluster = Option(datacenter).filter(_.nonEmpty)
        .foldLeft(Cluster.builder)((cluster, dcName) ⇒
          cluster.withLoadBalancingPolicy(
            LatencyAwarePolicy.builder(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dcName)))
              .withRetryPeriod(40, TimeUnit.SECONDS)
              .withMininumMeasurements(30)
              .build())
        )
        .addContactPoints(hosts: _*)
        .withQueryOptions(
          new QueryOptions()
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
        ).withSocketOptions(
        new SocketOptions()
          .setTcpNoDelay(true)
          .setKeepAlive(true)
          .setConnectTimeoutMillis(connectTimeoutMillis)
          .setReadTimeoutMillis(readTimeoutMillis)
      ).build()

      val listener = new HostListener(connectTimeoutMillis)
      cluster.register(listener)

      (cluster, listener)
    }

    private def session(cluster: Cluster, listener: HostListener, keyspace: String) = {
      logger.info(s"Start cassandra session: cluster=${cluster.getClusterName}, ks=$keyspace")

      val session =
        try cluster.connect(keyspace) catch {
          case e: NoHostAvailableException ⇒
            logger.error("NoHostAvailableException on connect: " + e.getErrors)
            throw e
        }

      listener.waitForConnection()
      session
    }

    def build(hosts: Seq[String], datacenter: String, keyspace: String, connectTimeoutMillis: Int, readTimeoutMillis: Int) = {
      val (cluster, listener) = newCluster(hosts, datacenter, connectTimeoutMillis, readTimeoutMillis)
      try {
        session(cluster, listener, keyspace)
      }
      catch {
        case NonFatal(e) ⇒
          cluster.close()
          throw e
      }
    }
  }

}
