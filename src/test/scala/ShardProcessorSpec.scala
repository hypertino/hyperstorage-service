/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.hyperstorage.internal.api.NodeStatus
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class ShardProcessorSpecZMQ extends FlatSpec
  with Matchers
  with ScalaFutures
  with TestHelpers
  with Eventually {

  override def defaultClusterTransportIsZMQ: Boolean = true

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  "ShardProcessor" should "become Active and shutdown gracefully" in {
    implicit val as = testActorSystem()
    val fsm = createShardProcessor("test-group")
    shutdownShardProcessor(fsm)
  }

  it should "process task" in {
    implicit val as = testActorSystem()
    val fsm = createShardProcessor("test-group")
    val (task, r) = testTask("abc", "t1")
    fsm ! task
    testKit().awaitCond(r.isProcessed)
    shutdownShardProcessor(fsm)
  }

  it should "stash task while Activating and process it later" in {
    implicit val as = testActorSystem()
    val fsm = createShardProcessor("test-group", waitWhileActivates = false)
    val (task, r) = testTask("abc", "t1")
    fsm ! task
    fsm.stateName should equal(NodeStatus.ACTIVATING)
    r.isProcessed should equal(false)
    testKit().awaitCond(r.isProcessed)
    fsm.stateName should equal(NodeStatus.ACTIVE)
    shutdownShardProcessor(fsm)
  }

  it should "stash task when workers are busy and process later" in {
    implicit val as = testActorSystem()
    val tk = testKit()
    val fsm = createShardProcessor("test-group")
    val (task1, r1) = testTask("abc", "t1")
    val (task2, r2) = testTask("abc2", "t2")
    fsm ! task1
    fsm ! task2
    tk.awaitCond(r1.isProcessed)
    tk.awaitCond(r2.isProcessed)
    shutdownShardProcessor(fsm)
  }

  it should "stash task when URL is 'locked' and it process later" in {
    implicit val as = testActorSystem()
    val tk = testKit()
    val fsm = createShardProcessor("test-group", 2)
    val (task1, r1) = testTask("abc1", "t1", 500)
    val (task1x, r1x) = testTask("abc1", "t1x", 500)
    val (task2, r2) = testTask("abc2", "t2", 500)
    val (task2x, r2x) = testTask("abc2", "t2x", 500)
    fsm ! task1
    fsm ! task1x
    fsm ! task2
    fsm ! task2x
    tk.awaitCond({
      r1.isProcessed && !r1x.isProcessed &&
        r2.isProcessed && !r2x.isProcessed
    }, 750.milli)
    tk.awaitCond({
      r1x.isProcessed && r2x.isProcessed
    }, 2.second)
    shutdownShardProcessor(fsm)
  }
}

class ShardProcessorSpecAkkaCluster extends ShardProcessorSpecZMQ{
  override def defaultClusterTransportIsZMQ: Boolean = false
}
