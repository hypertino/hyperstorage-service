/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import akka.cluster.Cluster
import com.hypertino.hyperstorage.internal.api.NodeStatus
import com.hypertino.hyperstorage.sharding.ShutdownProcessor
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class TwoNodesSpec extends FlatSpec with ScalaFutures with TestHelpers {
  "ShardProcessor" should "become Active" in {
    val (fsm1, actorSystem1, testKit1) = {
      implicit val actorSystem1 = testActorSystem(1)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem1, testKit(1))
    }

    val (fsm2, actorSystem2, testKit2) = {
      implicit val actorSystem2 = testActorSystem(2)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem2, testKit(2))
    }

    testKit1.awaitCond(fsm1.stateName == NodeStatus.ACTIVE && fsm1.stateData.nodes.nonEmpty)
    testKit2.awaitCond(fsm2.stateName == NodeStatus.ACTIVE && fsm2.stateData.nodes.nonEmpty)

    shutdownShardProcessor(fsm1)(actorSystem1)
    shutdownCluster(1)
    Thread.sleep(1000)
    shutdownShardProcessor(fsm2)(actorSystem2)
  }

  it should "become Active sequentially" in {
    val (fsm1, actorSystem1, testKit1) = {
      implicit val actorSystem1 = testActorSystem(1)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem1, testKit(1))
    }

    testKit1.awaitCond(fsm1.stateName == NodeStatus.ACTIVE && fsm1.stateData.nodes.isEmpty)

    val (fsm2, actorSystem2, testKit2) = {
      implicit val actorSystem2 = testActorSystem(2)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem2, testKit(2))
    }

    testKit2.awaitCond(fsm2.stateName == NodeStatus.ACTIVE && fsm2.stateData.nodes.nonEmpty, 5 second)

    shutdownShardProcessor(fsm1)(actorSystem1)
    shutdownCluster(1)
    //shutdownActorSystem(1)

    testKit2.awaitCond(fsm2.stateName == NodeStatus.ACTIVE && fsm2.stateData.nodes.isEmpty, 10 second)
    shutdownShardProcessor(fsm2)(actorSystem2)
  }

  "Tasks "should "distribute to corresponding actors" in {
    val (fsm1, actorSystem1, testKit1, address1) = {
      implicit val actorSystem1 = testActorSystem(1)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem1, testKit(1), Cluster(actorSystem1).selfAddress.toString)
    }

    val (fsm2, actorSystem2, testKit2, address2) = {
      implicit val actorSystem2 = testActorSystem(2)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem2, testKit(2), Cluster(actorSystem2).selfAddress.toString)
    }

    testKit1.awaitCond(fsm1.stateName == NodeStatus.ACTIVE && fsm1.stateData.nodes.nonEmpty, 5 second)
    testKit2.awaitCond(fsm2.stateName == NodeStatus.ACTIVE && fsm2.stateData.nodes.nonEmpty, 5 second)

    val task1 = TestShardTask("abc1", "t1")
    fsm1 ! task1
    testKit1.awaitCond(task1.isProcessed)
    task1.processorPath should include(address1)

    val task2 = TestShardTask("klx1", "t2")
    fsm2 ! task2
    testKit2.awaitCond(task2.isProcessed)
    task2.processorPath should include(address2)
  }

  it should "be forwarded to corresponding actors and results are forwarded back" in {
    val (fsm1, actorSystem1, testKit1, address1) = {
      implicit val actorSystem1 = testActorSystem(1)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem1, testKit(1), Cluster(actorSystem1).selfAddress.toString)
    }

    val (fsm2, actorSystem2, testKit2, address2) = {
      implicit val actorSystem2 = testActorSystem(2)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem2, testKit(2), Cluster(actorSystem2).selfAddress.toString)
    }

    testKit1.awaitCond(fsm1.stateName == NodeStatus.ACTIVE && fsm1.stateData.nodes.nonEmpty, 5 second)
    testKit2.awaitCond(fsm2.stateName == NodeStatus.ACTIVE && fsm2.stateData.nodes.nonEmpty, 5 second)

    val task1 = TestShardTask("abc1", "t3")

    {
      import testKit1._
      fsm2 ! task1
    }

    testKit1.awaitCond(task1.isProcessed)
    task1.processorPath should include(address1)
    val r1 = testKit1.expectMsgType[TestShardTaskResult]
    r1.id shouldBe task1.id

    val task2 = TestShardTask("klx1", "t4")
    //fsm1 ! task2

    {
      import testKit2._
      fsm1 ! task2
    }

    testKit2.awaitCond(task2.isProcessed)
    task2.processorPath should include(address2)
    val r2 = testKit2.expectMsgType[TestShardTaskResult]
    r2.id shouldBe task2.id
  }

  it should "for deactivating actor shouldn't be processed before deactivation complete" in {
    val (fsm1, actorSystem1, testKit1, address1) = {
      implicit val actorSystem1 = testActorSystem(1)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem1, testKit(1), Cluster(actorSystem1).selfAddress.toString)
    }

    val (fsm2, actorSystem2, testKit2, address2) = {
      implicit val actorSystem2 = testActorSystem(2)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem2, testKit(2), Cluster(actorSystem2).selfAddress.toString)
    }

    testKit1.awaitCond(fsm1.stateName == NodeStatus.ACTIVE && fsm1.stateData.nodes.nonEmpty)
    testKit2.awaitCond(fsm2.stateName == NodeStatus.ACTIVE && fsm2.stateData.nodes.nonEmpty)

    fsm1 ! ShutdownProcessor

    testKit1.awaitCond({
      fsm1.stateName == NodeStatus.DEACTIVATING
    }, 10.second)

    val task1 = TestShardTask("abc1", "t5", sleep = 500)
    fsm1 ! task1

    val task2 = TestShardTask("abc1", "t6", sleep = 500)
    fsm2 ! task2

    val c1 = Cluster(actorSystem1)
    c1.down(c1.selfAddress)

    testKit2.awaitCond({
      assert(!(
          !fsm2.stateData.nodes.forall(_._2.status == NodeStatus.PASSIVE)
          &&
          (task2.isProcessed || task1.isProcessed)
        ))
      fsm2.stateData.nodes.isEmpty
    }, 10 second)

    testKit2.awaitCond(task1.isProcessed && task2.isProcessed)
    task1.processorPath should include(address2)
    task2.processorPath should include(address2)
  }

  "Processor" should "not confirm sync/activation until completes processing corresponding task" in {
    val (fsm1, actorSystem1, testKit1, address1) = {
      implicit val actorSystem1 = testActorSystem(1)
      (createShardProcessor("test-group"), actorSystem1, testKit(1), Cluster(actorSystem1).selfAddress.toString)
    }

    val task1 = TestShardTask("klx1", "t7", sleep = 6000)
    fsm1 ! task1
    val task2 = TestShardTask("klx1", "t8")
    fsm1 ! task2
    testKit1.awaitCond(task1.isProcessingStarted)

    val (fsm2, actorSystem2, testKit2, address2) = {
      implicit val actorSystem2 = testActorSystem(2)
      (createShardProcessor("test-group", waitWhileActivates = false), actorSystem2, testKit(2), Cluster(actorSystem2).selfAddress.toString)
    }

    testKit1.awaitCond({
      assert(fsm2.stateName == NodeStatus.ACTIVATING)
      task1.isProcessed
    }, 10 second)

    task1.processorPath should include(address1)
    testKit2.awaitCond(task2.isProcessed, 10 second)
    task2.processorPath should include(address2)
  }
}
