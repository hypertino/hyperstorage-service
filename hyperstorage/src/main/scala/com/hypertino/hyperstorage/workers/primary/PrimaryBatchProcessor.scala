package com.hypertino.hyperstorage.workers.primary

import akka.actor.ActorRef
import com.hypertino.hyperstorage.sharding.{BatchProcessor, ShardTask}

/**
  * @param maxBatchRecords
  */
class PrimaryBatchProcessor(maxBatchRecords: Int) extends BatchProcessor {
  override def apply(input: Seq[(ActorRef, ShardTask)]): Seq[Seq[(ActorRef, ShardTask)]] = {
    if (input.tail.isEmpty) {
      Seq(input)
    }
    else {
      input.grouped(maxBatchRecords).toSeq
    }
  }
}
