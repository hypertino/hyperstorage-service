package com.hypertino.hyperstorage.workers.primary

import akka.actor.ActorRef
import com.hypertino.hyperbus.model.Method
import com.hypertino.hyperstorage.ContentLogic
import com.hypertino.hyperstorage.sharding.{BatchProcessor, LocalTask, ShardTask}

/**
  * @param maxBatchRecords
  */
class PrimaryBatchProcessor(maxBatchRecords: Int) extends BatchProcessor {
  override def apply(input: Seq[(ActorRef, ShardTask)]): Seq[Seq[(ActorRef, ShardTask)]] = {
    if (input.tail.isEmpty) {
      Seq(input)
    }
    else {
      input
        .filterNot(i => isCollectionDelete(i._2))
        .grouped(maxBatchRecords).toSeq ++
      input
        .filter(i => isCollectionDelete(i._2))
        .grouped(1).toSeq
    }
  }

  private def isCollectionDelete(task: ShardTask): Boolean = {
    task match {
      case LocalTask(key, _, _, _, r: PrimaryWorkerRequest, _)
        if ContentLogic.isCollectionUri(r.path) && r.headers.method == Method.DELETE => true

      case _ => false
    }
  }
}
