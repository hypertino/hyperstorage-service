package com.hypertino.hyperstorage.workers.primary

import akka.actor.ActorRef
import com.google.common.base.Utf8
import com.hypertino.binders.value
import com.hypertino.binders.value.{Bool, Lst, Obj, Text, Value, ValueVisitor}
import com.hypertino.hyperstorage.sharding.{BatchProcessor, LocalTask, ShardTask}

import scala.collection.mutable.ArrayBuffer

/**
  * @param maxBatchRecords
  * @param maxBatchSizeInBytes we need this to prevent exceeding mutation size in C*
  */
class PrimaryBatchProcessor(maxBatchRecords: Int,
                            maxBatchSizeInBytes: Long) extends BatchProcessor {
  override def apply(input: Seq[(ActorRef, ShardTask)]): Seq[Seq[(ActorRef, ShardTask)]] = {
    if (input.tail.isEmpty) {
      Seq(input)
    }
    else {
      val result = ArrayBuffer[Seq[(ActorRef, ShardTask)]]()
      var batch = ArrayBuffer[(ActorRef, ShardTask)]()
      var batchSizeInBytes: Long = 0
      val it = input.iterator
      while (it.hasNext) {
        val next = it.next()
        val nextSize = getSizeInBytes(next._2)
        if (batch.size + 1 > maxBatchRecords || (batchSizeInBytes + nextSize) > maxBatchSizeInBytes) {
          result += batch
          batch = ArrayBuffer[(ActorRef, ShardTask)]()
          batch += next
          batchSizeInBytes = nextSize
        }
        else {
          batch += next
          batchSizeInBytes += nextSize
        }
      }
      if (batch.nonEmpty) {
        result += batch
      }
      result
    }
  }

  def getSizeInBytes(task: ShardTask): Long = {
    task match {
      case lt: LocalTask => lt.request match {
        case pt: PrimaryWorkerRequest =>
          128 + // todo: calculate real size
            SizeHelpers.utf8size(pt.path) +
            pt.headers.underlying.map { kv =>
              4 + SizeHelpers.utf8size(kv._1) + kv._2 ~~ SizeVisitor
            }.sum +
            pt.body.content ~~ SizeVisitor
      }
    }
  }
}

private [primary] object SizeHelpers {
  def utf8size(s: String): Long = Utf8.encodedLength(s)
}

private [primary] object SizeVisitor extends ValueVisitor[Long] {
  override def visitNumber(d: value.Number): Long = {
    d.v.toString().length // todo: this is not the most efficient
  }
  override def visitText(d: Text): Long = SizeHelpers.utf8size(d.v)
  override def visitObj(d: Obj): Long = 2 + d.v.map { kv =>
    4 + SizeHelpers.utf8size(kv._1) + kv._2 ~~ SizeVisitor
  }.sum
  override def visitLst(d: Lst): Long = 2 + d.v.map { v =>
    1 + v ~~ SizeVisitor
  }.sum
  override def visitBool(d: Bool): Long = if (d.v) 4 else 5
  override def visitNull(): Long = 4
}
