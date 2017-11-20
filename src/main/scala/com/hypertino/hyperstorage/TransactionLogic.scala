/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage

import java.util.{TimeZone, UUID}
import java.util.zip.CRC32

import com.datastax.driver.core.utils.UUIDs
import com.hypertino.hyperstorage.db.Transaction
import com.hypertino.hyperstorage.sharding.{ShardTask, ShardedClusterData}

object TransactionLogic {
  final val MAX_TRANSACTIONS: Int = 1024
  final val timeZone = TimeZone.getTimeZone("UTC")
  final val HB_HEADER_TEMPLATE_URI = "HB-Template-Uri"
  final val HB_HEADER_FILTER = "HB-Filter"

  def newTransaction(documentUri: String, itemId: String, revision: Long, body: String, uuid: UUID) = Transaction(
    dtQuantum = getDtQuantum(System.currentTimeMillis()),
    partition = partitionFromUri(documentUri),
    documentUri = documentUri,
    itemId = itemId,
    revision = revision,
    uuid = uuid,
    body = body,
    obsoleteIndexItems = None,
    completedAt = None
  )

  def partitionFromUri(uri: String): Int = {
    val crc = new CRC32()
    crc.update(uri.getBytes("UTF-8"))
    (crc.getValue % MAX_TRANSACTIONS).toInt
  }

  def getDtQuantum(unixTime: Long): Long = {
    unixTime / (1000 * 60)
  }

  def getUnixTimeFromQuantum(qt: Long): Long = {
    qt * 1000 * 60
  }

  def getPartitions(data: ShardedClusterData): Seq[Int] = {
    0 until TransactionLogic.MAX_TRANSACTIONS flatMap { partition ⇒
      if (data.keyIsFor(partition.toString) == data.selfId)
        Some(partition)
      else
        None
    }
  }
}
