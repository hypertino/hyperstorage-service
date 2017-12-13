/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage.utils

object ErrorCode {
  val INTERNAL_SERVER_ERROR = "internal_server_error"

  final val INDEX_NOT_FOUND = "index-not-found"
  final val ALREADY_EXISTS = "already_exists"

  final val UPDATE_FAILED = "update_failed"
  final val COLLECTION_IS_VIEW = "collection_is_view"
  final val FIELD_IS_PROTECTED = "field_is_protected"
  final val COLLECTION_VIEW_CONFLICT = "collection_view_conflict"
  final val COLLECTION_PUT_NOT_IMPLEMENTED = "collection_put_not_implemented"
  final val COLLECTION_PATCH_NOT_IMPLEMENTED = "collection_patch_not_implemented"

  final val VIEW_MODIFICATION = "view_modification"
  final val NOT_MATCHED = "not_matched"

  final val QUERY_TIMEOUT = "query_timeout"
  final val REQUEST_TIMEOUT = "request_timeout"
  final val QUERY_COUNT_LIMITED = "query_count_limited"
  final val QUERY_SKIPPED_ROWS_LIMITED = "query_skipped_rows_limited"
  final val NOT_FOUND = "not_found"

  final val BACKGROUND_TASK_FAILED = "background_task_failed"
  final val TRANSACTION_LIMIT = "transaction_limit"
}
