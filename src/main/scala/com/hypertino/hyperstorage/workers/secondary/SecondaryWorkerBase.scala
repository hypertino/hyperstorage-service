package com.hypertino.hyperstorage.workers.secondary

import akka.event.LoggingAdapter
import com.hypertino.hyperbus.model.{ErrorBody, HyperbusError, InternalServerError, MessagingContext}
import com.hypertino.hyperstorage.{ContentLogic, ResourcePath}
import com.hypertino.hyperstorage.sharding.ShardTaskComplete

import scala.util.control.NonFatal

trait SecondaryWorkerBase {
  def log: LoggingAdapter

  protected def validateCollectionUri(uri: String) = {
    val ResourcePath(documentUri, itemId) = ContentLogic.splitPath(uri)
    if (!ContentLogic.isCollectionUri(uri) || !itemId.isEmpty || documentUri != uri) {
      throw new IllegalArgumentException(s"'$uri' isn't a collection URI.")
    }
  }

  protected def withHyperbusException(task: SecondaryTaskTrait): PartialFunction[Throwable, ShardTaskComplete] = {
    case NonFatal(e) ⇒
      log.error(e, s"Can't execute $task")
      val he = e match {
        case h: HyperbusError[ErrorBody] ⇒ h
        case other ⇒ InternalServerError(ErrorBody("failed", Some(e.toString)))(MessagingContext.empty)
      }
      ShardTaskComplete(task, IndexDefTaskTaskResult(he.serializeToString))
  }
}
