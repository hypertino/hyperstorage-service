/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperstorage

import com.hypertino.binders.value.{Lst, Value}
import com.hypertino.hyperbus.model.{DynamicRequest, RequestBase}
import com.hypertino.hyperbus.utils.uri._
import com.hypertino.hyperstorage.api.HyperStorageHeader
import com.hypertino.hyperstorage.db.{Content, ContentBase, ContentStatic}
import com.hypertino.inflector.English

import scala.collection.mutable

case class ResourcePath(documentUri: String, itemId: String)

object ContentLogic {

  // ? & # is not allowed, it means that query have been sent
  val allowedCharSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/[]@!$&'()*+,;=".toSet

  // todo: describe uri to resource/collection item matching
  def splitPath(path: String): ResourcePath = {
    if (path.startsWith("/") || path.endsWith("/"))
      throw new IllegalArgumentException(s"$path is invalid (ends or starts with '/')")

    if (path.exists(c ⇒ !allowedCharSet.contains(c)))
      throw new IllegalArgumentException(s"$path contains invalid characters")

    val segments = path.split('/')
    if (segments.isEmpty || segments.exists(_.isEmpty))
      throw new IllegalArgumentException(s"$path is invalid (empty segments)")

    if (segments.length > 1) {
      // collection item
      val r = segments.reverse
      val a = r(1)
      if (isCollectionUri(a)) {
        val documentUri = r.tail.reverse.mkString("/")
        val itemId = r.head
        ResourcePath(documentUri, itemId)
      }
      else {
        ResourcePath(path, "")
      }
    } else {
      // document
      ResourcePath(path, "")
    }
  }

  def isCollectionUri(path: String): Boolean = path.endsWith("~")

  implicit class ContentWrapper(val content: Content) {
    def uri = {
      if (content.itemId.isEmpty)
        content.documentUri
      else
        content.documentUri + "/" + content.itemId
    }

    def partition = TransactionLogic.partitionFromUri(content.documentUri)
  }

  implicit class ContentStaticWrapper(val content: ContentStatic) {
    def partition = TransactionLogic.partitionFromUri(content.documentUri)
  }

  def getIdFieldName(path: String): String = {
    val tokens = UriPathParser.tokens(path)
    var slashCount = 0
    var lastToken: Option[Token] = None
    while(tokens.hasNext) {
      lastToken = Some(tokens.next())
      if (lastToken.get == SlashToken) {
        slashCount += 1
      }
    }
    if (/*slashCount > 0 && */lastToken.exists {
      case TextToken(s) if s.forall(c ⇒ c == '~' || (c >= 'a' && c <= 'z')) ⇒ true
      case _ ⇒ false
    }) {
      val s = lastToken.get.asInstanceOf[TextToken].value
      if (s.endsWith("~")) {
        English.singular(s.substring(0, s.length - 1)) + "_id"
      }
      else {
        English.singular(s) + "_id"
      }
    }
    else {
      "id"
    }
  }

  def pathAndTemplateToId(path: String, template: String): Option[String] = {
    val templateTokens = UriPathParser.tokens(template) // todo: cache this!
    val pathTokens = UriPathParser.tokens(path)
    var matches = true
    val idElements = mutable.MutableList[String]()
    while (pathTokens.hasNext && templateTokens.hasNext) {
      val pt = pathTokens.next()
      val tt = templateTokens.next()
      tt match {
        case ParameterToken(_) ⇒ pt match {
          case TextToken(s) ⇒
            idElements += s
          case _ ⇒
            matches = false
        }

        case _ ⇒
          if (pt != tt) {
            matches = false
          }
      }
    }
    if (pathTokens.hasNext || templateTokens.hasNext) {
      matches = false
    }
    if (matches) {
      Some(idElements.mkString(":"))
    }
    else {
      None
    }
  }

  // todo: using ETag based on revision works not well for collection items
  def checkPrecondition(request: RequestBase, content: Option[ContentBase]): Boolean  = {
    val ifMatch = request.headers.get(HyperStorageHeader.IF_MATCH).forall { m ⇒
      val eTags = parseETags(m)
      content.exists {
        c ⇒
          eTags.contains("*") || eTags.contains("\"" + c.revision.toHexString + "\"")
      }
    }

    val ifNoneMatch = request.headers.get(HyperStorageHeader.IF_NONE_MATCH).forall { m ⇒
      val eTags = parseETags(m)
      content.forall {
        c ⇒
          !eTags.contains("*") && !eTags.contains("\"" + c.revision.toHexString + "\"")
      }
    }

    ifMatch && ifNoneMatch
  }

  def parseETags(v: Value): Set[String] = {
    {
      v match {
        case Lst(items) ⇒ items.map(_.toString()).toSet
        case _ ⇒ v.toString.split(',').map(_.trim).toSet
      }
    } map { s ⇒
      if (s.startsWith("W/")) { // We don't do weak comparasion
        s.substring(2)
      }
      else {
        s
      }
    }
  }
}
