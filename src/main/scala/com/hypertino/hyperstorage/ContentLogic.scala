package com.hypertino.hyperstorage

import com.hypertino.hyperbus.utils.uri.{SlashToken, TextToken, Token, UriPathParser}
import com.hypertino.hyperstorage.db.{Content, ContentStatic}
import com.hypertino.inflector.English

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
    if (slashCount > 0 && lastToken.exists {
      case TextToken(s) if s.forall(c ⇒ c == '~' || (c >= 'a' && c <= 'z')) ⇒ true
      case _ ⇒ false
    }) {
      val s = lastToken.get.asInstanceOf[TextToken].value
      English.singular(s.substring(0, s.length-1)) + "_id"
    }
    else {
      "id"
    }
  }
}
