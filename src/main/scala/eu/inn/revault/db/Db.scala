package eu.inn.revault.db

import java.util.Date

import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.{CamelCaseToSnakeCaseConverter, SnakeCaseToCamelCaseConverter}

import scala.concurrent.{Future, ExecutionContext}

/*
dt timestamp,
	channel int,
	path text,
	revision bigint,
	body text,
	is_complete boolean,
*/

case class Content(
                  path: String, // todo: rename to prefix?
                  lastSegment: String,
                  revision: Long,
                  monitorDt: Date,
                  monitorChannel: Int,
                  body: Option[String],
                  isDeleted: Boolean,
                  createdAt: Date,
                  modifiedAt: Option[Date]
                  )

case class Monitor(
                  dt: Date,
                  channel: Int,
                  path: String,
                  revision: Long,
                  body: String,
                  completedAt: Option[Date]
                  )

case class Channel(
                  channel: Int,
                  checkDate: Date
                  )

class Db(session: com.datastax.driver.core.Session)(implicit ec: ExecutionContext) {
  private [this] implicit val sessionQueryCache = new SessionQueryCache[CamelCaseToSnakeCaseConverter](session)

  def selectContent(path: String, lastSegment: String): Future[Option[Content]] = cql"""
      select path,last_segment,revision,monitor_dt,monitor_channel,body,is_deleted,created_at,modified_at from content
      where path=$path and last_segment=$lastSegment
    """.oneOption[Content]

  /*def selectAllContent: Future[Iterator[Content]] = cql"""
      select path,last_segment,revision,monitor_dt,monitor_channel,body,is_deleted,created_at,modified_at from content
    """.all[Content]*/

  def selectMonitor(dt: Date, channel: Int, path: String): Future[Option[Monitor]] = cql"""
      select dt,channel,path,revision,body,completed_at from monitor
      where dt=$dt and channel=$channel and path=$path
    """.oneOption[Monitor]

  def insertContent(content: Content): Future[Unit] = cql"""
      insert into content(path,last_segment,revision,monitor_dt,monitor_channel,body,is_deleted,created_at,modified_at)
      values(?,?,?,?,?,?,?,?,?)
    """.bind(content).execute()

  def insertMonitor(monitor: Monitor): Future[Unit] = cql"""
      insert into monitor(dt,channel,path,revision,body,completed_at)
      values(?,?,?,?,?,?)
    """.bind(monitor).execute()

  def completeMonitor(monitor: Monitor): Future[Unit] = cql"""
      update monitor set completed_at=dateOf(now())
      where dt=${monitor.dt} and channel=${monitor.channel} and path=${monitor.path} and revision=${monitor.revision}
    """.execute()

/*
  def insertUser(user: User): Future[Unit] = cql"insert into users(userid, name) values (?, ?)".bind(user).execute()

  // returns Future[Iterator[User]]
  def selectAllUsers: Future[Iterator[User]] = cql"select * from users".all[User]

  // if no user is found will throw NoRowsSelectedException
  def selectUser(userId: Int) = cql"select * from users where userId = $userId".one[User]

  // if no user is found will return None, otherwise Some(User)
  def selectUserIfFound(userId: Int) = cql"select * from users where userId = $userId".oneOption[User]
 */
}
