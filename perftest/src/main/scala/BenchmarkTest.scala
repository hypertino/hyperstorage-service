import com.hypertino.binders.value.{Lst, Obj, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{DynamicBody, DynamicResponse, HRL, Header, Headers, MessagingContext, Ok, Response}
import com.hypertino.hyperbus.transport.api.{ServiceRegistrator, ServiceResolver}
import com.hypertino.hyperbus.transport.registrators.DummyRegistrator
import com.hypertino.hyperstorage.api.{ContentDelete, ContentGet, ContentPatch, ContentPut}
import com.hypertino.service.config.ConfigModule
import com.hypertino.transport.resolvers.consul.ConsulServiceResolver
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicLong
import scaldi.{Injectable, Module}

import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}

class BenchmarkModule extends Module {
  val scheduler = monix.execution.Scheduler.Implicits.global
  bind[Scheduler] to scheduler
  bind[Hyperbus] to injected[Hyperbus]
  bind[ServiceRegistrator] to DummyRegistrator
}

class ServiceResolverModule extends Module {
  bind[ServiceResolver] to new ConsulServiceResolver(
    inject[Config].getConfig("service-resolver")
  )(inject[Scheduler])
}

case class Counters(success: AtomicLong, fail: AtomicLong)

object BenchmarkTest extends Injectable with StrictLogging {
  private val random = new Random()
  private val TEST_OBJECTS_SIZE = 10000
  private val TEST_REPEATED_OBJECTS_SIZE = 60
  private val TEST_REPEATED_SIZE = 500
  private val TEST_COLLECTIONS_SIZE = 30
  private val TEST_COLLECTIONS_ITEMS_SIZE = 500
  private val PARALLELISM = 256
  private val randomObjs = 0 until TEST_OBJECTS_SIZE map { _ ⇒ (nextRandomObj(), nextRandomObj()) }
  private val randomRepeatedObjs = 0 until TEST_REPEATED_OBJECTS_SIZE map { _ ⇒ (nextRandomObj(), nextRandomObj()) }
  private val randomCollectionKeys = 0 until TEST_COLLECTIONS_SIZE map { _ ⇒ random.alphanumeric.take(12).mkString }
  private val randomCollectionItems = 0 until TEST_COLLECTIONS_ITEMS_SIZE map { _ ⇒ nextRandomObj() }
  private var lastErrorLogged = AtomicLong(System.currentTimeMillis())

  def logErrorThrottled(ex: Throwable): Unit = {
    val l = lastErrorLogged.get
    val now = System.currentTimeMillis()
    if ((l + 5000) < now) {
      logger.error("error", ex)
      lastErrorLogged.compareAndSet(l, now)
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val mcx = MessagingContext.Implicits.emptyContext
    implicit val injector =
      new ServiceResolverModule ::
        new BenchmarkModule ::
        ConfigModule()
    implicit val scheduler = inject[Scheduler]
    val hyperbus = inject[Hyperbus]
    hyperbus.startServices()

    measure(s"PUT $TEST_OBJECTS_SIZE documents") { c ⇒
      val prefix = "test-objects/"
      parallel(randomObjs.map { obj ⇒
        val path = prefix + obj._1.dynamic.key.toString
        hyperbus
          .ask(ContentPut(path, DynamicBody(obj._1)))
          .materialize
          .map {
            case Success(_) ⇒ c.success.increment()
            case Failure(_) ⇒ c.fail.increment()
          }
      })
    }

    measure(s"GET $TEST_OBJECTS_SIZE objects") { c ⇒
      val prefix = "test-objects/"
      parallel(randomObjs.map { obj ⇒
        val path = prefix + obj._1.dynamic.key.toString
        hyperbus
          .ask(ContentGet(path))
          .materialize
          .map {
            case Success(_) ⇒ c.success.increment()
            case Failure(_) ⇒ c.fail.increment()
          }
      })
    }

    measure(s"PATCH $TEST_OBJECTS_SIZE documents") { c ⇒
      val prefix = "test-objects/"
      parallel(randomObjs.map { obj ⇒
        val path = prefix + obj._1.dynamic.key.toString
        hyperbus
          .ask(ContentPatch(path, DynamicBody(obj._2)))
          .materialize
          .map {
            case Success(_) ⇒ c.success.increment()
            case Failure(_) ⇒ c.fail.increment()
          }
      })
    }

    measure(s"REPATED $TEST_REPEATED_SIZE PATCHES of $TEST_REPEATED_OBJECTS_SIZE documents") { c ⇒
      val prefix = "test-objects/"
      parallel(
        0 until TEST_REPEATED_SIZE flatMap { _ =>
          randomRepeatedObjs.map { obj ⇒
            val path = prefix + obj._1.dynamic.key.toString
            hyperbus
              .ask(ContentPatch(path, DynamicBody(obj._2 % Obj.from("e" → random.alphanumeric.take(32).mkString))))
              .materialize
              .map {
                case Success(_) ⇒ c.success.increment()
                case Failure(_) ⇒ c.fail.increment()
              }
          }
        }
      )
    }

    measure(s"DELETE $TEST_OBJECTS_SIZE documents") { c ⇒
      val prefix = "test-objects/"
      parallel(randomObjs.map { obj ⇒
        val path = prefix + obj._1.dynamic.key.toString
        hyperbus
          .ask(ContentDelete(path))
          .materialize
          .map {
            case Success(_) ⇒ c.success.increment()
            case Failure(_) ⇒ c.fail.increment()
          }
      })
    }

    measure(s"INSERT $TEST_COLLECTIONS_SIZE collections with $TEST_COLLECTIONS_ITEMS_SIZE items") { c ⇒
      val prefix = "test-collection/"
      parallel(
        randomCollectionItems.flatMap { item ⇒
          randomCollectionKeys.map { key ⇒
            val collection = prefix + key + "~"

            val path = collection + "/" + item.dynamic.key.toString
            retryWithPause(hyperbus
              .ask(ContentPut(path, DynamicBody(item))),
              20,
              150.milliseconds
            )
              .materialize
              .map {
                case Success(_) ⇒ c.success.increment()
                case Failure(ex) ⇒
                  c.fail.increment()
                  logErrorThrottled(ex)
              }
          }
        }
      )
    }

    measure(s"SELECT WITH PAGING $TEST_COLLECTIONS_SIZE collections") { c ⇒
      val prefix = "test-collection/"
      parallel(randomCollectionKeys.map { key ⇒
        val collection = prefix + key + "~"
        hyperbus.ask(
          ContentGet(collection)
        ).materialize.flatMap {
          case Success(response) ⇒
            selectCollection(collection, c, Task.now(response))

          case Failure(ex) ⇒
            c.fail.increment()
            logErrorThrottled(ex)
            Task.unit
        }
      })
    }

    def selectCollection(path: String, c: Counters, t: Task[Response[DynamicBody]]): Task[Response[DynamicBody]] = {
      t.flatMap {
        response ⇒
          if (response.body.content.isEmpty) {
            Task.now(response)
          } else if (response.headers.link.get("next_page_url").isEmpty) {
            c.success.increment(response.body.content.toList.size)
            Task.now(response)
          }
          else {
            import com.hypertino.binders.value._
            c.success.increment(response.body.content.toList.size)
            val get = ContentGet(
              path
            ).copyWithHeaders(
              Headers(Header.HRL → HRL(ContentGet.location, response.headers.link.get("next_page_url").map(_.query).getOrElse(Obj.empty)).toValue)
            )
            selectCollection(path, c,
              hyperbus.ask(get).asInstanceOf[Task[Response[DynamicBody]]]
            )
          }
      }
    }
  }

  private def parallel[T](tasks: Seq[Task[T]]): Task[_] = {
    val batches = tasks.sliding(PARALLELISM, PARALLELISM).map { batch: Seq[Task[T]] ⇒
      Task.gather(batch)
    }
    Task.sequence(batches).map(_.flatten.toList)
  }

  private def measure(name: String)(block: Counters ⇒ Task[_])(implicit scheduler: Scheduler): Unit = {
    logger.info(s"---------------------------------- Measuring: $name")
    val before = System.currentTimeMillis()
    val c = Counters(AtomicLong(0), AtomicLong(0))
    val f = block(c).runAsync
    while (!f.isCompleted) {
      Thread.sleep(1000)
      logger.info(s"Successful: ${c.success.get}, failed: ${c.fail.get}")
    }
    val after = System.currentTimeMillis()
    logger.info(s"$name completed in ${(after - before).toDouble / 1000.0d}s.")
    logger.info(s"TOTAL Successful: ${c.success.get}, failed: ${c.fail.get}.")
    logger.info(s"${c.success.get.toDouble / ((after - before) / 1000.0d)} units/seq")
  }

  private def nextRandomObj() = Obj.from(
    "key" → random.alphanumeric.take(12).mkString,
    "a" → random.nextInt(),
    "b" → random.alphanumeric.take(32).mkString,
    "c" → random.alphanumeric.take(10 + random.nextInt(100)).mkString,
    "d" → random.nextDouble()
  )

  def retryWithPause[A](source: Task[A],
                        maxRetries: Int, firstDelay: FiniteDuration): Task[A] = {

    source.onErrorHandleWith {
      case ex: Exception =>
        if (maxRetries > 0)
          retryWithPause(source, maxRetries - 1, firstDelay)
            .delayExecution(firstDelay)
        else
          Task.raiseError(ex)
    }
  }
}
