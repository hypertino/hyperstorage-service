import com.hypertino.binders.value.{Lst, Obj, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{DynamicBody, MessagingContext}
import com.hypertino.hyperbus.transport.api.{ServiceRegistrator, ServiceResolver}
import com.hypertino.hyperbus.transport.registrators.DummyRegistrator
import com.hypertino.hyperstorage.api.{ContentGet, ContentPut}
import com.hypertino.service.config.ConfigModule
import com.hypertino.transport.resolvers.consul.ConsulServiceResolver
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicLong
import scaldi.{Injectable, Module}

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
  private val TEST_SIZE = 10000
  private val PARALLELISM = 32
  private val randomObjs = 0 until TEST_SIZE map { _ ⇒ nextRandomObj() }

  def main(args: Array[String]): Unit = {
    implicit val mcx = MessagingContext.Implicits.emptyContext
    implicit val injector =
      new ServiceResolverModule ::
      new BenchmarkModule ::
        ConfigModule()
    implicit val scheduler = inject[Scheduler]
    val hyperbus = inject[Hyperbus]
    hyperbus.startServices()

    measure(s"PUT $TEST_SIZE objects") { c ⇒
      val prefix = "test-objects/"
      parallel(randomObjs.map { obj ⇒
        val path = prefix + obj.dynamic.key.toString
        hyperbus
          .ask(ContentPut(path, DynamicBody(obj)))
          .materialize
          .map {
            case Success(_) ⇒ c.success.increment()
            case Failure(_) ⇒ c.fail.increment()
          }
      })
    }

    measure(s"GET $TEST_SIZE objects") { c ⇒
      val prefix = "test-objects/"
      parallel(randomObjs.map { obj ⇒
        val path = prefix + obj.dynamic.key.toString
        hyperbus
          .ask(ContentGet(path))
          .materialize
          .map {
            case Success(_) ⇒ c.success.increment()
            case Failure(_) ⇒ c.fail.increment()
          }
      })
    }
  }

  private def parallel[T](tasks: Seq[Task[T]]): Task[_] ={
    val batches = tasks.sliding(PARALLELISM,PARALLELISM).map { batch: Seq[Task[T]] ⇒
      Task.gather(batch)
    }
    Task.sequence(batches).map(_.flatten.toList)
  }

  private def measure(name: String)(block: Counters ⇒ Task[_])(implicit scheduler: Scheduler): Unit = {
    logger.info(s"---------------------------------- Measuring: $name")
    val before = System.currentTimeMillis()
    val c = Counters(AtomicLong(0), AtomicLong(0))
    val f = block(c).runAsync
    while(!f.isCompleted) {
      Thread.sleep(1000)
      logger.info(s"Successful: ${c.success.get}, failed: ${c.fail.get}")
    }
    val after = System.currentTimeMillis()
    logger.info(s"$name completed in ${(after-before).toDouble/1000.0d}s.")
    logger.info(s"TOTAL Successful: ${c.success.get}, failed: ${c.fail.get}.")
    logger.info(s"${c.success.get.toDouble/((after-before)/1000.0d)} units/seq")
  }

  private def nextRandomObj() = Obj.from(
    "key" → random.alphanumeric.take(12).mkString,
    "a" → random.nextInt(),
    "b" → random.alphanumeric.take(32).mkString,
    "c" → random.alphanumeric.take(10 + random.nextInt(100)).mkString,
    "d" → random.nextDouble()
  )
}
