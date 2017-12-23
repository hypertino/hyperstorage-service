import com.hypertino.binders.value.{Lst, Obj, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.MessagingContext
import com.hypertino.hyperbus.transport.api.{ServiceRegistrator, ServiceResolver}
import com.hypertino.hyperbus.transport.registrators.DummyRegistrator
import com.hypertino.hyperstorage.api.ContentGet
import com.hypertino.service.config.ConfigModule
import com.hypertino.transport.resolvers.consul.ConsulServiceResolver
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import scaldi.{Injectable, Module}

class BenchmarkModule extends Module {
  bind[Scheduler] to monix.execution.Scheduler.Implicits.global
  bind[ServiceRegistrator] to DummyRegistrator
  bind[ServiceResolver] to new ConsulServiceResolver(
    inject[Config].getConfig("service-resolver")
  )(inject [Scheduler])
  bind[Hyperbus] to injected[Hyperbus]
}

object BenchmarkTest extends Injectable with StrictLogging {
//  val random = new Random()
//  val waitDuration = 60.seconds
//  val colname = "col-bench-test~"
//  val config = ConfigLoader()
//  val transportConfiguration = TransportConfigurationLoader.fromConfig(config)
//  val transportManager = new TransportManager(transportConfiguration)
//  val hyperbus = new Hyperbus(transportManager)


  def main(args: Array[String]): Unit = {
    implicit val mcx = MessagingContext.Implicits.emptyContext
    implicit val injector =
      new BenchmarkModule ::
        ConfigModule()
    implicit val scheduler = inject[Scheduler]
    val hyperbus = inject[Hyperbus]
    hyperbus
      .ask(ContentGet("abc"))
      .materialize
      .map { result ⇒
        println(result)
      }
  }
//    Thread.sleep(10000)
//    println("Selecting...")
//    try {
//      val lst = query(pageSize=1000000).body.content
//      println("fetched: " + lst.toSeq.size)
//    }
//    catch {
//      case e: Throwable ⇒
//        println(e.toString)
//    }
//
//    val fi = hyperbus <~ IndexPost(colname, HyperStorageIndexNew(Some("index2"),
//      Seq(HyperStorageIndexSortItem("d", order = Some("desc"), fieldType = Some("text"))), Some("a > 5 and a < 60000000")))
//    wf(fi)
//
//    wf(hyperbus.shutdown(waitDuration))
//    System.exit(0)
//
//    try {
//      val itemCount = 200
//      println(s"Inserting $itemCount items sequentially!")
//      val insertTime = measure {
//        0 to itemCount map (_ ⇒ insert(random.alphanumeric.take(26).mkString, nextRandomObj()))
//      }
//      println(s"Total time to insert $itemCount items: $insertTime")
//
////      println(s"Inserting $itemCount items parallel!")
////      val insertTimeParallel = measure {
////        val futures = 0 to itemCount map { _ ⇒
////          val id = random.alphanumeric.take(26).mkString
////          val obj = nextRandomObj()
////          val f = hyperbus <~ HyperStorageContentPut(s"$colname/$id", DynamicBody(obj))
////          f
////        }
////        wf(Future.sequence(futures))
////      }
////      println(s"Total time to insert $itemCount items: $insertTimeParallel")
//
//      val queryCount = 10
//      val queryTime = measure {
//        0 to queryCount map (_ ⇒
//          query()
//          )
//      }
//
//      println(s"Total time to query $itemCount items: $queryTime")
//
//      try {
//        val lst = query(pageSize=1000000).body.content
//        println("fetched: " + lst.toSeq.size)
//      }
//      catch {
//        case e: Throwable ⇒
//          println(e.toString)
//      }
//    }
//    catch {
//      case e: Throwable ⇒
//        println(e.toString)
//    }
//    wf(hyperbus.shutdown(waitDuration))
//    System.exit(0)
//  }
//
//  def nextRandomObj() = Obj.from(
//    "a" → random.nextInt(),
//    "b" → random.alphanumeric.take(32).mkString,
//    "c" → random.alphanumeric.take(10 + random.nextInt(100)).mkString,
//    "d" → random.nextDouble()
//  )
//
//  def insert(id: String, content: Value) = {
//    val f = hyperbus <~ ContentPut(s"$colname/$id", DynamicBody(content))
//    wf(f)
//  }
//
//  def query(sort: Seq[SortBy] = Seq.empty, filter: Option[String] = None, pageSize: Int = 50): Response[DynamicBody] = {
//    import com.hypertino.hyperbus.model.utils.Sort._
//    val qb = new QueryBuilder() add("per_page", pageSize)
//    if (sort.nonEmpty) qb.sortBy(sort)
//    filter.foreach(qb.add("filter", _))
//
//    val f = hyperbus <~ ContentGet(colname,
//      body = qb.result()
//    )
//    wf(f)
//  }
//
//  def wf[T](f: Future[T]) = Await.result(f, waitDuration)
}
