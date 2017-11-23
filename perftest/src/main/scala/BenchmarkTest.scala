import com.hypertino.binders.value.{Lst, Obj.from, Value}
import com.hypertino.config.ConfigLoader
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{DynamicBody, QueryBuilder, Response}
import com.hypertino.hyperbus.model.utils.SortBy
import com.hypertino.hyperbus.transport.api.{TransportConfigurationLoader, TransportManager}
import com.hypertino.hyperstorage.api._
import org.scalameter._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

object BenchmarkTest {
  import ExecutionContext.Implicits.global
  val random = new Random()
  val waitDuration = 60.seconds
  val colname = "col-bench-test~"
  val config = ConfigLoader()
  val transportConfiguration = TransportConfigurationLoader.fromConfig(config)
  val transportManager = new TransportManager(transportConfiguration)
  val hyperbus = new Hyperbus(transportManager)


  def main(args: Array[String]): Unit = {
    Thread.sleep(10000)
    println("Selecting...")
    try {
      val lst = query(pageSize=1000000).body.content
      println("fetched: " + lst.toSeq.size)
    }
    catch {
      case e: Throwable ⇒
        println(e.toString)
    }

    val fi = hyperbus <~ IndexPost(colname, HyperStorageIndexNew(Some("index2"),
      Seq(HyperStorageIndexSortItem("d", order = Some("desc"), fieldType = Some("text"))), Some("a > 5 and a < 60000000")))
    wf(fi)

    wf(hyperbus.shutdown(waitDuration))
    System.exit(0)

    try {
      val itemCount = 200
      println(s"Inserting $itemCount items sequentially!")
      val insertTime = measure {
        0 to itemCount map (_ ⇒ insert(random.alphanumeric.take(26).mkString, nextRandomObj()))
      }
      println(s"Total time to insert $itemCount items: $insertTime")

//      println(s"Inserting $itemCount items parallel!")
//      val insertTimeParallel = measure {
//        val futures = 0 to itemCount map { _ ⇒
//          val id = random.alphanumeric.take(26).mkString
//          val obj = nextRandomObj()
//          val f = hyperbus <~ HyperStorageContentPut(s"$colname/$id", DynamicBody(obj))
//          f
//        }
//        wf(Future.sequence(futures))
//      }
//      println(s"Total time to insert $itemCount items: $insertTimeParallel")

      val queryCount = 10
      val queryTime = measure {
        0 to queryCount map (_ ⇒
          query()
          )
      }

      println(s"Total time to query $itemCount items: $queryTime")

      try {
        val lst = query(pageSize=1000000).body.content
        println("fetched: " + lst.toSeq.size)
      }
      catch {
        case e: Throwable ⇒
          println(e.toString)
      }
    }
    catch {
      case e: Throwable ⇒
        println(e.toString)
    }
    wf(hyperbus.shutdown(waitDuration))
    System.exit(0)
  }

  def nextRandomObj() = Obj.from(
    "a" → random.nextInt(),
    "b" → random.alphanumeric.take(32).mkString,
    "c" → random.alphanumeric.take(10 + random.nextInt(100)).mkString,
    "d" → random.nextDouble()
  )

  def insert(id: String, content: Value) = {
    val f = hyperbus <~ ContentPut(s"$colname/$id", DynamicBody(content))
    wf(f)
  }

  def query(sort: Seq[SortBy] = Seq.empty, filter: Option[String] = None, pageSize: Int = 50): Response[DynamicBody] = {
    import com.hypertino.hyperbus.model.utils.Sort._
    val qb = new QueryBuilder() add("per_page", pageSize)
    if (sort.nonEmpty) qb.sortBy(sort)
    filter.foreach(qb.add("filter", _))

    val f = hyperbus <~ ContentGet(colname,
      body = qb.result()
    )
    wf(f)
  }

  def wf[T](f: Future[T]) = Await.result(f, waitDuration)
}
