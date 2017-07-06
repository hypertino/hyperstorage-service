import com.hypertino.binders.value.{Obj, Text, Value}
import com.hypertino.hyperstorage.CollectionOrdering
import com.hypertino.hyperstorage.utils.SortBy
import org.scalatest.{FreeSpec, Matchers}


class CollectionOrderingTest extends FreeSpec with Matchers {
  "CollectionOrdering" - {
    val c1 = Obj.from("a" → "hello", "b" → 100500, "c" → 10)
    val c1x = Obj.from(c1.toMap.toSeq ++ Seq("id" → Text("item1")): _*)
    val c2 = Obj.from("a" → "goodbye", "b" → 1, "c" → 20)
    val c2x = Obj.from(c2.toMap.toSeq ++ Seq("id" → Text("item2")): _*)
    val c3 = Obj.from("a" → "way way", "b" → 12, "c" → 10)
    val c3x = Obj.from(c3.toMap.toSeq ++ Seq("id" → Text("item3")): _*)

    "sort" - {
      val list = List(c1x,c2x,c3x)
      val ordering = new CollectionOrdering(Seq(SortBy("a")))
      list.sorted(ordering) shouldBe List(c2x,c1x,c3x)
    }

    "sort descending" - {
      val list = List(c1x,c2x,c3x)
      val ordering = new CollectionOrdering(Seq(SortBy("a", descending=true)))
      list.sorted(ordering) shouldBe List(c3x,c1x,c2x)
    }

    "sort two fields" - {
      val list = List(c1x,c2x,c3x)
      val ordering = new CollectionOrdering(Seq(SortBy("c"), SortBy("a")))
      list.sorted(ordering) shouldBe List(c1x,c3x,c2x)
    }

    "sort descending two fields" - {
      val list: List[Value] = List(c1x,c2x,c3x)
      implicit val ordering = new CollectionOrdering(Seq(SortBy("c", descending=true), SortBy("a")))
      list.sorted shouldBe List(c2x,c1x,c3x)
    }
  }
}
