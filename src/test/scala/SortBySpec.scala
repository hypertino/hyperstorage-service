import com.hypertino.hyperbus.model.HRL
import com.hypertino.hyperstorage.utils.{Sort, SortBy}
import org.scalatest.{FlatSpec, FreeSpec, Matchers}

class SortBySpec extends FlatSpec with Matchers {
  "Sort" should "decode" in {
    Sort.parseQueryParam(Some("field1")) should equal(Seq(SortBy("field1")))
    Sort.parseQueryParam(Some("-field1")) should equal(Seq(SortBy("field1", descending = true)))
    Sort.parseQueryParam(Some("field1,field2")) should equal(Seq(SortBy("field1"), SortBy("field2")))
    Sort.parseQueryParam(Some("-field1,+field2")) should equal(Seq(SortBy("field1", descending = true), SortBy("field2")))
    Sort.parseQueryParam(Some("")) should equal(Seq.empty)
    Sort.parseQueryParam(Some("   ")) should equal(Seq.empty)
    Sort.parseQueryParam(Some(" -field1  , +field2")) should equal(Seq(SortBy("field1", descending = true), SortBy("field2")))
  }

  //    "should decode from HRL" in {
  //      val q = HRL("hb://test?sort=-field1%2C%2Bfield2%2Cfield3")
  //      import Sort._
  //
  //      q.sortBy.map(_.toList) should equal(Some(List(SortBy("field1", descending = true),SortBy("field2"),SortBy("field3"))))
  //    }

  it should "encode to" in {
    Sort.generateQueryParam(Seq(SortBy("field1"))) should equal("field1")
    Sort.generateQueryParam(Seq(SortBy("field1"), SortBy("field2", descending = true))) should equal("field1,-field2")
  }

  //    "should encode implicitly to QueryBuilder" in {
  //      val q = new QueryBuilder()
  //      import Sort._
  //      q.sortBy(Seq(SortBy("field1"),SortBy("field2",descending = true)))
  //      q.result().toQueryString() should equal("sort=field1%2C-field2")
  //    }

}
