import com.hypertino.binders.value.{Number, Text}
import com.hypertino.hyperstorage.db._
import com.hypertino.hyperstorage.indexing.FieldFilterMerger
import com.hypertino.parser.{HFormatter, HParser}
import org.scalatest.{FlatSpec, Matchers}

class FieldFilterMergerSpec extends FlatSpec with Matchers {
  "FieldFilterMerger" should "merge expression and ff seq" in {
    val expression = HParser(s""" x > 10 """)
    val ffseq = Seq(FieldFilter("y", Number(10), FilterGt))
    val r = FieldFilterMerger.merge(Some(expression), ffseq).map(HFormatter(_))
    r.get shouldBe "(y > 10) and (x > 10)"
  }

  it should "merge expression and replace existing >" in {
    val expression = HParser(s""" x > 10 and y > 5""")
    val ffseq = Seq(FieldFilter("y", Number(10), FilterGt))
    val r = FieldFilterMerger.merge(Some(expression), ffseq).map(HFormatter(_))
    r.get shouldBe "(x > 10) and (y > 10)"
  }

  it should "merge expression and replace existing <" in {
    val expression = HParser(s""" x > 10 and y < 5""")
    val ffseq = Seq(FieldFilter("y", Number(3), FilterLt))
    val r = FieldFilterMerger.merge(Some(expression), ffseq).map(HFormatter(_))
    r.get shouldBe "(x > 10) and (y < 3)"
  }

  it should "merge expression and replace existing >=" in {
    val expression = HParser(s""" x > 10 and y >= 5""")
    val ffseq = Seq(FieldFilter("y", Number(10), FilterGtEq))
    val r = FieldFilterMerger.merge(Some(expression), ffseq).map(HFormatter(_))
    r.get shouldBe "(x > 10) and (y >= 10)"
  }

  it should "merge expression and replace existing <=" in {
    val expression = HParser(s""" x > 10 and y <= 5""")
    val ffseq = Seq(FieldFilter("y", Number(3), FilterLtEq))
    val r = FieldFilterMerger.merge(Some(expression), ffseq).map(HFormatter(_))
    r.get shouldBe "(x > 10) and (y <= 3)"
  }

  it should "not merge expression and replace existing if op is different" in {
    val expression = HParser(s""" x > 10 and y < 5""")
    val ffseq = Seq(FieldFilter("y", Number(10), FilterGt))
    val r = FieldFilterMerger.merge(Some(expression), ffseq).map(HFormatter(_))
    r.get shouldBe "(y > 10) and ((x > 10) and (y < 5))"
  }
}
