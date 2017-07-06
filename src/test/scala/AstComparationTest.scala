import com.hypertino.binders.{value ⇒ bn}
import com.hypertino.hyperstorage.indexing.AstComparator
import com.hypertino.parser.HParser
import org.scalatest.{FreeSpec, Matchers}
import com.hypertino.hyperstorage.indexing.AstComparation._

class AstComparationTest extends FreeSpec with Matchers {


  "AstComparation" - {
    "Expression can be equal" in {
      AstComparator.compare(HParser("x + 5"), HParser("x + 5")) shouldBe Equal
      AstComparator.compare(HParser("x > 4"), HParser("x > 4")) shouldBe Equal
    }

    "Expression can be not equal" in {
      AstComparator.compare(HParser("x + y"), HParser("x + 5")) shouldBe NotEqual
      AstComparator.compare(HParser("x > 4"), HParser("x > 3")) shouldBe NotEqual
    }

    "Expression can be wider for `> <`" in {
      AstComparator.compare(HParser("x > 4"), HParser("x > 5")) shouldBe Wider
      AstComparator.compare(HParser("x >= 4"), HParser("x >= 5")) shouldBe Wider
      AstComparator.compare(HParser("x < 3"), HParser("x < 2")) shouldBe Wider
      AstComparator.compare(HParser("x <= 4"), HParser("x <= 1")) shouldBe Wider
      AstComparator.compare(HParser("x*4 <= 4"), HParser("x*4 <= 1")) shouldBe Wider
    }

    "Expression can be wider for `has / not`" in {
      AstComparator.compare(HParser("x has [1,2,3]"), HParser("x has [1,2]")) shouldBe Wider
      AstComparator.compare(HParser("x has not [1,2,3]"), HParser("x has not [1,2,3,4]")) shouldBe Wider
    }

    "Expression can be wider for `or`" in {
      AstComparator.compare(HParser("x > 5 or y < 2"), HParser("x > 5")) shouldBe Wider
      AstComparator.compare(HParser("x has [5,1,3,4] or y < 2"), HParser("x has [5,1,3]")) shouldBe Wider
      AstComparator.compare(HParser("x or y < 2"), HParser("y < 2")) shouldBe Wider
      AstComparator.compare(HParser("x or y"), HParser("y")) shouldBe Wider
    }

    "Expression can be wider for `and`" in {
      AstComparator.compare(HParser("x"), HParser("x and y = 20")) shouldBe Wider
      AstComparator.compare(HParser("x > 5"), HParser("x > 5 and y = 20")) shouldBe Wider
      AstComparator.compare(HParser("x"), HParser("x and y")) shouldBe Wider
      AstComparator.compare(HParser("x"), HParser("x+1 and y")) shouldBe NotEqual
    }
  }
}
