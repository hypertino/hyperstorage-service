import com.hypertino.hyperstorage.utils.BiMap
import org.scalatest.{FlatSpec, Matchers}

class BiMapSpec extends FlatSpec with Matchers {
  "BiMap" should "map in both ways" in {
    val m = BiMap("a" → 1, "b" → 2)
    m("a") shouldBe 1
    m("b") shouldBe 2
    m.inverse(1) shouldBe "a"
    m.inverse(2) shouldBe "b"
  }

  it should "remove in both" in {
    val m = BiMap("a" → 1, "b" → 2)
    (m - "b").get("b") shouldBe None
  }

  it should "add both" in {
    val m = BiMap("a" → 1, "b" → 2)
    val m2 = m + ("c" → 3)
    m2("c") shouldBe 3
  }
}
