package dynpro9

import org.scalatest.FunSuite

class LongestComSubsTest extends FunSuite {
  test("testLcsM") {
    assert("tsitest".equals(LongestComSubs.lcsM("thisiaatest".toList, "testing123testing".toList).mkString))
  }

  test("testLcsM 2") {
    val ans = LongestComSubs.lcsM("ananas".toList, "banane".toList).mkString
    println(ans)
    assert("anane".equals(ans))
  }

  test("testLcs") {
    assert("anane".equals(LongestComSubs.lcsM("ananas".toList, "banane".toList).mkString))
  }
}
