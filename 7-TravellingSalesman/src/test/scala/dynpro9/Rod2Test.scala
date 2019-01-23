package dynpro9

import org.scalatest.FunSuite

class Rod2Test extends FunSuite {
  test("testMemoizedCutRod") {
    assert(10 == Rod2.memoizedCutRod(4))
  }

  test("testRunCutRodNaive") {
    assert(10 == Rod2.runCutRodNaive(4))
  }

  test("testRunCutRodBottomUp") {
    assert(10 == Rod2.runCutRodBottomUp(4))
  }
}
