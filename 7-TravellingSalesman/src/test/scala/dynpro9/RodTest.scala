package dynpro9

import org.scalatest.FunSuite

class RodTest extends FunSuite {
  test("testCutRod") {
    assert(10 == Rod.topDownCutRod(4))
  }

  test("bottomUpCutRod") {
    assert(10 == Rod.bottomUpCutRod(4))
  }
}
