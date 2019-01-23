package algorithms

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import combinations._

@RunWith(classOf[JUnitRunner])
class PermutationSolutionTest extends FunSuite with PermutationSolution {

  test("Loesung des leeren Problems") {
    val x = calculateRoundTrip(List())
    assert(x === List())
  }

  test("Loesung des Problems mit einer Stadt") {
    val x = calculateRoundTrip(List(1))
    val y = calculateCycle(x)
    assert(x === List(1))
    assert(y === 0)
  }

  test("Loesung des Problems mit zwei Staedten") {
    val x = calculateRoundTrip(List(1, 2))
    val y = calculateCycle(x)
    assert(y === 478)
  }

  test("Loesung des Problems mit fünf Staedten") {
    val x = calculateRoundTrip(List(1, 2, 3, 4, 5))
    val y = calculateCycle(x)
    assert(y === 1793)
  }

  test("Loesung des Problems mit acht Staedten") {
    val x = calculateRoundTrip(List(1, 2, 3, 4, 5, 6, 7, 8))
    val y = calculateCycle(x)
    assert(y === 2134)
  }

  //1s
  test("Loesung des Problems mit neun Staedten") {
    val x = calculateRoundTrip(List(0, 1, 2, 3, 4, 5, 6, 7, 8))
    val y = calculateCycle(x)
    assert(y === 2226)
  }

  /** ************************
    * foldLeft Implementation
    * *************************/
  test("Loesung des Problems mit acht Staedten (foldLeft)") {
    val x = calculateRoundTripFoldLeft(List(1, 2, 3, 4, 5, 6, 7, 8))
    assert(x._2 === 2134)
  }

  test("Loesung des Problems mit neun Staedten (foldLeft)") {
    val x = calculateRoundTripFoldLeft(List(0, 1, 2, 3, 4, 5, 6, 7, 8))
    assert(x._2 === 2226)
  }

  //2s
  test("Loesung des Problems mit zehn Staedten (foldLeft)") {
    val x = calculateRoundTripFoldLeft(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    assert(x._2 === 2261)
  }

  /** ****************************
    * foldRight Implementation
    * ******************************/
  test("Loesung des Problems mit acht Staedten (foldRight)") {
    val x = calculateRoundTripFoldRight(List(1, 2, 3, 4, 5, 6, 7, 8))
    val y = calculateCycle(x)
    assert(y === 2134)
  }

  test("Loesung des Problems mit neun Staedten (foldRight)") {
    val x = calculateRoundTripFoldRight(List(0, 1, 2, 3, 4, 5, 6, 7, 8))
    val y = calculateCycle(x)
    assert(y === 2226)
  }

  /** *********************
    * fold Implementation
    * **********************/
  test("Loesung des Problems mit acht Staedten (fold)") {
    val x = calculateRoundTripFold(List(1, 2, 3, 4, 5, 6, 7, 8))
    val y = calculateCycle(x)
    assert(y === 2134)
  }

  test("Loesung des Problems mit neun Staedten (fold)") {
    val x = calculateRoundTripFold(List(0, 1, 2, 3, 4, 5, 6, 7, 8))
    val y = calculateCycle(x)
    assert(y === 2226)
  }

  test("Loesung des Problems mit zehn Staedten (fold)") {
    val x = calculateRoundTripFold(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    val y = calculateCycle(x)
    assert(y === 2261)
  }

  /** *********************
    * takes too much time
    * **********************/
  /*
  //19s
  test("Loesung des Problems mit zehn Staedten") {
    val x = calculateRoundTrip(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    val y = calculateDistanceL(x)
    assert(y === 2261)
  }

  test("Loesung des Problems mit elf Staedten") {
    val x = calculateRoundTrip(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val y = calculateDistanceL(x)
    assert(y === 2346)
  }

  //17s
  test("Loesung des Problems mit elf Staedten (foldLeft)") {
    val x = calculateRoundTripFoldLeft(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val y = calculateDistanceL(x)
    assert(y === 2346)
  }

  //4min
  test("Loesung des Problems mit zwölf Staedten (foldLeft)") {
    val x = calculateRoundTripFoldLeft(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
    val y = calculateDistanceL(x)
    assert(y === 2583)
  }

  test("Loesung des Problems mit dreizehn Staedten (foldLeft)") {
    val x = calculateRoundTripFoldLeft(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
    val y = calculateDistanceL(x)
    assert(y === 2763)
  }

  test("Loesung des Problems mit vierzehn Staedten (foldLeft)") {
    val x = calculateRoundTripFoldLeft(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
    val y = calculateDistanceL(x)
    assert(y === 2261)
  }

  //17s
  test("Loesung des Problems mit zehn Staedten (foldRight)") {
    val x = calculateRoundTripFoldRight(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    val y = calculateDistanceL(x)
    assert(y === 2261)
  }

  test("Loesung des Problems mit elf Staedten (foldRight)") {
    val x = calculateRoundTripFoldRight(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val y = calculateDistanceL(x)
    assert(y === 2346)
  }

  test("Loesung des Problems mit zwoelf Staedten (foldRight)") {
    val x = calculateRoundTripFoldRight(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
    val y = calculateDistanceL(x)
    assert(y === 2583)
  }

  //18s
  test("Loesung des Problems mit elf Staedten (fold)") {
    val x = calculateRoundTripFold(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val y = calculateDistanceL(x)
    assert(y === 2346)
  }

  //3min
    test("Loesung des Problems mit zwoelf Staedten (fold)"){
      val x=calculateRoundTripFold(List(0,1,2,3,4,5,6,7,8,9,10,11))
      val y=calculateDistanceL(x)
      assert(y===2583)
    }

    test("Loesung des Problems mit dreizehn Staedten (fold)"){
      val x=calculateRoundTripFold(List(0,1,2,3,4,5,6,7,8,9,10,11,12))
      val y=calculateDistanceL(x)
      assert(y===2763)
    }
  */
}
