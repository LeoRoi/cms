package test

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import combinations._

@RunWith(classOf[JUnitRunner])
class BacktrackingSolutionTest extends FunSuite with BacktrackingSolution {

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

  //	test("Loesung des Problems mit neun Staedten"){
  //	  val x=calculateRoundTrip(List(0,1,2,3,4,5,6,7,8))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2226)
  //	}
  //
  //	test("Loesung des Problems mit zehn Staedten"){
  //	  val x=calculateRoundTrip(List(0,1,2,3,4,5,6,7,8,9))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2261)
  //	}
  //
  //	test("Loesung des Problems mit elf Staedten"){
  //	  val x=calculateRoundTrip(List(0,1,2,3,4,5,6,7,8,9,10))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2346)
  //	}
  //
  //	test("Loesung des Problems mit zwölf Staedten"){
  //	  val x=calculateRoundTrip(List(0,1,2,3,4,5,6,7,8,9,10,11))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2583)
  //	}
  //
  //	test("Loesung des Problems mit dreizehn Staedten"){
  //	  val x=calculateRoundTrip(List(0,1,2,3,4,5,6,7,8,9,10,11,12))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2763)
  //	}
  //
  //  test("Loesung des Problems mit fünfzehn Staedten"){
  //	  val x=calculateRoundTrip(List(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2261)
  //	}

  test("Suche Stadt mit minimalem Abstand") {
    val x1 = getMin(0, List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
    assert(x1 === 8)
    val x2_1 = getMin(20, List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 0, 19, 21))
    assert(x2_1 === 0)
    val x2_2 = getMin(20, List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 0, 19, 20, 21))
    assert(x2_2 === 20)
    val x3 = getMin(10, List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
    assert(x3 === 10)
    val x4 = getMin(7, List())
    assert(x4 === 0)
    val x5 = getMin(9, List(1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14))
    assert(x5 === 12)
  }

  test("Einfache Suche nach einem kurzen Weg") {
    val x = findShortWay(0, List(1, 2, 3, 4, 5, 6))
    assert(x === List(0, 4, 3, 5, 6, 2, 1))
  }

  test("Loesung des leeren Problems (mit Cut nach Abschätzung)") {
    val x = calculateRoundTripWithSimpleBacktracking(List())
    assert(x === List())
  }

  test("Loesung des Problems mit einer Stadt (mit Cut nach Abschätzung)") {
    val x = calculateRoundTripWithSimpleBacktracking(List(1))
    val y = calculateCycle(x)
    assert(x === List(1))
    assert(y === 0)
  }

  test("Loesung des Problems mit zwei Staedten (mit Cut nach Abschätzung)") {
    val x = calculateRoundTripWithSimpleBacktracking(List(1, 2))
    val y = calculateCycle(x)
    assert(y === 478)
  }

  test("Loesung des Problems mit fünf Staedten (mit Cut nach Abschätzung)") {
    val x = calculateRoundTripWithSimpleBacktracking(List(1, 2, 3, 4, 5))
    val y = calculateCycle(x)
    assert(y === 1793)
  }

  test("Loesung des Problems mit acht Staedten (mit Cut nach Abschätzung)") {
    val x = calculateRoundTripWithSimpleBacktracking(List(1, 2, 3, 4, 5, 6, 7, 8))
    val y = calculateCycle(x)
    assert(y === 2134)
  }

  //	test("Loesung des Problems mit neun Staedten (mit Cut nach Abschätzung)"){
  //	  val x=calculateRoundTripWithSimpleBacktracking(List(0,1,2,3,4,5,6,7,8))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2226)
  //	}
  //
  //	test("Loesung des Problems mit zehn Staedten (mit Cut nach Abschätzung)"){
  //	  val x=calculateRoundTripWithSimpleBacktracking(List(0,1,2,3,4,5,6,7,8,9))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2261)
  //	}
  //
  //	test("Loesung des Problems mit elf Staedten (mit Cut nach Abschätzung)"){
  //	  val x=calculateRoundTripWithSimpleBacktracking(List(0,1,2,3,4,5,6,7,8,9,10))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2346)
  //	}
  //
  //	test("Loesung des Problems mit zwölf Staedten (mit Cut nach Abschätzung)"){
  //	  val x=calculateRoundTripWithSimpleBacktracking(List(0,1,2,3,4,5,6,7,8,9,10,11))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2583)
  //	}
  //
  //	test("Loesung des Problems mit dreizehn Staedten (mit Cut nach Abschätzung)"){
  //	  val x=calculateRoundTripWithSimpleBacktracking(List(0,1,2,3,4,5,6,7,8,9,10,11,12))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2763)
  //	}
  //
  //  test("Loesung des Problems mit fünfzehn Staedten (mit Cut nach Abschätzung)"){
  //	  val x=calculateRoundTripWithSimpleBacktracking(List(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2989)
  //	}
  //
  //
  //  test("Loesung des Problems mit siebzehn Staedten (mit Cut nach Abschätzung)"){
  //	  val x=calculateRoundTripWithOptimizedBacktracking(List(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2989)
  //	}

  test("Loesung des leeren Problems (mit optimiertem Cut)") {
    val x = calculateRoundTripWithOptimizedBacktracking(List())
    assert(x === List())
  }

  test("Loesung des Problems mit einer Stadt (mit optimiertem Cut)") {
    val x = calculateRoundTripWithOptimizedBacktracking(List(1))
    val y = calculateCycle(x)
    assert(x === List(1))
    assert(y === 0)
  }

  test("Loesung des Problems mit zwei Staedten (mit optimiertem Cut)") {
    val x = calculateRoundTripWithOptimizedBacktracking(List(1, 2))
    val y = calculateCycle(x)
    assert(y === 478)
  }

  test("Loesung des Problems mit fünf Staedten (mit optimiertem Cut)") {
    val x = calculateRoundTripWithOptimizedBacktracking(List(1, 2, 3, 4, 5))
    val y = calculateCycle(x)
    assert(y === 1793)
  }

  test("Loesung des Problems mit acht Staedten (mit optimiertem Cut)") {
    val x = calculateRoundTripWithOptimizedBacktracking(List(1, 2, 3, 4, 5, 6, 7, 8))
    val y = calculateCycle(x)
    assert(y === 2134)
  }

  //	test("Loesung des Problems mit neun Staedten (mit optimiertem Cut)"){
  //	  val x=calculateRoundTripWithOptimizedBacktracking(List(0,1,2,3,4,5,6,7,8))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2226)
  //	}
  //
  //	test("Loesung des Problems mit zehn Staedten (mit optimiertem Cut)"){
  //	  val x=calculateRoundTripWithOptimizedBacktracking(List(0,1,2,3,4,5,6,7,8,9))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2261)
  //	}
  //
  //	test("Loesung des Problems mit elf Staedten (mit optimiertem Cut)"){
  //	  val x=calculateRoundTripWithOptimizedBacktracking(List(0,1,2,3,4,5,6,7,8,9,10))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2346)
  //	}
  //
  //	test("Loesung des Problems mit zwölf Staedten (mit optimiertem Cut)"){
  //	  val x=calculateRoundTripWithOptimizedBacktracking(List(0,1,2,3,4,5,6,7,8,9,10,11))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2583)
  //	}
  //
  //	test("Loesung des Problems mit dreizehn Staedten (mit optimiertem Cut)"){
  //	  val x=calculateRoundTripWithOptimizedBacktracking(List(0,1,2,3,4,5,6,7,8,9,10,11,12))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2763)
  //	}
  //
  //  test("Loesung des Problems mit fünfzehn Staedten (mit optimiertem Cut)"){
  //	  val x=calculateRoundTripWithOptimizedBacktracking(List(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===2989)
  //	}
  //
  //  test("Loesung des Problems mit siebzehn Staedten (mit optimiertem Cut)"){
  //	  val x=calculateRoundTripWithOptimizedBacktracking(List(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16))
  //	  val y=calculateDistanceL(x)
  //	  assert(y===3119)
  //	}
}
