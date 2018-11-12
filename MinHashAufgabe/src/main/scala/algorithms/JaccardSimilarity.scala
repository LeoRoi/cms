package algorithms

import scala.util.Random

object JaccardSimilarity {
  val randGen = new Random

  /*
   * Calculate the Jaccard Distance of two Sets
   */
  def calculateJaccardDistanceSet[T](set1: Set[T], set2: Set[T]): Double =
    set1.intersect(set2).size / set1.union(set2).size.toDouble

  /*
  * Calculate the Jaccard Distance of two Bags
  * the greater the number, the greater the similarity
  * 0 -> not similar, 1 -> same text
  */
  def calculateJaccardDistanceBag[T](bag1: Iterable[T], bag2: Iterable[T]): Double = {
    val bagDelta = bag2.toList.diff(bag1.toList)
    val intersection = bag1.toList.intersect(bag2.toList)
    val union = bag1.toList.union(bagDelta)

    intersection.size / union.size.toDouble
  }

  def calculateJaccardDistanceBag2[T](bag1: Iterable[T], bag2: Iterable[T]): Double = {
    def getIntersection(listOne: List[T], listTwo: List[T], acc: List[T]): List[T] = listOne match {
      case h :: t => {
        if (listTwo.contains(h)) {
          val listTwoWithoutHead = listTwo.diff(List(h))
          val listTwoIntersect = listTwo.diff(listTwoWithoutHead)
          getIntersection(t, listTwoWithoutHead, (acc :+ h) ::: listTwoIntersect)
        }
        else getIntersection(t, listTwo, acc)
      }
      case Nil => acc
    }

    val intersection = getIntersection(bag1.toList, bag2.toList, List())
    val union = bag1 ++ bag2

    intersection.size / union.size.toDouble
  }

  /*
   * Calculate an Array of Hash Functions
   * size = array size
   *
   * Each function of the array should have the following structure
   * h(x)= (m*x + b) mod c, where
   *    m is random integer
   *    b is a random integer
   *    c is the parameter nrHashFuns, that is passed in the signature of the method
   */

  def createHashFuntions(size: Integer, nrHashFuns: Int): Array[(Int => Int)] = {
    val out = new Array[Int => Int](size)

    for(i <- 0 until size) {
      val f: Int => Int = x => (randGen.nextInt() * x + randGen.nextInt()) % nrHashFuns
      out(i) = f
    }

    out
  }

  /*
   * Implement the MinHash algorithm presented in the lecture
   *
   * Input:
   * matrix: Document vectors (each column should corresponds to one document)
   * hFuns: Array of Hash-Functions
   *
   * Output:
   * Signature Matrix:
   * columns: Each column corresponds to one document
   * rows: Each row corresponds to one hash function
   */

  def minHash[T](matrix: Array[Array[T]], hFuns: Array[Int => Int]): Array[Array[Int]] = {
    val out = new Array[Array[Int]](matrix.length)
    val hashes = new Array[(Int, Int, Int)](hFuns.length)

    for (i <- out.indices) {
      out(i) = new Array[Int](matrix(i).length)
      for (j <- hashes.indices) {
        hashes(j) = (i, j, hFuns(j)(i))
      }
    }

    out.map(row => )

    out
  }

  /*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   *
   * Helper functions that are used in the tests
   *
   * +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   */

  def printMultipleSets(data: Array[Array[Int]]): Unit = {
    data.foreach(x => println(x.mkString(" ")))
  }

  def createRandomSetAsArray(nrElements: Int): Array[Int] = {
    val res = Array.tabulate(nrElements)(_ => 0)
    (for (i <- 0 to nrElements - 1) {

      if (randGen.nextFloat < 0.3) res(randGen.nextInt(nrElements - 1)) = 1
    })
    res
  }

  def transformArrayIntoSet(data: Array[Int]): Set[Int] = {

    (for (i <- 0 to data.size - 1 if (data(i) == 1)) yield i).toSet

  }

  def findNextPrim(x: Int): Int = {

    def isPrim(X: Int, i: Int, Max: Int): Boolean = {
      if (i >= Max) true
      else if (X % i == 0) false
      else isPrim(X, i + 1, Max)
    }

    if (isPrim(x, 2, math.sqrt(x).toInt + 1)) x
    else findNextPrim(x + 1)
  }

  def compareSignatures(sig1: Array[Int], sig2: Array[Int]): Int = {

    var res = 0
    for (i <- 0 to sig1.size - 1) {
      if (sig1(i) == sig2(i)) res = res + 1
    }
    res
  }

}