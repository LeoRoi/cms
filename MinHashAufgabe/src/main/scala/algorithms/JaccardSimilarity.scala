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
  def createHashFunctions(size: Integer, nrHashFuns: Int): Array[Int => Int] = {
    val out = new Array[Int => Int](size)
    val verbose = false

    for(i <- out.indices) {
      val m = randGen.nextInt()
      val b = randGen.nextInt()
      val c = nrHashFuns

      val f: Int => Int = x => (m * x + b) % c
      out(i) = f
      if(verbose) println("(" + m + "*x + " + b + ") % " + c)
    }
    if(verbose) println("out.size = " + out.length)
    out
  }

  def createHashFunctionsG(size: Integer, nrHashFuns: Int): Array[Int => Int] = {
    val verbose = false

    val out = for(i <- 0 to nrHashFuns) yield {
      val m = randGen.nextInt()
      val b = randGen.nextInt()

      if(verbose) println("(" + m + "*x + " + b + ") % " + size)
      ((x: Int) => ((m*x + b) % size)): (Int => Int)
    }

    if(verbose) println("out.size = " + out.length)
    out.toArray
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
  def minHash2[T](matrix: Array[Array[T]], functions: Array[Int => Int]): Array[Array[Int]] = {
    val out = new Array[Array[Int]](functions.length) // 2 x ? (4)
    val hashes = Array.ofDim[Int](matrix.length, functions.length) // 5 x 2
    val verbose = true

    def calculateHashes(verbose: Boolean): Unit = {
      // row
      for (i <- matrix.indices) {
        // cell
        for (j <- functions.indices) {
          hashes(i)(j) = functions(j)(i)
          if(verbose) println(functions(j)(i))
        }
      }
    }

    def initSignatureMatrix(verbose: Boolean): Unit = {
      if(verbose) {
        println("initSignatureMatrix: out.size (row amount) = " + out.length)
      }

      // rows = number of hash functions
      for(i <- out.indices) {
        if(verbose) println("initSignatureMatrix: i = " + i)
        // keep number of cells like in origin
        out(i) = new Array[Int](matrix(i).length)
        // init each cell with max_int to imitate the infinity
        for(j <- out(i).indices)
          out(i)(j) = Integer.MAX_VALUE
      }
      if(verbose) println(out)
    }

    calculateHashes(false)
    initSignatureMatrix(verbose)

    // iterate over rows
    for(row <- matrix.indices) { //0-4
      // all hashes for this row
      for (hash <- hashes(row).indices) { //0-1
        // inspect all cells
        for (col <- matrix(0).indices) { //0-3
          val outValue = out(hash)(col)
          val hashValue = hashes(row)(hash)
          if (matrix(row)(col) == 1 && outValue > hashValue)
          out(hash)(col) = hashValue
        }
      }
    }

    out
  }

  def minHash[T](matrix: Array[Array[Int]], hFuns: Array[Int => Int]): Array[Array[Int]] = {

    val numOfRows = hFuns.length
    val numOfColumns = matrix(0).length

    val signatureMatrix = Array.ofDim[Int](numOfRows, numOfColumns) // initialize result matrix with 0
    for ((row, i) <- signatureMatrix.zipWithIndex) {
      for ((column, j) <- row.zipWithIndex) {
        signatureMatrix(i)(j) = -1 // set all elements to -1
      }
    }

    for ((row, i) <- matrix.zipWithIndex) { // loop throw row -> hash -> columns. Applying hash to s1-s4 columns then go to next hash then agaon s1-s4 then do with the second row
      for ((h, k) <- hFuns.zipWithIndex) {
        for ((column, j) <- row.zipWithIndex) {
          val hash = h(i)

          //          val temp0 = matrix(i)(j)
          if (matrix(i)(j) == 1) {
            if (hash <= matrix(i)(j) || signatureMatrix(k)(j) == -1) {
              signatureMatrix(k)(j) = hash
            }
          }
          //          val temp1 = 0
        }
      }
    }

    signatureMatrix
  }

  def minHashG[T](matrix: Array[Array[Int]], hFuns: Array[Int => Int]): Array[Array[Int]] = {

    val numOfRows = hFuns.length
    val numOfColumns = matrix(0).length

    val signatureMatrix = Array.ofDim[Int](numOfRows, numOfColumns) // initialize result matrix with 0
    for ((row, i) <- signatureMatrix.zipWithIndex) {
      for ((column, j) <- row.zipWithIndex) {
        signatureMatrix(i)(j) = -1 // set all elements to -1
      }
    }

    for ((row, i) <- matrix.zipWithIndex) { // loop throw row -> hash -> columns. Applying hash to s1-s4 columns then go to next hash then agaon s1-s4 then do with the second row
      for ((h, k) <- hFuns.zipWithIndex) {
        for ((column, j) <- row.zipWithIndex) {
          val hash = h(i)

          //          val temp0 = matrix(i)(j)
          if (matrix(i)(j) == 1) {
            if (hash <= matrix(i)(j) || signatureMatrix(k)(j) == -1) {
              signatureMatrix(k)(j) = hash
            }
          }
          //          val temp1 = 0
        }
      }
    }

    signatureMatrix
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
    for (i <- 0 until nrElements)
      if (randGen.nextFloat < 0.3) res(randGen.nextInt(nrElements - 1)) = 1

    res
  }

  def transformArrayIntoSet(data: Array[Int]): Set[Int] =
    (for (i <- 0 to data.size - 1 if (data(i) == 1)) yield i).toSet

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

  // interesting concept, but not for this context
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
}