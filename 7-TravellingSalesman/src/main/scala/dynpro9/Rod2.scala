package dynpro9

/* Implements the naive recursive solution to the rod cutting problem,
 * where the running time is exponential in n (T(n) = 2^n)
 */
object Rod2 {
  val DEFAULT_REVENUE = -1
  val prices = Array(1, 5, 8, 9, 10, 17, 17, 20, 24, 30, 32, 35, 37, 40, 43, 45, 49, 50)

  def runCutRodNaive(rodLength: Int): Int = {
    if (rodLength == 0)
      return 0

    var maxRevenue = -1
    for (lngth <- 0 to rodLength - 1) {
      maxRevenue = math.max(maxRevenue,
        prices(lngth) + runCutRodNaive(rodLength - (lngth + 1)))
    }
    return maxRevenue
  }

  def memoizedCutRod(rodLength: Int): Int = {
    /* Implements the top down dynamic programming solution of the rod cutting
 * problem which has a running time of theta of n squared
 */
    def run(prices: Array[Int], rodLength: Int, revenues: Array[Int]): Int = {
      if (revenues(rodLength - 1) >= 0)
        return revenues(rodLength - 1)

      var maxRevenue = DEFAULT_REVENUE
      if (rodLength == 0)
        maxRevenue = 0
      else {
        for (lngth <- 0 to rodLength - 1) {

          /* Handle the case where rodLength goes bellow 0 */
          var snd = 0;
          if ((rodLength - (lngth + 1)) > 0)
            snd = run(prices, rodLength - (lngth + 1), revenues)

          maxRevenue = math.max(maxRevenue, prices(lngth) + snd)
        }
      }

      revenues.update(rodLength - 1, maxRevenue)
      return maxRevenue
    }

    var revenues = new Array[Int](rodLength)
    0 to rodLength - 1 foreach(i => revenues.update(i, DEFAULT_REVENUE))
    run(prices, rodLength, revenues)
  }


/* Implements the bottom up dynamic programming approach to the rod cutting
 * problem with a running time of theta of n squared due to the doubly nested
 * loops
 */
  def runCutRodBottomUp(rodLength: Int): Int = {
    var revenues = new Array[Int](rodLength + 1)
    revenues.update(0, 0)

    for (j <- 1 to rodLength) {
      var maxRevenue = DEFAULT_REVENUE
      for (i <- 1 to j) {
        maxRevenue = math.max(maxRevenue, prices(i - 1) + revenues(j - i))
      }
      revenues(j) = maxRevenue
    }

    return revenues(rodLength)
  }


/* Run with scalac -deprecation CutRod.scala && scala CutRod */
  def main(args: Array[String]) = {
    println(runCutRodNaive(prices.size))
    println(memoizedCutRod(prices.size))
    println(runCutRodBottomUp(prices.size))
  }
}
