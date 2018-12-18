package combinations

trait BacktrackingSolution extends TravellingSalesman {

  /** **********************************************
    * CalculateRoundTrip should find a shortest path
    * by creating an enumeration tree using recursions
    *
    * Input: List of cities represented by a number
    * Output: One shortest path (zum next node)
    * ***********************************************/
  def calculateRoundTrip(list: List[Int]): List[Int] = {
//   tree dfs
//    opt
    ???
  }

  /** **********************************************
    * getMin should find the nearest city
    *
    * Input:
    *   1 - The start point
    *   2 - List of cities
    * Output: One shortest path
    * ***********************************************/
  def getMin(city: Int, cities: List[Int]): Int = ???


  /** **********************************************
    * findShortWay should find a short round trip
    * must not be the shortest path
    *
    * Input: List of cities
    * Output: A short roundtrip
    * ***********************************************/
  def findShortWay(start: Int, nodes: List[Int]): List[Int] = ???

  /** **********************************************
    * CalculateRoundTripSimpleBacktracking should find a shortest path
    * by using backtracking. The tree need to be cut
    * when the length of the subtree exceeds the
    * determined short way
    *
    * Input: List of cities represented by a number
    * Output: One shortest path
    * ***********************************************/
  def calculateRoundTripWithSimpleBacktracking(list: List[Int]): List[Int] = ???

  /** **********************************************
    * CalculateRoundTripOptimizedBacktracking should find a shortest path
    * by using backtracking. The tree need to be cut at least
    * when the length of the subtree exceeds the
    * determined short way. If a shorter way is found,
    * the length should be used for the cut.
    *
    * Input: List of cities represented by a number
    * Output: One shortest path
    * ***********************************************/
  def calculateRoundTripWithOptimizedBacktracking(list: List[Int]): List[Int] = ???
}