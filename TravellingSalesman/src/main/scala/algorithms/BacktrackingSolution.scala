package combinations

trait BacktrackingSolution extends TravellingSalesman {
  def getMin(city: Int, otherCities: List[Int]): Int = {
    if (otherCities == Nil) 0
    else otherCities.foldLeft(otherCities.head)((acc, e) =>
      if (abstand(city)(acc) < abstand(city)(e)) acc else e)
  }

  /** **********************************************
    * findShortWay should find a short round trip
    * must not be the shortest path
    *
    * Input: List of cities
    * Output: A short roundtrip
    * ***********************************************/
  def findShortWay(start: Int, cities: List[Int]): List[Int] = {
    if (cities == Nil) List(start)
    else {
      val next = getMin(start, cities)
      start :: findShortWay(next, cities.diff(List(next)))
    }
  }

  /** **********************************************
    * CalculateRoundTripSimpleBacktracking should find a shortest path
    * by using backtracking. The tree need to be cut
    * when the length of the subtree exceeds the
    * determined short way
    *
    * Input: List of cities represented by a number
    * Output: One shortest path
    * ***********************************************/
  def calculateRoundTripWithSimpleBacktracking(list: List[Int]): List[Int] = {
    var estimation = Integer.MAX_VALUE

    def tree2(visited: List[Int], notVisited: List[Int], distance: Int): (List[Int], Int) = notVisited match {
      case Nil => (visited, distance + abstand(visited.last)(visited.head))
      case next =>
        if (distance > estimation) (List(), estimation)
        else {
          val ways: List[(List[Int], Int)] = {
            for {
              city <- notVisited
              val newDistance = distance + abstand(visited.head)(city)
              val next = tree(city :: visited, notVisited.filter(x => (x != city)), newDistance)
            } yield next
          }
          ways.foldLeft(List(): List[Int], Integer.MAX_VALUE)((acc, e) =>
            if (acc._2 < e._2) acc else e)
        }
    }

    def tree(visited: List[Int], notVisited: List[Int], km: Int): (List[Int], Int) = notVisited match {
      case Nil => (visited, km + abstand(visited.last)(visited.head))
      case rest => {
        val ways: List[(List[Int], Int)] = {
          for {
            city <- notVisited
            val newKm = km + abstand(visited.head)(city)
            val nextSol = tree(city :: visited, notVisited.diff(List(city)), newKm)
          } yield nextSol
        }
        ways.foldLeft(List(): List[Int], Integer.MAX_VALUE)((opt, e) =>
          if(opt._2 < e._2) opt else e)
      }
    }

    if (list.isEmpty) List()
    else {
      estimation = calculateCycle(findShortWay(list.head, list.tail))
      tree(List(list.head), list.tail, 0)._1
    }
  }

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
  def calculateRoundTripWithOptimizedBacktracking(list: List[Int]): List[Int] = {
    def tree(visited: List[Int], notVisited: List[Int], km: Int, optKm: Int, optWay: List[Int]): (List[Int], Int) = notVisited match {
      case Nil => (visited, km + abstand(visited.last)(visited.head))
      case rest => {
        val ways = notVisited.map(city => (city :: visited, notVisited.diff(List(city)), km + abstand(visited.head)(city)))

        ways.foldLeft(optWay, optKm)((opt, next) =>
          if (opt._2 < next._3) opt
          else {
            val newWay = tree(next._1, next._2, next._3, optKm, optWay)
            if (newWay._2 < opt._2) newWay else (opt._1, opt._2)
          })
      }
    }

    if (list.isEmpty) List()
    else tree(List(list.head), list.tail, 0, Integer.MAX_VALUE, List())._1
  }
}