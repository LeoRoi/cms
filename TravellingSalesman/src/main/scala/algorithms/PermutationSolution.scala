package combinations

trait PermutationSolution extends TravellingSalesman {
  def calculateRoundTripFoldLeft(list: List[Int]): (List[Int], Int) = list match {
    case Nil => null
    case list => list.permutations.map(X => (X, calculateCycle(X))).
      foldLeft(List(): List[Int], Integer.MAX_VALUE)((acc, e) => if (acc._2 < e._2) acc else e)
  }

  def calculateRoundTripFoldRight(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case list => val (way, km) = list.permutations.map((X) => (X, calculateCycle(X))).
      foldRight(List(): List[Int], 2147483647)((X, Y) => if (X._2 < Y._2) X else Y)
      way
  }

  def calculateRoundTripFold(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case list => val (way, km) = list.permutations.map((X) => (X, calculateCycle(X))).
      fold(List(): List[Int], 2147483647)((X, Y) => if (X._2 < Y._2) X else Y)
      way
  }
}
