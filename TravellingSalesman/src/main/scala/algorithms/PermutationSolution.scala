package combinations

trait PermutationSolution extends TravellingSalesman {

  def calculateRoundTrip(list: List[Int]): List[Int] = {
    def findOpt(opt: List[Int], kmOpt: Int, list: List[List[Int]]): List[Int] = list match {
      case x :: xs => {
        val km = calculateDistanceL(x)
        if (km < kmOpt) findOpt(x, km, xs)
        else findOpt(opt, kmOpt, xs)
      }
      case Nil => opt
    }

    if (list == List()) List()
    else if (list.length == 1) list
    else {
      val perms = permut(list)
      val (x :: xs) = perms
      findOpt(perms(0), calculateDistanceL(perms(0)), xs)
    }
  }

  def permut(L: List[Int]): List[List[Int]] = L match {
    case Nil => List(Nil)
    case _ => for {j <- L
                   k <- permut(L.diff(List(j)))
                   erg = j}
      yield erg :: k
  }

  def calculateRoundTripFoldLeft(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case list => val (way, km) = list.permutations.map((X) => (X, calculateDistanceL(X))).
      foldLeft(List(): List[Int], Integer.MAX_VALUE)((X, Y) => if (X._2 < Y._2) X else Y)
      way
  }

  def calculateRoundTripFoldRight(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case list => val (way, km) = list.permutations.map((X) => (X, calculateDistanceL(X))).
      foldRight(List(): List[Int], 2147483647)((X, Y) => if (X._2 < Y._2) X else Y)
      way
  }

  def calculateRoundTripFold(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case list => val (way, km) = list.permutations.map((X) => (X, calculateDistanceL(X))).
      fold(List(): List[Int], 2147483647)((X, Y) => if (X._2 < Y._2) X else Y)
      way
  }
}
