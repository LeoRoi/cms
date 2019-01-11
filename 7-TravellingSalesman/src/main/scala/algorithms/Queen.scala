package algorithms

object Queen extends App {
  def isSafe(placed: List[Int], pos: Int) = {
    val count = placed.length

    def threaten(A: (Int, Int)): Boolean =
      ((A._1 == pos) ||
        (A._1 + A._2 == pos + count) ||
        (A._2 - A._1 == count - pos))

    val places = placed.zip(0 until count)
    places.forall((Y) => (threaten(Y) == false))
  }

  def nQueensProblem(N: Int) = {
    def placeQueen(K: Int): List[List[Int]] = {
      if (K == 0) List(Nil)
      else for {
        placed <- placeQueen(K - 1)
        newrow <- (0 until N)
        if isSafe(placed, newrow)
      } yield placed ++ List(newrow)
    }

    val ans = placeQueen(N)
    println("nQueenProblem solutions found with n=" + N + ": " + ans.size + "\n")
    ans
  }

  /*
  8 - 92
  9 - 352
  10 - 724
  */
  println(nQueensProblem(1))
}
