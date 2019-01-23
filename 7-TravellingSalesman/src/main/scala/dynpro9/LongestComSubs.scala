package dynpro9

object LongestComSubs extends App {

  /*
  LCS-Length (X,Y)
  m=X.laenge; n= Y.laenge;
  Seien d[1..m,1..n] und c[1..m,1..n] neue Tabellen
  FOR i=1 TO m DO d[i,0]= 0
  FOR j=0 TO n DO d[0,j]= 0
  FOR i=1 TO m DO
  FOR j=1 TO n DO
  IF x i = y j THEN
  d[i,j] = d[i-1,j-1]+1
  c[i,j] = “ “
  ELSEIF d[i-1,j] ≥ d[i,j-1]
  d[i,j]= d[i-1,j]
  c[i,j]= “↓“
  ELSE c[i,j] = c[i,j-1]
  b[i,j]= “→“
  RETURN c und d
   */
  //dynamic
  case class Memoized[A1, A2, B](f: (A1, A2) => B) extends ((A1, A2) => B) {
    val cache = scala.collection.mutable.Map.empty[(A1, A2), B]

    def apply(x: A1, y: A2) = cache.getOrElseUpdate((x, y), f(x, y))
  }

  lazy val lcsM: Memoized[List[Char], List[Char], List[Char]] = Memoized {
    case (_, Nil) => Nil
    case (Nil, _) => Nil
    case (x :: xs, y :: ys) if x == y => x :: lcsM(xs, ys)
    case (x :: xs, y :: ys) => {
      (lcsM(x :: xs, ys), lcsM(xs, y :: ys)) match {
        case (xs, ys) if xs.length > ys.length => xs
        case (xs, ys) => ys
      }
    }
  }

  //recursive
  def lcs[T]: (List[T], List[T]) => List[T] = {
    case (_, Nil) => Nil
    case (Nil, _) => Nil
    case (x :: xs, y :: ys) if x == y => x :: lcs(xs, ys)
    case (x :: xs, y :: ys) => {
      (lcs(x :: xs, ys), lcs(xs, y :: ys)) match {
        case (xs, ys) if xs.length > ys.length => xs
        case (xs, ys) => ys
      }
    }
  }
}