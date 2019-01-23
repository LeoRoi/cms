package dynpro9

object Rod extends App {
  val prices = Map(1 -> 1, 2 -> 5, 3 -> 8, 4 -> 9, 5 -> 10,
    6 -> 17, 7 -> 17, 8 -> 20, 9 -> 24, 10 -> 30)

  /*
  Cut_Rod( p, n)
  IF n==0
  return 0
  q=-∞
  Erlös durch einen Schnitt
  FOR i=1 TO n
  q=max( q, p [i] + Cut_Rod(p,n-i))
  RETURN q
   */
  def topDownCutRod(n: Int): Int = {
    if (n == 0) 0
    else {
      var ans = -Integer.MAX_VALUE
      for (i <- 1 to n) {
        ans = Math.max(ans, prices(i) + topDownCutRod(n - i))
      }
      ans
    }
  }

  /*
  Memoized-Cut-Rod(p,n)
  sei r[0..n] ein neues Feld
  FOR i=0 TO n r[i]= -∞
  RETURN Memoized-Cut_Rod_Aux(p,n,r)
  Memoized-Cut_Rod_Aux(p,n,r)
  IF r[n] ≥ 0 RETURN r[n] //Wert schon vorher berechnet ????
  IF n==0
  // Wenn Länge =0 dann Ergebnis= 0
  q=0
  else q= -∞
  else FOR i=1 TO n
  q= max( q, p[i] + Memoized-Cut_Rod_Aux(p,n-i,r))
  r[n]=q
  RETURN q
   */
  def topDownMemorizedCutRod(n: Int): Int = ???

  /*
  sei r[0..n] ein neues Feld
  r[0]=0
  FOR j=1 TO n
  q= -∞
  FOR i=1 TO j
  q= max(q,p[i]+r[j-i])
  r[j]=q
  RETURN r[n]
   */
  def bottomUpCutRod(n: Int): Int = {
//    var r = Array.fill(n)(0)
    val r = Array.tabulate[Int](n)(_ => 0)

    for(j<- 1 to n){
      var ans = -Integer.MAX_VALUE
      for( i <- 1 to j) {
        ans = Math.max(ans, prices(i) + r(j - i))
      }
      r(j) = ans
    }
    r(n)
  }
}