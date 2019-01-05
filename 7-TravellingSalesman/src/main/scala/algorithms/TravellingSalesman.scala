package combinations

// https://en.wikipedia.org/wiki/Travelling_salesman_problem
abstract trait TravellingSalesman {

  // Berechnung des Travelling Salesman fuer die Staedte
  // 0 - Aachen ||| 1 - Augsburg ||| 2 - Bayreuth
  // 3 - Berlin ||| 4 - Bremen ||| 5 - Cottbus
  // 6 - Dresden ||| 7 - Erfurt ||| 8 - Essen
  // 9 - Frankfurt/Main ||| 10 - Frankfurt/Oder ||| 11 - Freiburg
  // 12 - Fulda ||| 13 - Garmisch-Partenkirchen ||| 14 - Hamburg
  // 15 - Hannover ||| 16 - Karlsruhe ||| 17 - Kassel
  // 18 - Kiel ||| 19 - Koblenz ||| 20 - Köln ||| 21 - Leipzig

  val staedte = Array("Aachen", "Augsburg", "Bayreuth", "Berlin", "Bremen", "Cottbus", "Dresden", "Erfurt", "Essen", "Frankfurt/Main",
    "Frankfurt/Oder", "Freiburg", "Fulda", "Garmisch-Partenkirchen", "Hamburg", "Hannover", "Kassel", "Kiel", "Koblenz", "Koeln", "Leipzig")

  // Erzeugen der Abstandtabelle
  // Uebernommen von http://www.auslandversicherung.de/entfernungstabelle_deutschland.html
  val abstand: Array[Array[Int]] = new Array[Array[Int]](22)
  abstand(0) = Array(0, 570, 532, 637, 369, 739, 651, 446, 123, 240, 721, 466, 330, 740, 475, 354, 345, 307, 556, 145, 60, 573)
  abstand(1) = Array(570, 0, 239, 593, 715, 574, 472, 422, 601, 365, 649, 340, 335, 117, 720, 600, 221, 432, 820, 430, 538, 431)
  abstand(2) = Array(532, 239, 0, 352, 572, 339, 237, 187, 494, 264, 414, 457, 187, 334, 596, 460, 337, 278, 693, 345, 431, 198)
  abstand(3) = Array(637, 593, 352, 0, 375, 125, 214, 288, 480, 564, 91, 800, 474, 686, 279, 258, 670, 367, 343, 610, 553, 184)
  abstand(4) = Array(369, 715, 572, 375, 0, 496, 478, 351, 249, 450, 467, 722, 388, 856, 110, 118, 595, 288, 205, 415, 315, 367)
  abstand(5) = Array(739, 574, 339, 125, 496, 0, 138, 320, 608, 585, 119, 800, 490, 686, 430, 378, 670, 502, 511, 633, 683, 244)
  abstand(6) = Array(651, 472, 237, 214, 478, 138, 0, 220, 581, 485, 177, 700, 390, 586, 492, 385, 570, 402, 573, 533, 583, 140)
  abstand(7) = Array(446, 422, 187, 288, 351, 320, 220, 0, 367, 268, 366, 533, 180, 515, 376, 289, 403, 135, 459, 318, 373, 170)
  abstand(8) = Array(123, 601, 494, 480, 249, 608, 581, 367, 0, 256, 600, 524, 297, 736, 350, 258, 397, 188, 454, 160, 75, 475)
  abstand(9) = Array(240, 365, 264, 564, 450, 585, 485, 268, 256, 0, 661, 262, 95, 502, 509, 362, 135, 190, 599, 125, 185, 407)
  abstand(10) = Array(721, 649, 414, 91, 467, 119, 177, 366, 600, 661, 0, 873, 547, 759, 382, 331, 743, 440, 446, 683, 626, 211)
  abstand(11) = Array(466, 340, 457, 800, 722, 800, 700, 533, 524, 262, 873, 0, 357, 490, 759, 624, 130, 457, 852, 333, 435, 642)
  abstand(12) = Array(330, 335, 187, 474, 388, 490, 390, 180, 297, 95, 547, 357, 0, 477, 410, 272, 238, 105, 504, 227, 277, 311)
  abstand(13) = Array(740, 117, 334, 686, 856, 686, 586, 515, 735, 502, 759, 490, 477, 0, 869, 742, 360, 575, 960, 580, 675, 513)
  abstand(14) = Array(475, 720, 596, 279, 110, 430, 492, 376, 350, 509, 382, 759, 410, 869, 0, 154, 640, 312, 85, 520, 381, 387)
  abstand(15) = Array(354, 600, 460, 258, 118, 378, 385, 289, 258, 362, 331, 624, 272, 742, 154, 0, 500, 238, 238, 410, 295, 252)
  abstand(16) = Array(345, 221, 337, 670, 595, 670, 570, 403, 397, 135, 743, 130, 238, 360, 640, 500, 0, 330, 731, 206, 310, 515)
  abstand(17) = Array(307, 432, 278, 367, 288, 502, 402, 135, 188, 190, 440, 457, 105, 575, 312, 238, 330, 0, 405, 243, 243, 278)
  abstand(18) = Array(556, 820, 693, 343, 205, 511, 573, 459, 454, 599, 446, 852, 504, 960, 85, 238, 731, 405, 0, 600, 493, 485)
  abstand(19) = Array(145, 430, 345, 610, 415, 633, 533, 318, 160, 125, 683, 333, 227, 580, 520, 410, 206, 243, 600, 0, 110, 454)
  abstand(20) = Array(60, 538, 431, 553, 315, 683, 583, 373, 75, 185, 626, 435, 277, 675, 381, 295, 310, 243, 493, 110, 0, 488)
  abstand(21) = Array(573, 431, 198, 184, 367, 244, 140, 170, 475, 407, 211, 642, 311, 513, 387, 252, 515, 278, 485, 454, 488, 0)

  //returns only the optimal cycle, without using km
  def calculateRoundTrip(list: List[Int]): List[Int] = {
    def findOpt(wayOpt: List[Int], kmOpt: Int, tail: List[List[Int]]): List[Int] = tail match {
      case x :: xs => {
        val km = calculateCycle(x)

        if (km < kmOpt) findOpt(x, km, xs)
        else findOpt(wayOpt, kmOpt, xs)
      }
      case Nil => wayOpt
    }

    if (list == List()) List()
    else if (list.length == 1) list
    else {
      val perms = findPermutations(list)
      val head :: tail = perms
      findOpt(head, calculateCycle(head), tail)
    }
  }

  // n! = all possible routes
  def findPermutations(list: List[Int]): List[List[Int]] = list match {
    case Nil => List(Nil)
    case _ =>
      for {j <- list
           k <- findPermutations(list.diff(List(j)))
      } yield j :: k
  }

  def calculateCycle(way: List[Int]): Int = {
    def calculateDistance(start: Int, way: List[Int]): Int = way match {
      case x :: y :: rest => abstand(x)(y) + calculateDistance(start, y :: rest)
      case x :: Nil => abstand(x)(start)
      case _ => 0
    }
    calculateDistance(way.head, way)
  }

  // visualization
  def printRoundTrip(way: List[Int]) = {
    def printWay(start: Int, way: List[Int]): Unit = way match {
      case x :: y :: rest => {
        println("Von " + staedte(way(x)) + " nach " + staedte(way(y)) + " sind es " + abstand(x)(y) + "km")
        printWay(start, y :: rest)
      }
      case x :: Nil =>
        println("Von " + staedte(way(x)) + " nach " + staedte(way(start)) + " sind es " + abstand(x)(start) + "km")
      case _ => println()
    }
    println("\nInsgesamt sind es: " + calculateCycle(way) + " km.\n")
  }
}
