package bintree

/**
  * tree structure
  */
abstract class IntSet {
  def insert(x: Int): IntSet
  def contains(x: Int): Boolean
  def union(that: IntSet): IntSet
}

case object Empty extends IntSet {
  def contains(x: Int): Boolean = false
  def insert(x: Int): IntSet = new NonEmpty(x, Empty, Empty)
  override def union(that: IntSet): IntSet = that
}

case class NonEmpty(elem: Int, val left: IntSet, val right: IntSet) extends IntSet {
  def contains(x: Int): Boolean =
    if (x < elem) left contains x
    else if (x > elem) right contains x
    else true

  def insert(x: Int): IntSet =
    if (x < elem) NonEmpty(elem, left insert x, right)
    else if (x > elem) NonEmpty(elem, left, right insert x)
    else this

  override def union(that: IntSet): IntSet =
    ((left union right) union that) insert elem
}

/**
  tree operations
  */
object Aufgaben {
  def mergeListIntoTree(is: IntSet, list: List[Int]): IntSet = {
    def loop(acc: IntSet, list: List[Int]): IntSet = list match {
      case Nil => acc
      case head :: tail => loop(acc.insert(head), tail)
    }

    loop(is, list)
  }

  def findNewRoot(tree: IntSet): Option[Int] = {
    def findMostLeftElem(tree: IntSet): Option[Int] = tree match {
      case NonEmpty(value, Empty, _) => Option(value)
      case NonEmpty(value, left, _) => findMostLeftElem(left)
    }

    tree match {
      case NonEmpty(_, Empty, _) => None
      case NonEmpty(_, _, right) => findMostLeftElem(right)
    }
  }

  def delete(deleteMe: Int, tree: IntSet): IntSet = tree match {

    // Fall 1: Elememt nicht gefunden
    case Empty => Empty

    // Fall 2: Element gefunden mit einem Teilbaum leer
    case NonEmpty(`deleteMe`, Empty, right) => right
    case NonEmpty(`deleteMe`, left, Empty) => left

    // Fall 3: Knoten gefunden und ist in der Mitte des Baums
    case NonEmpty(`deleteMe`, left, right) => {
      val successor = findNewRoot(tree)
      NonEmpty(successor.get, left, delete(successor.get, right))
    }

    // Weiter Suchen....
    case NonEmpty(el, left, right) =>
      if (deleteMe < el) NonEmpty(el, delete(deleteMe, left), right)
      else NonEmpty(el, left, delete(deleteMe, right))
  }

  // Funktion ueberfuehrt eine Liste von Zahlen in einen Binary Tree
  def list2Tree(l: List[Int]): IntSet = {
    def loop(list: List[Int], acc: IntSet): IntSet = list match {
      case Nil => acc
      case head :: tail => loop(tail, acc.insert(head))
    }

    if (l.isEmpty) Empty
    else if (l.size == 1) NonEmpty(l.head, Empty, Empty)
    else loop(l.tail, NonEmpty(l.head, Empty, Empty))
  }

  // Funktion ueberfuehrt einen Binaeren Suchbaum in eine sortierte Liste
  def tree2sortedList(tree: IntSet): List[Int] = {
    def loop(tree: IntSet, acc: List[Int], lastElement: Int): List[Int] = tree match {
      case Empty => acc
      case node: NonEmpty => {
        val head = node.elem
        if (acc.isEmpty) loop(delete(head, tree), List(head), head)
        else if (head >= lastElement) loop(delete(head, tree), acc :+ head, head)
        else loop(delete(head, tree), head :: acc, head)
      }
    }

    loop(tree, List(), 0)
  }

  def tree2SortedListG(t: IntSet): List[Int] = t match {
    case Empty => List()
    case NonEmpty(elem, left, right) =>
      tree2sortedList(left) ++ List(elem) ++ tree2sortedList(right)
  }

  // Funktion ueberfuerhrt einen Binaeren Suchbaum in eine Liste,
  // indem der Baum Ebene fuer Ebene durchlaufen wird
  def breadthFirstSearch(tree: IntSet): List[Int] = {
    def bfs(nodes: List[IntSet]): List[Int] = nodes match {
      case List() => Nil //nil == empty list
      case Empty :: tail => bfs(tail)
      case NonEmpty(head, Empty, Empty) :: tail => head :: bfs(tail)
      case NonEmpty(head, left: IntSet, right: IntSet) :: tail =>
        head :: bfs(tail ++ List(left, right))
      case _ => throw new Error("omg")
    }

    bfs(List(tree))
  }

  def printBreadthFirst(tree: IntSet): List[(Int, Int)] = {
    def bfs(nodes: List[IntSet], lvl: Int): List[(Int, Int)] = nodes match {
      case List() => Nil
      case Empty :: tail => bfs(tail, lvl+1)
      case NonEmpty(head , _, _) => {

        case NonEmpty(head, Empty, Empty) :: tail => (head, lvl) :: bfs(tail, lvl)
        case NonEmpty(head, left: IntSet, right: IntSet) :: tail =>
          (head, lvl) :: bfs(tail ++ List(left, right), lvl)
      }
      case _ => throw new Error("omg")
    }

    bfs(List(tree), 0)
  }

  def printLevelOrder(tree: IntSet): List[Int] = {
    var out = List[Int]()

    def printGivenLevel (tree: IntSet, level: Int): Unit = tree match {
      case node: NonEmpty => {
        if (level == 1)
//          print(node.elem + " ")
          out :+ node.elem
        else if (level > 1) {
          printGivenLevel(node.left, level - 1)
          printGivenLevel(node.right, level - 1)
        }
      }
      case Empty =>
    }
    val h = height(tree)
    for (i <- 1 to h+1) printGivenLevel(tree, i)
    out
  }

  // https://www.geeksforgeeks.org/level-order-tree-traversal/
  // Funktion bestimmt die Hoehe eines Binaeren Suchbaums
  def height(tree: IntSet): Int = {
    def traverse(acc: Int, tree: IntSet): Int = tree match {
      case node: NonEmpty => {
        val left = height(node.left)
        val right = height(node.right)
        if(left > right) left+1
        else right+1
      }
      case Empty => acc
    }
    traverse(-1, tree)
  }

  def heightG(tree: IntSet): Int = tree match {
    case Empty => -1
    case NonEmpty(_, left, right) =>
      (1+heightG(left)).max(1+heightG(right))
  }
}
