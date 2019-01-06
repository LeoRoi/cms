package bintree


/**
  * tree construct
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

  @Override
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
      case head :: tail => mergeListIntoTree(acc.insert(head), tail)
    }

    loop(is, list)
  }

  def findSuccessor(tree: IntSet): Option[Int] = {
    def findMostLeftElem(tree: IntSet): Option[Int] = tree match {
      case NonEmpty(value, Empty, _) => Option(value)
      case NonEmpty(value, left, _) => findMostLeftElem(left)
    }

    tree match {
      case NonEmpty(_, Empty, _) => None
      case NonEmpty(_, _, right) => findMostLeftElem(right)
    }
  }

  def delete(Elem: Int, tree: IntSet): IntSet = tree match {

    // Fall 1: Elememt nicht gefunden
    case Empty => Empty
    // Fall 2: Element gefunden und rechter oder linker Teilbaum leer
    case NonEmpty(Elem, Empty, right) => right
    case NonEmpty(Elem, left, Empty) => left
    // Fall 3: Knoten gefunden und ist in der Mitte des Baums
    case NonEmpty(Elem, left, right) => {
      val successor = findSuccessor(tree)
      new NonEmpty(successor.get, left, delete(successor.get, right))
    }
    // Weiter Suchen....
    case NonEmpty(value, left, right) => if (Elem < value) new NonEmpty(value, delete(Elem, left), right)
    else new NonEmpty(value, left, delete(Elem, right))
  }

  // Funktion ueberfuehrt eine Liste von Zahlen in einen Binary Tree
  def list2Tree(l: List[Int]): IntSet = {
    def unrollList(list: List[Int], acc: IntSet): IntSet = list match {
      case head :: tail => unrollList(tail, acc.insert(head))
      case Nil => acc
    }

    if (l.size == 0)
      Empty
    else if (l.size == 1)
      NonEmpty(l.head, Empty, Empty)
    else
      unrollList(l.tail, NonEmpty(l.head, Empty, Empty))
  }

  // Funktion ueberfuehrt einen Binaeren Suchbaum in eine sortierte Liste
  def tree2SortedList(t: IntSet): List[Int] = {
    def unrollTree(tree: IntSet, acc: List[Int], lastElement: Int): List[Int] = tree match {
      case node: NonEmpty => {
        val head = node.elem
        if (acc.isEmpty) unrollTree(delete(head, tree), List(head), head)
        else if (head >= lastElement) unrollTree(delete(head, tree), acc :+ head, head)
        else unrollTree(delete(head, tree), List(head) ::: acc, head)
      }
      case Empty => acc
    }

    unrollTree(t, List(), 0)
  }

  def tree2SortedListG(t: IntSet): List[Int] = t match {
    case Empty => List()
    case NonEmpty(elem, left, right) =>
      tree2SortedList(left) ++ List(elem) ++ tree2SortedList(right)
  }

  // Funktion ueberfuerhrt einen Binaeren Suchbaum in eine Liste, in dem der
  // Baum Ebene fuer Ebene durchlaufen wird
  def breadthFirstSearch(tree: IntSet): List[Int] = {
    def bfs(nodes: List[IntSet]): List[Int] = nodes match {
      case Nil => List() //nil == empty list
      case Empty :: tail => bfs(tail)
      case NonEmpty(head, Empty, Empty) :: tail => head :: bfs(tail)
      case NonEmpty(head, left: IntSet, right: IntSet) :: tail =>
        head :: bfs(tail ++ List(left, right))
      case _ => throw new Error("omg")
    }
    bfs(List(tree))
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
