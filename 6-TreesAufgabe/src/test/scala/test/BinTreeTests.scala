package test

import bintree._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Beleg1Tests extends FunSuite {

  trait TestTrees {
    val l = List(7, 3, 10, 15)
    val t0 = NonEmpty(7, NonEmpty(3, Empty, Empty), NonEmpty(10, Empty, NonEmpty(15, Empty, Empty)))
    val t1 = Aufgaben.list2Tree(List(3, 6, 2, 5, 9, 1))
    val t2 = Aufgaben.list2Tree(List(3, 6, 2, 5, 9, 1, 2, 9))
    val t3 = Aufgaben.list2Tree(List(3, 6, 2, 5, 9, 1, 4, 11))
    val t4 = NonEmpty(7, NonEmpty(3, Empty, Empty), NonEmpty(10, Empty, NonEmpty(15, NonEmpty(1, Empty, Empty), Empty)))
  }

  /* t0
         7
       /   \
     3      10
    / \     / \
   x   x   x   15
               / \
              x   x
   */

  test("Union") {
    new TestTrees {
      assert(t0.union(Aufgaben.list2Tree(List(2,4))) === Aufgaben.list2Tree(List(2,4,3,15,10,7)))
    }
  }

  test("mergeListIntoTree") {
    new TestTrees {
      val newTree = Aufgaben.mergeListIntoTree(t0, List(2,4))
      assert(newTree === Aufgaben.list2Tree(List(7,3,10,2,4,15)))
      println(newTree)
    }
  }

  test("List2Tree-Test") {
    new TestTrees {
      assert(t0 === Aufgaben.list2Tree(l))
    }
  }
  test("Tree2sortedList") {
    new TestTrees {
      assert(List(1, 2, 3, 5, 6, 9) === Aufgaben.tree2sortedList(t1))
    }
  }

  test("Tree2sortedList G") {
    new TestTrees {
      assert(List(1, 2, 3, 5, 6, 9) === Aufgaben.tree2SortedListG(t1))
    }
  }

  test("Binary Tree insert mit Doppelten") {
    new TestTrees {
      assert(List(1, 2, 3, 5, 6, 9) === Aufgaben.tree2sortedList(t2))
    }
  }
  test("BreadtFirstSearchTest") {
    new TestTrees {
      assert(List(3, 2, 6, 1, 5, 9, 4, 11) === Aufgaben.breadthFirstSearch(t3))
    }
  }
  test("HeightTest") {
    new TestTrees {
      assert(0 === Aufgaben.height(NonEmpty(9, Empty, Empty)))
      assert(1 === Aufgaben.height(NonEmpty(9, NonEmpty(4, Empty, Empty), Empty)))
      assert(2 === Aufgaben.height(t0))
      assert(3 === Aufgaben.height(t4))
    }
  }
  test("HeightG") {
    new TestTrees {
      assert(0 === Aufgaben.heightG(NonEmpty(9, Empty, Empty)))
      assert(1 === Aufgaben.heightG(NonEmpty(9, NonEmpty(4, Empty, Empty), Empty)))
      assert(2 === Aufgaben.heightG(t0))
      assert(3 === Aufgaben.heightG(t4))
    }
  }
  test("findSuccessor-Test") {
    new TestTrees {
      assert(Option(4) === Aufgaben.findNewRoot(t3))
    }
  }
  test("Delete-Elem not found") {
    new TestTrees {
      val t = Aufgaben.delete(45, t3)
      assert(Aufgaben.breadthFirstSearch(t3) === Aufgaben.breadthFirstSearch(t))
    }
  }
  test("Delete-Elem Mitte") {
    new TestTrees {
      val t = Aufgaben.delete(3, t3)
      assert(List(4, 2, 6, 1, 5, 9, 11) === Aufgaben.breadthFirstSearch(t))
    }
  }
  test("Delete-Elem Blatt") {
    new TestTrees {
      val t = Aufgaben.delete(11, t3)
      assert(List(3, 2, 6, 1, 5, 9, 4) === Aufgaben.breadthFirstSearch(t))
    }
  }
  //  test("BreadthFirstSearchTest2") {
  //    new TestTrees {
  //      val tree = NonEmpty(1, NonEmpty(2, NonEmpty(4, Empty, Empty), NonEmpty(5, Empty, Empty)), NonEmpty(3, Empty, Empty))
  //      assert(List(1,2,3,4,5) === Aufgaben.printLevelOrder(tree))
  //    }
  //  }
}