package test

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import matrix._

class ListTest extends FunSuite {

  trait TestMatrizes {

    val m0 = new Matrix(Array(Array(2, 4, 1), Array(123, 21, 34), Array(34, 1, 2345)))
    val m1 = new Matrix(Array(Array(2, 4, 1), Array(123, 21, 34), Array(34, 1, 2345)))
    val m2 = new Matrix(Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9)))
    val m3 = new Matrix(Array(Array(2, 4, 6), Array(8, 10, 12), Array(14, 16, 18)))
    val m4 = new Matrix(Array(Array(30, 36, 42), Array(66, 81, 96), Array(102, 126, 150)))
    val m5 = new Matrix(Array(Array(1, 2), Array(4, 5), Array(7, 8)))
    val m6 = new Matrix(Array(Array(30, 36), Array(66, 81), Array(102, 126)))
    val m7 = new Matrix(Array(Array(1, 2, 3), Array(4, 5, 6)))
    val m8 = new Matrix(Array(Array(0, 4, 0), Array(8, 0, 0), Array(0, 0, 18)))
    val m9 = new Matrix(Array(Array(11, 12), Array(13, 14), Array(15, 16)))
    val m2_m9 = new Matrix(Array(Array(82, 88), Array(199, 214), Array(316, 340)))
    val l8 = List(((1, 0), 8.0), ((2, 2), 18.0), ((0, 1), 4.0))
    val l2 = List(((0, 0), 1.0), ((0, 1), 2.0), ((0, 2), 3.0), ((1, 0), 4.0), ((1, 1), 5.0), ((1, 2), 6.0), ((2, 0), 7.0), ((2, 1), 8.0), ((2, 2), 9.0))
    val l3 = List(((0, 0), 1.0), ((0, 1), 2.0), ((0, 2), 3.0), ((1, 0), 4.0), ((1, 1), 5.0), ((1, 2), 6.0), ((2, 0), 7.0), ((2, 1), 8.0))
    val sm = new SparseMatrix(l3, 3, 3)
    val sm_m9 = new Matrix(Array(Array(82, 88), Array(199, 214), Array(181, 196)))
  }

  test("Kein gueltiges Array bei der Konstruktion") {

    intercept[IllegalArgumentException] {
      new Matrix(Array(Array(2, 4, 1), Array(123, 34)))
    }
  }

  test("Leeres Array bei der Konstruktion 1") {

    intercept[IllegalArgumentException] {
      new Matrix(Array())
    }
  }

  test("Leeres Array bei der Konstruktion 2") {

    intercept[IllegalArgumentException] {
      new Matrix(Array(Array()))
    }
  }

  test("Uebergabe von null bei der Konstruktion") {

    intercept[IllegalArgumentException] {
      new Matrix(null)
    }
  }
  test("Test der Addition 1") {

    new TestMatrizes {

      assert((m2 + m2).equals(m3))
    }
  }

  test("Test Multiplikation mit Konstante") {

    new TestMatrizes {

      assert((m2 * 2).equals(m3))
    }
  }


  test("Multiplikation mit einem Vektor") {

    intercept[IllegalArgumentException] {
      new TestMatrizes {
        m1 * Array(2.0, 3.0)
      }
    }
  }

  test("Multiplikation mit einem Vektor 2") {

    new TestMatrizes {
      val r = m2 * Array(1.0, 2.0, 1.0)
      assert(r.sameElements(Array(8, 20, 32)))
    }
  }

  test("Multiplikation mit einer Matrize 1") {

    new TestMatrizes {
      val r = m2 * m2
      assert(r.equals(m4))
    }
  }

  test("Multiplikation mit einer Matrize 2") {

    new TestMatrizes {
      val r = m2 * m5
      assert(r.equals(m6))
    }
  }

  test("Multiplikation mit einer Matrize 3") {

    new TestMatrizes {
      intercept[IllegalArgumentException] {

        val r = m2 * m7
      }
    }
  }

  test("Multiplikation mit einer Matrize 4") {

    new TestMatrizes {
      val r = m2 * m9
      assert(r.equals(m2_m9))
    }
  }

  test("Test der Umwandlung zur Sparse-Matrix") {

    new TestMatrizes {
      val r = m8.toSparseMatrix.toList.toSet
      assert(r === l8.toSet)
    }
  }

  test("Test der Umwandlung zur Sparse-Matrix 2") {

    new TestMatrizes {
      val r = m2.toSparseMatrix.toList.toSet
      assert(r === l2.toSet)
    }
  }

  test("Test der Umwandlung von Sparse-Matrix") {

    new TestMatrizes {
      val r = Matrix.createFromSparse(l8, 3, 3)
      assert(r.equals(m8))
    }
  }

  test("Test der Umwandlung von Sparse-Matrix 2") {

    new TestMatrizes {
      val r = Matrix.createFromSparse(l2, 3, 3)
      assert(r.equals(m2))
    }
  }


}