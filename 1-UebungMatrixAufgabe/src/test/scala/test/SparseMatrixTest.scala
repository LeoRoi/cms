package test

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import matrix._

class SparseMatrixTest extends FunSuite {

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
    val l4_m8square = List(((0, 0), 32.0), ((1, 1), 32.0), ((2, 2), 324.0))
    val sm = new SparseMatrix(l3, 3, 3)
    val sm_m9 = new Matrix(Array(Array(82, 88), Array(199, 214), Array(181, 196)))
  }

  test("Test getRow") {
    new TestMatrizes {
      val spm = new SparseMatrix(l8, 3, 3)
      val row = spm.getRow(1)
      assert(row === Array(8.0, 0.0, 0.0))
    }
  }
  test("Test getColumn") {
    new TestMatrizes {
      val spm = new SparseMatrix(l8, 3, 3)
      val row = spm.getColumn(0)
      assert(row === Array(0.0, 8.0, 0.0))
    }
  }
  test("Test Equals Method") {
    new TestMatrizes {
      val sp1 = new SparseMatrix(l8, 3, 3)
      val sp2 = new SparseMatrix(((2, 1), 0.0) :: l8, 3, 3)
      assert(sp1.equals(sp2))
    }
  }
  test("Test Sparse-Matrix mal Vector") {
    new TestMatrizes {
      val m2s = m2.toSparseMatrix
      val r = m2s * Array(1.0, 2.0, 1.0)
      assert(r.sameElements(Array(8, 20, 32)))
    }
  }

  test("Test Sparse-Matrix mal Matrix") {
    new TestMatrizes {
      val m2s = m2.toSparseMatrix
      val m9s = m9.toSparseMatrix
      val r = m2s * m9s
      assert(Matrix.createFromSparse(r.toList, 3, 2).equals(m2_m9))
    }
  }

  test("Test Sparse-Matrix mal Matrix 2") {
    new TestMatrizes {
      val m8s = m8.toSparseMatrix
      val r = m8s * m8s
      assert(Matrix.createFromSparse(r.toList, 3, 3).equals(Matrix.createFromSparse(l4_m8square, 3, 3)))
    }
  }
}