package test

import org.scalatest.FunSuite
import matrix._

class SparseMatrixMRTest extends FunSuite{
  
  val data_tabA_1= List(((0,0),1.0),((0,1),2.0))
  val data_tabB_1= List(((0,0),11.0),((1,0),12.0))
  val result1= List(((0,0),35.0))  
   /* 						11 				
   * 	1 2 	*			12 		= 	35
   */
  
  
  val data_tabA_2= List(((0,0),1.0),((0,1),2.0),((1,0),3.0),((1,1),4.0))
  val data_tabB_2= List(((0,0),11.0),((0,1),12.0),((1,0),13.0),((1,1),14.0))
  val result2= List(((0,0),37.0),((0,1),40.0),((1,0),85.0),((1,1),92.0))
   /* 1 2 				11 12				37  40
   * 	3 4 		*		13 14		= 	85  92
   */

  val data_tabA_3= List(((0,0),1.0),((0,1),2.0),((0,2),3.0),((1,0),4.0),((1,1),5.0),((1,2),6.0),
      ((2,0),7.0),((2,1),8.0),((2,2),9.0))
  val data_tabB_3= List(((0,0),11.0),((0,1),12.0),((1,0),13.0),((1,1),14.0),((2,0),15.0),((2,1),16.0))    
  val result3= List(((0,0),82.0),((0,1),88.0),((1,0),199.0),((1,1),214.0),((2,0),316.0),((2,1),340.0))
  
  /* 1 2 3				11 12				082 088
   * 4 5 6		*		13 14		= 	199 214
   * 7 8 9				15 16				316 340
   */
  val l=3 // nr rows A
  val m=3 // nr columns A == nr rows B
  val n= 2 // nr columns B
  
  test("Matrix Multiplikation 1"){
    
    val A= new SparseMatrix(data_tabA_1,1,2)
    val B= new SparseMatrix(data_tabB_1,2,1)
    val C= new SparseMatrix(result1,1,1)
    assert(A.matrixMultiplicationMR(B).equals(C))
  }
  
  test("Matrix Multiplikation 2"){

    val A= new SparseMatrix(data_tabA_2,2,2)
    val B= new SparseMatrix(data_tabB_2,2,2)
    val C= new SparseMatrix(result2,2,2)
    assert(A.matrixMultiplicationMR(B).equals(C))    
  }

  test("Matrix Multiplikation 3"){
  
    val A= new SparseMatrix(data_tabA_3,3,3)
    val B= new SparseMatrix(data_tabB_3,3,2)
    val C= new SparseMatrix(result3,3,2)
    assert(A.matrixMultiplicationMR(B).equals(C)) 
  }
}