package matrix

class Matrix(data: Array[Array[Double]]) {

  type Vector = Array[Double]

  // tests the matrix: 
  //      -  size of rows
  //      -  row or size empty?
  require(data != null && (data forall (X => X.size == data(0).size)) && data.size > 0 && data(0).size > 0)

  def toArray: Array[Array[Double]] = data

  val rows = data.size
  val columns = data(0).size

  def equals(matrix: Matrix): Boolean = {

    data.flatMap(x => x).sameElements(matrix.toArray flatMap (x => x))
  }

  def transpose: Matrix = new Matrix(data.transpose)

  override def toString = {

    val s = for (row <- data) yield (row mkString ("\t"))
    s mkString ("\n")
  }

  // adds two matrices
  def +(matrix: Matrix): Matrix = {

    require(matrix.columns == this.columns || matrix.rows == this.rows)

    for(rowIndex <- this.rows)
      for(colIndex <- this.columns)
        this.toArray(rowIndex)(colIndex) += matrix(rowIndex)(colIndex)
  }

  // multiplies a matrix with a constant
  def *(value: Double): Matrix = {

    ???
  }

  // multiplies each row with a vector and sums all components 
  def *(v: Vector): Vector = {

    require(columns == v.size)
    ???
  }

  // multiplies two matrices
  def *(matrix: Matrix): Matrix = {

    require(this.columns == matrix.rows)
    ???
  }

  // transform a matrix in to a sparse representation
  def toSparseMatrix: SparseMatrix = {

    ???
  }
}

object Matrix {

  type Vector = Array[Double]

  // creates a matrix from a sparse representation
  def createFromSparse(l: List[((Int, Int), Double)], row: Int, col: Int): Matrix = {

    ???
  }

  def vectorMult(v1: Vector, v2: Vector): Double =

    ???
}