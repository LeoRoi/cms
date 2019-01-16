package matrix

class Matrix(data: Array[Array[Double]]) {
  type Vector = Array[Double]

  // data has rows of the matrix, top down
  require(data != null
    && (data forall (X => X.size == data(0).size)) // all rows have same length
    && data.size > 0
    && data(0).size > 0)

  val rows = data.size
  val columns = data(0).size

  def toArray: Array[Array[Double]] = data

  def equals(matrix: Matrix): Boolean =
    data.flatten.sameElements(matrix.toArray flatMap (x => x))

  def transpose: Matrix =
    new Matrix(data.transpose)

  override def toString = {
    val s = for (row <- data) yield (row mkString ("\t"))
    s mkString ("\n")
  }

  //   adds two matrices
  def +(matrix: Matrix): Matrix = {
    require(matrix.columns == this.columns || matrix.rows == this.rows)
    val t = matrix.toArray zip data map (X => X._1 zip X._2)
    new Matrix(for (row <- t) yield row map (X => X._1 + X._2))
  }

  // multiplies a matrix with a constant
  def *(value: Double): Matrix = {
    new Matrix(for (row <- data)
      yield for (e <- row)
        yield e * value)
  }

  // multiplies each row with a vector and sums all components
  def *(v: Vector): Vector = {
    require(columns == v.size)
    for (row <- this.data)
      yield row.zip(v).map(x => x._1 * x._2).sum
  }

  // multiplies two matrices
  def *(matrix: Matrix): Matrix = {
    require(this.columns == matrix.rows)
    new Matrix(for (row <- data)
      yield for (col <- matrix.transpose.toArray)
        yield row zip col map (x => x._1 * x._2) reduceLeft (_ + _))
  }

  // transform a matrix in to a sparse representation
  def toSparseMatrix: SparseMatrix = {
    new SparseMatrix(
      (for {i <- data.indices; j <- data(0).indices if (data(i)(j) != 0)}
        yield ((i, j), data(i)(j))) toList, rows, columns)
  }
}

object Matrix {
  type Vector = Array[Double]

  // creates a matrix from a sparse representation
  def createFromSparse(l: List[((Int, Int), Double)], row: Int, col: Int): Matrix = {
    val init: Array[Array[Double]] = Array.tabulate(row, col)((X, Y) => 0)
    l foreach (X => init(X._1._1)(X._1._2) = X._2)
    new Matrix(init)
  }

  def vectorMult(v1: Vector, v2: Vector): Double =
    v1.zip(v2).map(x => x._1 * x._2).sum
}