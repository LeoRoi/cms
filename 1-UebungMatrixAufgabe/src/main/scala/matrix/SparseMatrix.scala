package matrix

class SparseMatrix(data_list: List[((Int, Int), Double)], val rows: Int, val columns: Int) {
  type Vector = Array[Double]
  val data = data_list.toMap

  def toList: List[((Int, Int), Double)] =
    data.toList

  def toMatrix: Matrix =
    Matrix.createFromSparse(data.toList, rows, columns)

  def getRow(row_nr: Int): Vector =
  this.toMatrix.toArray(row_nr)

  def getColumn(col_nr: Int): Vector =
  this.toMatrix.transpose.toArray(col_nr)

  def equals(matrix: SparseMatrix): Boolean =
  this.toMatrix.equals(matrix.toMatrix)

  def *(v: Vector): Vector =
    this.toMatrix * v

  def *(matrix: SparseMatrix): SparseMatrix = {
    val m = new Matrix((
      for (row <- 0 to rows - 1) yield
      (for (col <- 0 to matrix.columns - 1) yield
        Matrix.vectorMult(getRow(row), matrix.getColumn(col))).toArray).toArray)

    m.toSparseMatrix
  }

  def matrixMultiplicationMR(data: SparseMatrix): SparseMatrix =
    (this.toMatrix * data.toMatrix).toSparseMatrix
}