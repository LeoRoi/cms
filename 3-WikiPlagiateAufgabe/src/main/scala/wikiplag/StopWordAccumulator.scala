package wikiplag

import org.apache.spark.util.AccumulatorV2

//sc.register
// word and doc id from where is comes
class StopWordAccumulator extends AccumulatorV2[(String, Long), Map[String, List[Long]]] {

  var map = Map[String, List[Long]]()

  def reset: Unit = map.empty

//  def add(el: (String, Long)) = map.updated(el._1, map(el._1) :+ el._2)
  def add2(el: (String, Long)) = map = map.updated(el._1, map.getOrElse(el._1, List()) :+ el._2)

  def add(el: (String, Long)) = {
    val newMap = map.updated(el._1, el._2 :: map.getOrElse(el._1, List()))
    map = newMap
  }

  def merge(other: AccumulatorV2[(String, Long), Map[String, List[Long]]]): Unit = {
    val merged = map.toSeq ++ other.value.toSeq
    // merged: Seq[(Int, Int)] = ArrayBuffer((1,2), (1,4))

    val grouped = merged.groupBy(_._1)
    // grouped: immutable.Map[Int, Seq[(Int, Int)]] = Map(1 -> ArrayBuffer((1,2), (1,4)))

    val cleaned = grouped.mapValues(_.flatMap(_._2).toList)
    // cleaned: scala.collection.immutable.Map[Int,List[Int]] = Map(1 -> List(2, 4))

    map = cleaned
  }

  def value: Map[String, List[Long]] = map

//  empty map
  def copy: AccumulatorV2[(String, Long), Map[String, List[Long]]] = this

  def isZero: Boolean = map.isEmpty
}