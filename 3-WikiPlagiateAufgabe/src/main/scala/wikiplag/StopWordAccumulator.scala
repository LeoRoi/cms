package wikiplag

import org.apache.spark.util.AccumulatorV2

//sc.register
// word and doc id from where is comes
class StopWordAccumulator extends AccumulatorV2[(String, Long), Map[String, List[Long]]] {

  var map = Map[String, List[Long]]()

  def reset: Unit = map.empty

//  def add(el: (String, Long)) = map.updated(el._1, map(el._1) :+ el._2)
  def add2(el: (String, Long)) = map = map.updated(el._1, map.getOrElse(el._1, List()) :+ el._2)

  def add(el: (String, Long)) = map = map.updated(el._1, el._2 :: map.getOrElse(el._1, List()))

//  fix me
  def merge(other: AccumulatorV2[(String, Long), Map[String, List[Long]]]): Unit = {
//    val mapSum = map.toSeq ++ other.value.toSeq
    val mapSum = other.value.toSeq
    val g = mapSum.groupBy(_._1)
    val gg = g.mapValues(x => x.flatMap(_._2).toList)
    gg.map(x => x._2.map(z => this.add(x._1, z)))
//    g.mapValues(x => x.map(_._2))
//    gg.map(x => x._2.fothis.add(x._1, x._2))
//    g.foreach(println)
//    println(g)

  }

  def value: Map[String, List[Long]] = map

//  empty map
  def copy: AccumulatorV2[(String, Long), Map[String, List[Long]]] = this

  def isZero: Boolean = map.isEmpty
}