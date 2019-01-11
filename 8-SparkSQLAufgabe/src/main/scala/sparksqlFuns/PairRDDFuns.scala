package sparksqlFuns

import org.apache.spark.SparkContext

class PairRDDFuns(sc: SparkContext) extends University {
  val professorenRDD = sc.parallelize(data_profs)
  val studentenRDD = sc.parallelize(data_studenten)
  val hoerenRDD = sc.parallelize(data_hoeren)
  val vorlesungenRDD = sc.parallelize(data_vorlesungen)
  val voraussetzenRDD = sc.parallelize(data_voraussetzen)
  val assistentenRDD = sc.parallelize(data_assis)
  val pruefenRDD = sc.parallelize(data_pruefen)
  val hoerenBig = sc.parallelize(hoeren_big)

  // Determine Socrates' students by using PairRDDs
  // and join operators
  def studentsFromSocrates: List[Student] = ???
//  {
//    val sokratesP = professorenRDD.filter(p => p.name.equals("Sokrates")).first().persnr
//  }

  // Determine all students that haven't heard any lecture
  def studentsHearingNoLecture: List[Student] = ???

  //Determine all students hearing the lecture ("Grundzuege")
  // use the hoerenBig-Dataset
  def studentsHearingGrundzuege: List[Student] = {
    val v = vorlesungenRDD.filter(v => v.titel.equals("Grundzuege")).map(v => (v.vorlnr, v))
    val h = hoerenBig.map(h => (h.vorlnr, h.matrnr))
    val vh = h.join(v).map(x => (x._2._1, 1))
    val res = vh.join(studentenRDD.map(s=>(s.matrnr, s))).map(_._2._2).collect()
    res.toList
  }

  //Determine all students hearing the lecture ("Grundzuege")
  // use the hoerenBig-Dataset and a cartesian products
  def studentsHearingGrundzuegeCartesianProduct: List[Student] = ???
//  {
//    val set = vorlesungenRDD.cartesian(hoerenBig).cartesian(studentenRDD)
//    set.filter(s=>s._1._1.titel.equals("Grungquege"))
//  }
}
