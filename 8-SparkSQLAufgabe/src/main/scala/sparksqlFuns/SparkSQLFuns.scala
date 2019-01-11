package sparksqlFuns

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
//sqlContext.implicits._

class SparkSQLFuns(ss: SparkSession, sc: SparkContext) extends University {
  sc.setLogLevel("ERROR")
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val profSchema = StructType(Array(
    StructField("persnr", IntegerType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("rang", StringType, nullable = true),
    StructField("raum", IntegerType, nullable = true)))

  val profRDD = sc.parallelize(data_profs).map(r => Row(r.persnr, r.name, r.rang, r.raum))
  val professorenDF = ss.createDataFrame(profRDD, profSchema)
  professorenDF.createOrReplaceTempView("Professoren")

  val studentenDF = sc.parallelize(data_studenten).toDF
  studentenDF.createOrReplaceTempView("Studenten")

  val vorlesungenDF = sc.parallelize(data_vorlesungen).toDF
  vorlesungenDF.createOrReplaceTempView("Vorlesungen")

  val hoerenDF = sc.parallelize(data_hoeren).toDF
  hoerenDF.createOrReplaceTempView("hoeren")

  val voraussetzenDF = sc.parallelize(data_voraussetzen).toDF
  voraussetzenDF.createTempView("voraussetzen")

  val assistentenDF = sc.parallelize(data_assis).toDF
  assistentenDF.createOrReplaceTempView("Assistenten")

  val pruefenDF = sc.parallelize(data_pruefen).toDF
  pruefenDF.createTempView("pruefen")

  val hoerenBigDF = sc.parallelize(hoeren_big).toDF
  hoerenBigDF.createTempView("hoerenBig")

  // Determine Socrates' students by applying an SQL-statement
  def studentsFromSocrates: List[Student] = {
    val resDF=ss.sql("""SELECT s.matrnr, s.name, s.semester FROM Professoren p, Student s, Vorlesung v, hoeren h where persnr=gelesenvon and h.vorlnr=v.vorlnr and s.matrnr=h.matrnr and.name="Sokrates" """)
    resDF.show()
    val res = resDF.collect()
//    res.map()
    List()
  }

  // Determine Socrates' students by using the DataFrame-API
  def studentsFromSokratesDFAPI: List[Student] = ???

  // Determine the number of the C4-profs that don't
  // give any lectures
  def countAllC4ProfessorsGivingNoLecture: Long = ???
//  {
//    val p = professorenDF.where($"rang"==="C4").select($"persnr")
//    val v = vorlesungenDF.select("gelesenVon")
//    val res=p.except(v).agg(count($"persnr")).first
//    res.getLong
//  }

  // count the number of lectures per student
  // result: tuple (matrnr, name, number)
  def numberOfLecturePerStudent: List[(Int, String, Long)] = ???
//  {
//    val res=studentenDF.join(hoerenDF, studentenDF("matrnr")===hoerenDF("matrnr"))
//      .groupBy(studentenDF("matrnr"), $"name")
//      .agg(count)
//  }

  //Determine all students hearing the lecture ("Grundzuege")
  // use the hoerenBig-Dataset
  // execute the where operation after the joins
  def studentsHearingGrundzuege: List[Student] = ???

  //Determine all students hearing the lecture ("Grundzuege")
  // use the hoerenBig-Dataset
  // execute the where operation before the join
  def studentsHearingGrundzuegeWhereBefore: List[Student] = ???
}
