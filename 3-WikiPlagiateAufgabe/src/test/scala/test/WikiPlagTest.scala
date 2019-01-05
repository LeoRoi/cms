package test

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.Row
import wikiplag._

@RunWith(classOf[JUnitRunner])
class WikiPlagTest extends FunSuite with BeforeAndAfterAll {

  var conf: org.apache.spark.SparkConf = _
  var sc: SparkContext = _
  var parsed_articles: RDD[(Long, String, List[String])] = _
  var word_index: Map[String, List[(Long, Int)]] = _

  val data1 = List((1L, "Titel 1", List("Dies", "ist", "ein", "Text", "und", "jetzt", "kommt",
    "noch", "ein", "Text")))
  val data2 = List((2L, "Titel 2", List("Die", "Gulaschprogrammiernacht", "ist", "eine",
    "viertägige", "Konferenz", "vom", "Chaos", "Computer", "Club")))
  val data3 = List((3L, "Titel 3", List("Der", "Döner", "besteht", "aus", "mit", "Marinade", "gewürzten", "Fleisscheiben", "die",
    "schichtweise", "auf", "einen", "senkrecht", "stehenden", "Drehspieß", "gesteckt", "und",
    "seitlich", "gegrillt", "werden")))
  val data4 = List((4L, "Titel 4", List("Der",
    "Döner", "beinhaltet", "mit", "Marinade", "gewürzte", "Fleisscheiben", "die",
    "schichtweise", "auf", "einen", "senkrecht", "stehenden", "Drehspieß", "gesteckt", "und",
    "dann", "gegrillt", "werden")))

  val testdata = data1 ++ data2 ++ data3 ++ data4
  val eStopWords = List("der", "die", "das", "und", "aus", "auf")
  val invInd = List(("einen", List((3, 7), (4, 7))), ("seitlich", List((3, 12))), ("stehenden", List((3, 9), (4, 9))),
    ("gewürzten", List((3, 4))), ("schichtweise", List((3, 6), (4, 6))), ("senkrecht", List((3, 8), (4, 8))),
    ("chaos", List((2, 6))), ("ist", List((1, 1), (2, 1))), ("club", List((2, 8))), ("ein", List((1, 2), (1, 7))),
    ("gegrillt", List((3, 13), (4, 13))), ("eine", List((2, 2))), ("marinade", List((3, 3), (4, 3))), ("döner", List((3, 0), (4, 0))),
    ("dies", List((1, 0))), ("text", List((1, 3), (1, 8))), ("computer", List((2, 7))), ("beinhaltet", List((4, 1))),
    ("noch", List((1, 6))), ("gulaschprogrammiernacht", List((2, 0))), ("konferenz", List((2, 4))), ("mit", List((3, 2), (4, 2))),
    ("gewürzte", List((4, 4))), ("werden", List((3, 14), (4, 14))), ("fleisscheiben", List((3, 5), (4, 5))),
    ("besteht", List((3, 1))), ("dann", List((4, 12))), ("kommt", List((1, 5))), ("jetzt", List((1, 4))), ("vom", List((2, 5))),
    ("gesteckt", List((3, 11), (4, 11))), ("viertägige", List((2, 3))), ("drehspieß", List((3, 10), (4, 10)))).sortBy(_._1)

  override protected def beforeAll() {

//    local execution with 4 processes
    conf = new SparkConf().setMaster("local[4]").setAppName("WikiPlagTest")
    conf.set("spark.executor.memory", "8g")
    conf.set("spark.driver.memory", "4g")
    sc = new SparkContext(conf)

    parsed_articles = Utilities.getData("wiki_articlesShort.xml", "resources", sc).cache
    if (parsed_articles == null) {
      println("RDD is null!"); System.exit(0)
    }
    println("Anzahl:" + parsed_articles.count)
  }

  test("Parsing Successful") {

    assert(parsed_articles.count == 383)
  }

  test("Top X words") {

    val words = WikiPlagFuns.getTopXWords(parsed_articles, 10)
    val expresult = List(("der", 28753), ("und", 24243), ("die", 21400), ("in", 14868), ("von", 11657),
      ("den", 8626), ("des", 8011), ("mit", 6957), ("im", 6009), ("zu", 5911))
    assert(words == expresult)
  }

  test("extractIndexes Test") {

    val res = WikiPlagFuns.extractIndexes(data3.head, eStopWords).sortBy(_._2._2)
    val exp = List(("döner", (3, 0)), ("besteht", (3, 1)), ("mit", (3, 2)), ("marinade", (3, 3)), ("gewürzten", (3, 4)),
      ("fleisscheiben", (3, 5)), ("schichtweise", (3, 6)), ("einen", (3, 7)), ("senkrecht", (3, 8)), ("stehenden", (3, 9)),
      ("drehspieß", (3, 10)), ("gesteckt", (3, 11)), ("seitlich", (3, 12)), ("gegrillt", (3, 13)), ("werden", (3, 14)))
    assert(res == exp)
  }

  test("Create Inverse Index Test") {

    val res = WikiPlagFuns.createIndex(sc.parallelize(testdata), eStopWords).toList.sortBy(_._1)
    assert(res === invInd)
  }

  test("Create Inverse Index with Accumulator Test") {
    val swa = new StopWordAccumulator
    val acc = sc.register(swa, "StopWordAccumulator")
    val res = WikiPlagFuns.createIndexWithAccumulator(sc.parallelize(testdata), eStopWords, swa).toList.sortBy(_._1)
    val resacc = List(("auf", List(3, 4)), ("aus", List(3)), ("der", List(3, 4)), ("die", List(2, 3, 4)), ("und", List(1, 3, 4)))
    val a = swa.value.toList.sortBy(_._1).map(x => (x._1, x._2.sorted))
    val res_s = res.map(x => (x._1, x._2.sortWith((el1, el2) => {
      if (el1._1 < el2._1) true
      else if (el1._1 == el2._1 && el1._2 < el2._2) true
      else false
    })))
    assert(res_s === invInd)
    println("acc = " + acc)
    println("swa = " + swa.value.toList)
  }

  test("Create Inverse Index Test with broadcast") {
    val bcstopWords: Broadcast[List[String]] = sc.broadcast(eStopWords)
    val res = WikiPlagFuns.createIndex(sc.parallelize(testdata), eStopWords).toList.sortBy(_._1)
    val res_s = res.map(x => (x._1, x._2.sortWith((el1, el2) => {
      if (el1._1 < el2._1) true
      else if (el1._1 == el2._1 && el1._2 < el2._2) true
      else false
    })))
    assert(res_s === invInd)
  }

  test("Create Inverse Index with Accumulator Action Test") {
    val swa = new StopWordAccumulator
    val acc = sc.register(swa, "StopWordAccumulator")
    val res = WikiPlagFuns.createIndexWithAccumulatorAction(sc.parallelize(testdata), eStopWords, swa).toList.sortBy(_._1)
    val resacc = List(("auf", List(3, 4)), ("aus", List(3)), ("der", List(3, 4)), ("die", List(2, 3, 4)), ("und", List(1, 3, 4)))

    val a = swa.value.toList.sortBy(_._1).map(x => (x._1, x._2.sorted))
    val res_s = res.map(x => (x._1, x._2.sortWith((el1, el2) => {
      if (el1._1 < el2._1) true
      else if (el1._1 == el2._1 && el1._2 < el2._2) true
      else false
    })))
    assert(res_s === invInd)
  }

  test("Create Inverse Index") {

    val stopWords = WikiPlagFuns.getTopXWords(parsed_articles, 10).map(_._1)
    word_index = WikiPlagFuns.createIndex(parsed_articles, stopWords)
  }

  override protected def afterAll() {

    if (sc != null) {
      sc.stop; println("Spark stopped......")
    }
    else println("Cannot stop spark - reference lost!!!!")
  }
}
