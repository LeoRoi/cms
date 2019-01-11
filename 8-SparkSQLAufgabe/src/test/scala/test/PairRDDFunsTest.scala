package test

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import sparksqlFuns._

@RunWith(classOf[JUnitRunner])
class PairRDDFunsTest extends FunSuite with BeforeAndAfterAll{
  
  var conf:org.apache.spark.SparkConf=_
  var sc:SparkContext=_
  var pairs:PairRDDFuns= _

  override protected def beforeAll() {

    conf= new SparkConf().setMaster("local[4]").setAppName("PairRDDFuns")
    conf.set("spark.executor.memory","4g")
    conf.set("spark.storage.memoryFraction","0.8")
    conf.set("spark.driver.memory", "2g")

    sc= new SparkContext(conf)
    pairs= new PairRDDFuns(sc)
      
  }
  
  test("Initialization Test"){

    val rdd= sc.parallelize( pairs.hoeren_big, 4)
    val res= rdd.filter(_.vorlnr==5001)
    assert(res.count===1)
    assert(res.first.matrnr===28106)  
  }
  
  test("Determine students from Sokrates"){
    
    val res= pairs.studentsFromSocrates
    res.foreach(println)
    val r=(res.map(x=>x.matrnr)).sorted
    assert(r===List(27550,28106,29120,29120))
  }
  
  test("Students hearing no lecture"){
    
    val res= pairs.studentsHearingNoLecture
    val r=(res.map(x=>x.matrnr)).sorted
    assert(r===List(24002,26830))
  }
  
  test("Students attending to Grundzuege"){
   
    val res=pairs.studentsHearingGrundzuege
    assert(res.head.matrnr===28106)
  }
  
  test("Students attending to Grundzuege Cartesian Product"){
   
    val res=pairs.studentsHearingGrundzuegeCartesianProduct
    assert(res.head.matrnr===28106)
  }
  
  override protected def afterAll() {

     if (sc!=null) {sc.stop; println("Spark stopped......")}
     else println("Cannot stop spark - reference lost!!!!")
  }

}

