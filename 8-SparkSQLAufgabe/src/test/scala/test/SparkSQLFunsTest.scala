package test

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import sparksqlFuns._

@RunWith(classOf[JUnitRunner])
class SparkSQLFunsTest extends FunSuite with BeforeAndAfterAll{
  
  var conf:org.apache.spark.SparkConf=_
  var sc: SparkContext= _ 
  var ss:SparkSession=_
  var sqlFuns:SparkSQLFuns= _

  override protected def beforeAll() {

    ss= SparkSession.builder.appName("SparkSQLApp").
      config("spark.sql.crossJoin.enabled","true").
      master("local[4]").getOrCreate
    sqlFuns= new SparkSQLFuns(ss,ss.sparkContext)
      
  }
  
  test("Find Sokrates' students (SQL-Literal)"){

    
     val sol= List(Student(27550,"Schopenhauer",6),Student(28106,"Carnap",3),Student(29120,"Theophrastos",2),
         Student(29120,"Theophrastos",2))
         
     val res=sqlFuns.studentsFromSocrates.sortBy(x=>x.matrnr)
     assert(res===sol)     
  }
  
  test("Find Sokrates' students (DataFrame-API)"){
    
     val sol= List(Student(27550,"Schopenhauer",6),Student(28106,"Carnap",3),Student(29120,"Theophrastos",2),
         Student(29120,"Theophrastos",2))
         
     val res=sqlFuns.studentsFromSokratesDFAPI.sortBy(x=>x.matrnr)
     assert(res===sol)
  }
  
  test("count the C4-professors giving no lecture"){
    
    val res= sqlFuns.countAllC4ProfessorsGivingNoLecture
    assert(res===1)
  }
  
  test("Count the number of students per lecture"){
    
    val sol= List((25403,"Jonas",1),(26120,"Fichte",1),(27550,"Schopenhauer",2),(28106,"Carnap",4),
             (29120,"Theophrastos",3),(29555,"Feuerbach",2))
    val res= sqlFuns.numberOfLecturePerStudent.sortBy(_._1)
    assert(res===sol) 
  }
  
  test("Students attending to Grundzuege (Join before where)"){
   
    val res=sqlFuns.studentsHearingGrundzuege
    assert(res.length===1)
    assert(res.head.matrnr===28106)
  }
  
  test("Students attending to Grundzuege (where before join)"){
   
    val res=sqlFuns.studentsHearingGrundzuege
    assert(res.length===1)
    assert(res.head.matrnr===28106)
  }
    
  override protected def afterAll() {

     if (ss!=null) {ss.stop; println("Spark stopped......")}
     else println("Cannot stop spark - reference lost!!!!")
  }
}
