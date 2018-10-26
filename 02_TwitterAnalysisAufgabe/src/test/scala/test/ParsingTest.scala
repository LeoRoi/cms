package test

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import org.junit.runner.RunWith
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.Row
import java.time.OffsetDateTime
import twitter._

@RunWith(classOf[JUnitRunner])
class ParsingTest extends FunSuite with BeforeAndAfterAll {

  var conf: org.apache.spark.SparkConf = _
  var sc: SparkContext = _

  var twitterData: RDD[String] = _

  override protected def beforeAll() {

    conf = new SparkConf().setMaster("local[4]").setAppName("TwitterTest")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "2g")
    sc = new SparkContext(conf)

    twitterData = Utilities.getData("tweets-big.txt", "resources", sc)
    println("Number of Datasets:" + twitterData.count)
  }

  test("Date Parsing Successful") {

    val date = "Sun Sep 21 15:05:39 +0000 2014"
    val TDate = Utilities.getTwitterDate(date)
    assert(TDate.toString === "2014-09-21T15:05:39Z")

  }

  test("Parsing Test") {


    val tweet = "{\"created_at\":\"Sun Sep 21 15:05:39 +0000 2014\",\"id\":513705624245653505,\"id_str\":\"513705624245653505\",\"text\":\"This is a tweet\",\"truncated\":false,\"user\":{\"id\":2533019970,\"name\":\"donald\"},\"lang\":\"en\"}"

    val res = Utilities.parse(tweet)
    assert(res.head.get(0).asInstanceOf[OffsetDateTime].toString === ("2014-09-21T15:05:39Z"))
    assert(res.head.getString(1) === ("donald"))
    assert(res.head.getString(2) === ("This is a tweet"))
    assert(res.head.getString(3) === ("en"))

  }

//  test("Parse All") {
//
//    val onlyTweets = twitterData.flatMap(x => {
//      Utilities.parse(x)
//    }).cache
//
//    assert(twitterData.count === 21326)
//    assert(onlyTweets.count === 17570)
//  }

  override protected def afterAll() {

    if (sc != null) {
      sc.stop; println("Spark stopped......")
    }
    else println("Cannot stop spark - reference lost!!!!")
  }
}
