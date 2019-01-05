package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner
import twitter._

@RunWith(classOf[JUnitRunner])
class TwitterAnalyzerTest extends FunSuite with BeforeAndAfterAll {

  var conf: org.apache.spark.SparkConf = _
  var sc: SparkContext = _

  var twitterData: RDD[String] = _
  var onlyTweets: RDD[Row] = _
  var TWA: TwitterAnalyzer = _


  override protected def beforeAll() {

    conf = new SparkConf().setMaster("local[4]").setAppName("TwitterTest")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "2g")
    sc = new SparkContext(conf)

    twitterData = Utilities.getData("tweets-big.txt", "resources", sc)
    onlyTweets = twitterData.flatMap(x => {
      Utilities.parse(x)
    }).cache
    println(onlyTweets.count)
    TWA = new TwitterAnalyzer(onlyTweets)
  }

  test("Number of German Tweets") {

    val nr = TWA.getGermanTweetsCount
    assert(nr === 90)

  }

  test("number of tweets per user") {

    val res = TWA.numberOfGermanTweetsPerUser
    assert(res.forall(x => (x._2 == 5)))
  }

  test("Texts of the German Tweets") {

    val data = TWA.getGermanTweetTexts
    //data.foreach(println)
    assert(data.contains("Oh wie ich dich liebe PAX\n#pax #ikea #ikeapaxsystem #paxcloset #michaelkors #longchamp #louisvuitton #rayban #lac"))
    assert(data.contains("Ein neuer Erfolg: Landwirt! Versuche doch auch dein Glück! http://t.co/fNkt54x3FG #android,#androidgames,#gameinsight"))

  }

  test("Extraction of Hashtags") {

    val tweet = "This is a Hashtag #super #mega and this is another #hashtag"
//    val res = ("#hashtag", "#mega", "#super") // call your own function
    val res = TwitterAnalyzer.getHashtags(tweet)
//    assert(res === List("#hashtag", "#mega", "#super"))
    assert(res === List("#super", "#mega", "#hashtag"))
  }

  test("Top ten hashtags") {

    val ht = TWA.getTopTenHashtags
    ht.foreach(println)
    assert(ht.head._2 == 65 || ht.tail.head._2 == 65)
  }

}
