package twitter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.time.OffsetDateTime

//index 0 (Type: Date)
//index 1 (Type: String) - Name fthe user that created the tweet
//index 2 (Type: String) - Text of the tweet
//index 3 (Type: String) - Language of the tweet

class TwitterAnalyzer(tData: RDD[Row]) {

  /*
   * Write a function that counts the number of tweets using the german lanuage
   */
  def getGermanTweetsCount: Long =
    tData.map(x => x.getString(3))
    .filter(x => x == "de")
    .count()

  /*
   * Write a function that extracts the texts of all german tweets
   */
  def getGermanTweetTexts: Array[String] =
    tData.map(x => (x.getString(2), x.getString(3)))
      .filter(x => x._2 == "de")
      .map(x => x._1)
      .collect()

  /*
 * Write a function that counts the number of german tweets that users created
 */
  def numberOfGermanTweetsPerUser: Array[(String, Int)] =
    tData.map(x => (x.getString(1), x.getString(3)))
      .filter(x => x._2 == "de")
      .map(x => x._1)
      .groupBy(x => x)
      .map(x => (x._1, x._2.size))
      .collect()

  /*
  * Write a function that finds the top ten hashtags by extracting them from their texts
  */
  def getTopTenHashtags: List[(String, Int)] = {
    val textData = tData.flatMap(x => TwitterAnalyzer.getHashtags(x.getString(2)))

    textData.groupBy(x => x)
      .map(x => (x._1, x._2.size))
      .collect().toList
      .sortWith((x, y) => x._2 > y._2)
      .take(10)
  }
}

object TwitterAnalyzer {


  def getHashtags(text: String): List[String] = {


    if (text.isEmpty || text.length == 1) List()
    else if (text.head == '#') {
      val tag = text.takeWhile(x => (x != ' '))
      val rest = text.drop(tag.length)
      tag :: getHashtags(rest)
    }
    else getHashtags(text.tail)
  }
}

