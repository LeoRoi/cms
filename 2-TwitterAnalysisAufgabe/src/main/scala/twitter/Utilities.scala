package twitter

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.util.parsing.json.JSON

object Utilities {
  val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss X uuuu", Locale.ENGLISH)
  // please enter your hadoop user path  - if necessary
  val hadoopfs = "hdfs://localhost:9000/user/hendrik/"
  val FILENAME_ACCESS_PATTERN ="""^(.+)\/(\w+\.\w+)$""".r

  def getData(filename: String, source: String, sc: SparkContext): RDD[String] = {
    if (source.equals("resources")) {
      val url = getClass.getResource("/" + filename).getPath
      sc.textFile("file://" + url)
    }
    else if (source.equals("hadoop-fs")) {
      sc.textFile(hadoopfs + filename)
    }
    else null
  }

  /*
   *
{
  "created_at": "Thu Apr 06 15:24:15 +0000 2017",
  "id_str": "850006245121695744",
  "text": "1\/ Today we\u2019re sharing our vision for the future of the Twitter API platform!\nhttps:\/\/t.co\/XweGngmxlP",
  "user": {
    "id": 2244994945,
    "name": "Twitter Dev",
    "screen_name": "TwitterDev",
    "location": "Internet",
    "url": "https:\/\/dev.twitter.com\/",
    "description": "Your official source for Twitter Platform news, updates & events. Need technical help? Visit https:\/\/twittercommunity.com\/ \u2328\ufe0f #TapIntoTwitter"
  },
  "place": {
  },
  "entities": {
    "hashtags": [
    ],
    "urls": [
      {
        "url": "https:\/\/t.co\/XweGngmxlP",
        "unwound": {
          "url": "https:\/\/cards.twitter.com\/cards\/18ce53wgo4h\/3xo1c",
          "title": "Building the Future of the Twitter API Platform"
        }
      }
    ],
    "user_mentions": [
    ]
  },
  "lang":"en"
}

  Please analyse the Twitter Data Format and write a function parse that extracts
  the date, the username, the text and the language of a tweet.

  The input of the function is a string that represents a json document.
  You need to parse this document by using the scala.util.parsing.JSON library.
  Attention: Not all json documents are tweets actually.

  Result of the function should be a Row consisting of four elements:
  index 0 (Type: Date) - Date of the tweet (you can use the given function for parsing
  index 1 (Type: String) - Name fthe user that created the tweet
  index 2 (Type: String) - Text of the tweet
  index 3 (Type: String) - Language of the tweet
   */
  def parse(line: String): List[Row] = {
    //  def parse(line: String) = {
    val tweet = JSON.parseFull(line)
    tweet match {
      case Some(m) => val map = m.asInstanceOf[Map[String, Any]]
        (
          map.get("created_at"), map.get("user"), map.get("text"), map.get("lang")
        ) match {
          case (Some(data: String), Some(user: Map[String, Any]), Some(text: String), Some(lang: String)) =>
            //            println(getTwitterDate(data), user.getOrElse("name", ""), text, lang)
            List(Row(getTwitterDate(data), user.getOrElse("name", ""), text, lang))
          case _ => List()
        }
      case None => List()
    }
  }

  /*
      Option
      /   \
      /     \
      /       \
      Some     None
  */
  def getTwitterDate(date: String): OffsetDateTime = {
    try {
      OffsetDateTime.parse(date, dtf)
    } catch {
      case e: Exception => {
        println(date);
        OffsetDateTime.now
      }
    }
  }
}