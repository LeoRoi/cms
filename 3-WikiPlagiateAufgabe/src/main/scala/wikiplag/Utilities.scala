package wikiplag

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

object Utilities {

  val hadoopfs = "hdfs://localhost:9000/user/hendrik/"
  val FILENAME_ACCESS_PATTERN ="""^(.+)\/(\w+\.\w+)$""".r

  def getData(filename: String, source: String, sc: SparkContext): RDD[(Long, String, List[String])] = {

    if (source.equals("resources")) {

      val url = getClass.getResource("/" + filename).getPath
      val sqlContext = new SQLContext(sc)
      val df = sqlContext.read
        .format("com.databricks.spark.xml")
        .option("rowTag", "page")
        .load(url)

      df.filter("ns = 0")
        .select("id", "title", "revision.text").rdd.
        map(X => (X.getLong(0).toInt, X.getString(1),
          WikiDumpParser.extractPlainText(WikiDumpParser.parseXMLWikiPage(X.getStruct(2).getString(0)))))
    }
    else {
      System.err.printf("Only Filesystem Access is implemented!");
      System.exit(0);
      null
    }
  }
}