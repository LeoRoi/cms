package wikiplag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object WikiPlagFuns {

  // Write a function that finds the top X words .
  // docId, title, list of words
  def getTopXWords(data: RDD[(Long, String, List[String])], x: Int): List[(String, Int)] =
    data.flatMap(_._3)
      .groupBy(x => x)
      .map(x => (x._1, x._2.size))
      .sortBy(_._2, false)
      .take(x).toList

  /*
   *  Write a function that extracts all word occurrences in a document by
   *      - deleting all stop-words and
   *      - extracting all word indexes (docID, list index)
 */
  def addIndexes(doc: (Long, String, List[String]), stopWords: List[String]):
  List[(String, (Long, Int))] = {
    def loop(i: Int, id: Long, title: String, tail: List[String], acc: List[(String, (Long, Int))]):
    List[(String, (Long, Int))] = tail match {
      case Nil => acc
      case word::tail => loop(i+1, id, title, tail, acc :+ (word, (id, i)))
    }

    loop(0, doc._1, doc._2, doc._3.map(_.toLowerCase).diff(stopWords), List())
  }

  /*
   *  Write a function that creates an Inverse Index.
   *  The Inverse Index should contain all words as the key of the map
   *  and all occurrences (document id, index) that word
 */

  def createInverseIndex(data: RDD[(Long, String, List[String])], stopWords: List[String]):
  Map[String, List[(Long, Int)]] = {
    val dataPrep = data.map(x => (x._1, x._2, x._3.map(_.toLowerCase).diff(stopWords)))
      .flatMap(x => addIndexes(x, stopWords))
      .collect().toList

    dataPrep.foldLeft(Map[String, List[(Long, Int)]]())((acc, e) =>
      acc + (e._1 -> (acc.getOrElse(e._1, List()) :+ (e._2._1, e._2._2))))
  }

  /*
   *  Write a function that creates an Inverse Index.
   *  Use an accumulator for counting all elements that are deleted from the document.
   *  Send a tuple (String=stop-word, Long=docID) for each delete.
 */
  def createIndexWithAccumulator(data: RDD[(Long, String, List[String])],
                                  stopWords: List[String],
                                  swa: StopWordAccumulator): Map[String, List[(Long, Int)]] = {
    val sw = data.map(x => (x._1, x._2, x._3.map(_.toLowerCase)))
      .flatMap(x => addIndexes(x, stopWords))
      .filter(x => stopWords.contains(x._1))
      .map(x => swa.add(x._1, x._2._1))
    println(swa.value)

    createInverseIndex(data, stopWords)
  }

  def extractIndexesWithAccumulator(doc: (Long, String, List[String]),
                                    stopWords: List[String],
                                    swa: StopWordAccumulator): List[(String, (Long, Int))] = ???

  /*
  *  Write a function that creates an Inverse Index.
  *  Use a broadcast variable for the distribution of the stop words.
*/
  def createIndexWithBroadcast(data: RDD[(Long, String, List[String])], stopWords: Broadcast[List[String]]):
  Map[String, List[(Long, Int)]] = {
    val dataPrep = data.map(x => (x._1, x._2, x._3.map(_.toLowerCase).diff(stopWords.value)))
      .flatMap(x => addIndexes(x, stopWords.value)).collect().toList

    dataPrep.foldLeft(Map[String, List[(Long, Int)]]())((acc, e) =>
      acc + (e._1 -> (acc.getOrElse(e._1, List()) :+ (e._2._1, e._2._2))))
  }

  def extractIndexesWithBroadcast(doc: (Long, String, List[String]), stopWords: Broadcast[List[String]]):
  List[(String, (Long, Int))] = {
    def slave(i: Int, id: Long, title: String, tail: List[String], acc: List[(String, (Long, Int))]):
    List[(String, (Long, Int))] = tail match {
    case h :: t => slave(i+1, id, title, t, acc :+ (h, (id, i)))
    case Nil => acc
  }

  slave(0, doc._1, doc._2, doc._3.map(_.toLowerCase).diff(stopWords.value), List())
}

  /*
   *  Write a function that creates an Inverse Index.
   *  Use an accumulator for counting all elements that are deleted from the document.
   *  Send a tuple (String, Long) for each delete.
   *  Use the accumulator only in an action.
 */
  def createIndexWithAccumulatorAction(data: RDD[(Long, String, List[String])],
                                       stopWords: List[String],
                                       swa: StopWordAccumulator): Map[String, List[(Long, Int)]] = {
    val sw = data.map(x => (x._1, x._2, x._3.map(_.toLowerCase)))
        .flatMap(x => addIndexes(x, stopWords))
        .filter(x => stopWords.contains(x._1))
        .foreach(x => swa.add(x._1, x._2._1))
//    sw.collect { case x => swa.add(x._1, x._2._1) }

//    aggreate
    println(swa.value)

    createInverseIndex(data, stopWords)
  }
}

