package textanalyse

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

class EntityResolution(sc: SparkContext, dat1: String, dat2: String, stopwordsFile: String, goldStandardFile: String) {

  // tuples where 1=ID and 2=text
  val amazonRDD: RDD[(String, String)] = Utils.getData(dat1, sc)
  val googleRDD: RDD[(String, String)] = Utils.getData(dat2, sc)

  val stopWords: Set[String] = Utils.getStopWords(stopwordsFile)
  val goldStandard: RDD[(String, String)] = Utils.getGoldStandard(goldStandardFile, sc)

  // tuples where 1=ID and 2=tokens (list of all words)
  var amazonTokens: RDD[(String, List[String])] = _
  var googleTokens: RDD[(String, List[String])] = _
  var corpusRDD: RDD[(String, List[String])] = _

  // inverse document index ie occurrence/value factor of WORD: high=seldom, low=often
  var idfDict: Map[String, Double] = _

  var idfBroadcast: Broadcast[Map[String, Double]] = _
  var similarities: RDD[(String, String, Double)] = _

  /*
   * getTokens soll die Funktion tokenize auf das gesamte RDD anwenden
   * und aus allen Produktdaten eines RDDs die Tokens extrahieren.
   * IN: 1 >> id, _2 >> "title","description","manufacturer"
   * OUT: id, list of words from _2
   */
  def getTokens(data: RDD[(String, String)]): RDD[(String, List[String])] = {
    val stopWords_ = stopWords
    data.map(x => (x._1, EntityResolution.tokenize(x._2, stopWords_)))
  }

  /*
   * Zählt alle Tokens innerhalb eines RDDs
   * Duplikate sollen dabei nicht eliminiert werden
   */
  def countTokens(data: RDD[(String, List[String])]): Long =
    data.map(x => x._2.size).sum().toLong

  /*
   * Findet den Datensatz mit den meisten Tokens
   */
  def findBiggestRecord(data: RDD[(String, List[String])]): (String, List[String]) =
    data.sortBy(_._2.size, false).take(1).head

  /*
   * Erstellt die Tokenmenge für die Amazon und die Google-Produkte
   * (amazonRDD und googleRDD), vereinigt diese und speichert das
   * Ergebnis in corpusRDD (id, list of words)
   */
  def createCorpus(): Unit = {
    amazonTokens = getTokens(amazonRDD)
    googleTokens = getTokens(googleRDD)
    corpusRDD = amazonTokens ++ googleTokens
  }

  /*
   * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
   * Speichern des Dictionaries in die Variable idfDict
   *
   * docAmount / number of docs the word occurs in
   * IN var corpusRDD: RDD[(String, List[String])] = _
   * OUT var idfDict: Map[String, Double] = _
   */
  def calculateIDF(): Unit = {
    val docAmount = corpusRDD.count.toDouble //each line is a doc
    val wordSet = corpusRDD.map(row => row._2)
      .reduce((doc1, doc2) => doc1 ::: doc2).toSet

    idfDict = wordSet.map(word =>
      (word, docAmount / corpusRDD.map(row => row._2)
        .filter(doc => doc.contains(word)).count.toDouble))
      .toMap
  }

  /**
    * Berechnung der Document-Similarity für alle möglichen
    * Produktkombinationen aus dem amazonRDD und dem googleRDD.
    * Ergebnis ist ein RDD aus Tripeln bei dem an erster Stelle die AmazonID
    * steht, an zweiter die GoogleID und an dritter der Wert.
    */
  def simpleSimilarityCalculation: RDD[(String, String, Double)] = {
    val amazonRDD_ = amazonRDD
    val googleRDD_ = googleRDD
    val similarity = EntityResolution.computeSimilarity(_: ((String, String), (String, String)),
      _: Map[String, Double], _: Set[String])

    val stopWords_ = stopWords
    val idfDict_ = idfDict

    amazonRDD_.cartesian(googleRDD_)
      .map(x => similarity(x, idfDict_, stopWords_))
  }

  def simpleSimilarityCalculationDonat: RDD[(String, String, Double)] = {
    val amazonRDD_ = amazonRDD
    val googleRDD_ = googleRDD
    val similarity = EntityResolution.computeSimilarity(_: ((String, String), (String, String)),
      _: Map[String, Double], _: Set[String])

    val stopWords_ = stopWords
    val idfDict_ = idfDict

    val res = amazonRDD_.cartesian(googleRDD_)
      .map(x => similarity(x, idfDict_, stopWords_))

    similarities = res
    res
  }

  /*
   * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
   */
  def findSimilarity(vendorID1: String, vendorID2: String,
                     sim: RDD[(String, String, Double)]): Double =
    sim.filter(x => x._1 == vendorID1 && x._2 == vendorID2)
      .take(1).head._3

  def simpleSimilarityCalculationWithBroadcast: RDD[(String, String, Double)] =
    simpleSimilarityCalculation

  /*
   * Gold Standard Evaluation
   * Berechnen Sie die folgenden Kennzahlen:
   *
   * Anzahl der Duplikate im Sample
   * Durchschnittliche Consinus Similaritaet der Duplikate
   * Durchschnittliche Consinus Similaritaet der Nicht-Duplikate
   *
   * Ergebnis-Tripel:
   * (AnzDuplikate, avgCosinus-SimilaritätDuplikate, avgCosinus-SimilaritätNicht-Duplikate)
   */
  def evaluateModel(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {

    // structure: col 1 = A+G, (col 2 = similarity, col 3 = A+G gold)
    val leftJoin = simpleSimilarityCalculation.map(x => (x._1 + " " + x._2, x._3))
      .leftOuterJoin(goldStandard)

    // ACCUMULATORS
    val duplicatesAcc = sc.doubleAccumulator("duplicatesAcc")
    val duplicatesAccCounter = sc.longAccumulator("duplicatesAccCounter")

    val originAcc = sc.doubleAccumulator("originAcc")
    val originAccCounter = sc.doubleAccumulator("originAccCounter")

    leftJoin.foreach {
      case (sim, (value, Some(gold))) => {
        duplicatesAcc.add(value)
        duplicatesAccCounter.add(1)
      }
      case (sim, value) => {
        originAcc.add(value._1)
        originAccCounter.add(1)
      }
    }

    (duplicatesAccCounter.value,
      duplicatesAcc.value / duplicatesAccCounter.value,
      originAcc.value / originAccCounter.value)
  }

  def evaluateModelDonat(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {

    //extract actual ids from RDD
    val gs = goldStandard.map(x => x._1).map(x => (x.split(" ")(0), x.split(" ")(1)))

    //convert similarities to a map, where the keys are a tuple of the two ids
    val simMap = similarities.map(x => ((x._1, x._2), x._3)).collect().toMap

    //intersect the goldStandard id tuples with the keys of the just created map
    val keysOfDuplicates = simMap.keys.toSet.intersect(gs.collect().toSet)

    //extract duplicates and notDuplicates by checking, whether the keysOfDuplicates contains the respective key of the map
    val duplicates = simMap.filter(x => keysOfDuplicates.contains(x._1))
    val notDuplicates = simMap.filterNot(x => keysOfDuplicates.contains(x._1))


    val num_dup = duplicates.size

    //extract similarities and mean them
    val cos_dup = duplicates.map(x => x._2).toList.foldLeft(0.0)((x, y) => x + y) / num_dup
    val cos_not_dup = notDuplicates.map(x => x._2).toList.foldLeft(0.0)((x, y) => x + y) / notDuplicates.size

    (num_dup, cos_dup, cos_not_dup)
  }

  def evaluateModelK(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {
    val leftJoin = simpleSimilarityCalculation.map(x => (x._1 + " " + x._2, x._3))
      .leftOuterJoin(goldStandard).cache()

    val duplicates = leftJoin.filter(x => {
      x._2._2 match {
        case None => false
        case _ => true
      }
    })

    val origins = leftJoin.filter(x => {
      x._2._2 match {
        case None => true
        case _ => false
      }
    })

    (duplicates.count(), duplicates.map(x => x._2._1).mean(), origins.map(x => x._2._1).mean())
  }
}

object EntityResolution {
  /*
   * Tokenize splittet einen String in die einzelnen Wörter auf
   * und entfernt dabei alle Stopwords.
   * Verwenden Sie zum Aufsplitten die Methode Utils.tokenizeString
   */
  def tokenize(s: String, stopws: Set[String]): List[String] =
    Utils.tokenizeString(s).filterNot(stopws)

  /*
   * Berechnet die Relative Haeufigkeit eine Wortes in Bezug zur
   * Menge aller Wörter innerhalb eines Dokuments
   */
  def getTermFrequencies(tokens: List[String]): Map[String, Double] =
    tokens.groupBy(x => x)
      .map(x => (x._1, x._2.size / tokens.size.toDouble))

  /*
   * Bererechnung der Document-Similarity einer Produkt-Kombination
   * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem
   * Sie die erforderlichen Parameter extrahieren
   */
  def computeSimilarity(record: ((String, String), (String, String)),
                        idfDictionary: Map[String, Double],
                        stopWords: Set[String]): (String, String, Double) = {
    // split input tuple
    val entryOne = record._1
    val entryTwo = record._2

    (entryOne._1, entryTwo._1,
      calculateDocumentSimilarity(entryOne._2, entryTwo._2, idfDictionary, stopWords))
  }

  /*
  * Berechnung von TF-IDF Wert für eine Liste von Wörtern >> DOC VECTOR
  * Ergebnis ist eine Map die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
  */
  def calculateTF_IDF(terms: List[String], idfDictionary: Map[String, Double]): Map[String, Double] =
    getTermFrequencies(terms).map(tf => (tf._1, tf._2 * idfDictionary(tf._1)))

  /*
   * Berechnung des Dot-Products von zwei Vectoren
   */
  def calculateDotProduct(v1: Map[String, Double], v2: Map[String, Double]): Double =
//    v1.keys.toSet.intersect(v2.keys.toSet).map(key => v1(key) * v2(key)).sum
//    v1.map { case (k, v) => (k, v * v2.getOrElse(k, .0)) }.values.sum
    v1.map(x => (x._1, x._2 * v2.getOrElse(x._1, .0))).values.sum

  /*
   * Berechnung der Norm eines Vectors
   */
  def calculateNorm(vec: Map[String, Double]): Double =
    Math.sqrt((for (el <- vec.values) yield el * el).sum)

  /*
   * Berechnung der Cosinus-Similarity für zwei Vectoren
   */
  def calculateCosinusSimilarity(doc1: Map[String, Double], doc2: Map[String, Double]): Double = {
    calculateDotProduct(doc1, doc2) / (calculateNorm(doc1) * calculateNorm(doc2))
  }

  /*
   * Berechnung der Document-Similarity für ein Dokument
   */
  def calculateDocumentSimilarity(doc1: String, doc2: String,
                                  idfDictionary: Map[String, Double],
                                  stopWords: Set[String]): Double = {

    val vec1 = calculateTF_IDF(tokenize(doc1, stopWords), idfDictionary)
    val vec2 = calculateTF_IDF(tokenize(doc2, stopWords), idfDictionary)

    calculateCosinusSimilarity(vec1, vec2)
  }

  /*
   * Bererechnung der Document-Similarity einer Produkt-Kombination
   * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem
   * Sie die erforderlichen Parameter extrahieren
   * Verwenden Sie die Broadcast-Variable.
   */
  def computeSimilarityWithBroadcast(record: ((String, String), (String, String)),
                                     idfBroadcast: Broadcast[Map[String, Double]],
                                     stopWords: Set[String]): (String, String, Double) = {

    val entryOne = record._1
    val entryTwo = record._2

    (entryOne._1, entryTwo._2,
      calculateDocumentSimilarity(entryOne._2, entryTwo._2, idfBroadcast.value, stopWords))
  }
}
