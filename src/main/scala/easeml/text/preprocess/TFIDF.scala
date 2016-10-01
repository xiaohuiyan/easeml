package easeml.text.preprocess

import org.apache.spark.rdd.RDD
import easeml.util.Counter

/** TFIDF statistic and transformation */
object TFIDF {

  /**
    * Transform docs from word sequence into tfidf representation
    *
    * @param docs Each docs is a word Array
    * @param idf  The idf values of each word
    * @return A document sequence, each document is represent by a sequence
    *         of word-tfidf value tuple
    */
  def transform(docs: RDD[Array[Int]],
                idf: Map[Int, Double]): RDD[Map[Int, Double]] = {

    val bc_idf = docs.context.broadcast(idf)
    docs.map { doc =>
      Counter(doc).toSeq.filter {
        // filter words without idf values
        case (w, tf) => bc_idf.value.contains(w)
      }.map {
        case (w, tf) => (w, tf * bc_idf.value(w))
      }.toMap
    }
  }

  /**
    * Stat idf of words in the documents, and then transform them into tfidf.
    *
    * @return A tfidf representation of the documents, and a idf-value Map
    */
  def fitAndTransform(docs: RDD[Array[Int]]):
  (RDD[Map[Int, Double]], Map[Int, Double]) = {
    val idf = fit(docs)
    val new_docs = transform(docs, idf)
    (new_docs, idf)
  }


  /**
    * Compute idf of words in a document collection
    */
  def fit(docs: RDD[Array[Int]]): Map[Int, Double] = {
    val D = docs.count()
    val w_dfs = docs.flatMap { doc =>
      doc.distinct.map { w =>
        (w, 1)
      }
    }.reduceByKey(_ + _).collect
    w_dfs.map {
      case (w, df) =>
        val idf = math.log((D + 1.0) / (df + 1.0))
        (w, idf)
    }.toMap
  }
}