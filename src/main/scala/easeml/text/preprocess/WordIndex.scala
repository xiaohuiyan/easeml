package easeml.text.preprocess

import org.apache.spark.rdd.RDD

object WordIndex {

  /**
    * Transform words in each document into indexes
    *
    * @param docs Each document is a sequence of words
    * @param w2id word-index Map
    * @return A document collection represented by word indexes
    */
  def transform(docs: RDD[Array[String]],
                w2id: Map[String, Int]): RDD[Array[Int]] = {
    docs.map { doc =>
      // filter out unindexed words
      doc.filter(w2id.contains(_)).map { w =>
        w2id(w)
      }
    }
  }

  /**
    * Index words in the document collection
    *
    * @return A word-index map
    */
  def fit(docs: RDD[Array[String]]): Map[String, Int] =
    docs.flatMap(_.distinct).distinct().collect().zipWithIndex.toMap

  /**
    * First build the index of words and then transform words to indexes
    *
    * @param docs  documents represented by word arrays
    * @return (doc_wids, w2ids)
    */
  def fitAndTransform(docs: RDD[Array[String]]): (RDD[Array[Int]], Map[String, Int]) = {
    val w2ids = fit(docs)
    val dwids = transform(docs, w2ids)
    (dwids, w2ids)
  }
}