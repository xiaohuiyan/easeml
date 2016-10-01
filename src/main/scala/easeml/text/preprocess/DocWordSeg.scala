package easeml.text.preprocess

import org.apache.spark.rdd.RDD
import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis

/**
  * Chinese word segment using Ansj
  */
object DocWordSeg {

  /**
    * Chinese word segment
    *
    * @return A document RDD where each document is a word array
    */
  def transform(docs: RDD[String], language: String): RDD[Array[String]] = {
    docs.map { doc =>
      if (language != "cn")
        doc.toLowerCase.split("\\s+")
      else
        seg(doc)
    }
  }

  /** Segment a String into a word sequence */
  def seg(content: String): Array[String] =
    ToAnalysis.parse(content).getTerms.toArray().map {
      case t: Term => t.getName
    }
}