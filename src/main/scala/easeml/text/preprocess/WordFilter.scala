package easeml.text.preprocess

import org.apache.spark.rdd.RDD

object WordFilter {

  /**
    * Filter words via black-list, frequency, and length
    * @param docs  Input documents
    * @param stop_words   black-list of words to be filtered
    * @param min_freq  threshold to filter low-frequent words
    * @param topN    filter topN most frequent words besides
    * @param min_len  filter short words
    * @param max_len filter long words
    * @return
    */
  def transform(docs: RDD[Array[String]],
            stop_words: Set[String] = Set.empty[String],
            min_freq: Int = 5,
            max_freq: Int = Int.MaxValue,
            topN: Int = 100,
            min_len: Int = 2,
            max_len: Int = 10
           ): RDD[Array[String]] = {
    // collect the words to be filtered
    val wfs: Map[String, Int] = DocsTermCount(docs)

    val ws1 = wfs.toSeq.sortBy(_._2).takeRight(topN).map(_._1)
    val ws2 = wfs.toSeq.filter {
      case (w, f) =>
        f < min_freq || f > max_freq || w.length < min_len || w.length > max_len
    }.map(_._1)

    val filter_ws: Set[String] = stop_words ++ ws1 ++ ws2
    docs.map { doc =>
      doc.filter(!filter_ws.contains(_))
    }
  }
}
