package easeml.text.preprocess

import easeml.text.StopWords
import easeml.util.IO.{readMap, writeMap}
import org.apache.spark.rdd.RDD

/**
  * Processing functions for text
  *
  * @author YanXiaohui on 2016-09-30.
  */
object DwidProc {

  /**
    * Transform documents into word id sequences
    * @param raw_docs raw document collection
    * @param language "cn": Chinese, "en:" English (default)
    * @param mid_dir  middle file paths, default is "/tmp"
    * @return document list transformed from raw_docs one by one
    */
  def fitAndTransform(raw_docs: RDD[String],
                      language: String = "en",
                 mid_dir: String = "/tmp",
                 stop_word_pt: String = ""): RDD[Array[Int]] = {
    val stop_words: Set[String] = if (!stop_word_pt.isEmpty)
      StopWords.getFromFile(stop_word_pt)
    else
      StopWords.getEnglish

    // main process
    val seg_docs = DocWordSeg.transform(raw_docs, language)
    val filter_docs = WordFilter.transform(seg_docs, stop_words)
    val (dwids, w2ids) = WordIndex.fitAndTransform(filter_docs)

    // write middle files
    val dir = if (mid_dir.last=='/') mid_dir else mid_dir + "/"
    writeMap(dir + "w2id.txt", w2ids)

    dwids
  }

  /**
    * Transform documents with existing vocabulary
    * @param raw_docs  documents to be transformed
    * @param mid_dir  The directory contains vocabulary file, i.e., w2id.txt
    */
  def transform(raw_docs: RDD[String],
                language: String = "en",
                mid_dir: String = "/tmp"): RDD[Array[Int]] = {
    val dir = if (mid_dir.last=='/') mid_dir else mid_dir + "/"
    val seg_docs = DocWordSeg.transform(raw_docs, language)

    val w2ids: Map[String, Int] = readMap(dir + "w2id.txt").mapValues(_.toInt).map(identity)
    WordIndex.transform(seg_docs, w2ids)
  }
}
