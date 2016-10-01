package easeml.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import easeml.text.preprocess.{DwidProc, TFIDFProc}

/**
  * Example on 20news group data
  *
  * @author YanXiaohui on 2016-09-30.
  */
private[easeml] object Newsgroup {

  def main(args: Array[String]) {
    val train_pt = "data/20news/train.txt"
    val test_pt = "data/20news/test.txt"

    val conf = new SparkConf().setAppName("20news preprocess").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val train_docs = readDocs(sc, train_pt)
    val test_docs = readDocs(sc, test_pt)

    val mid_dir = "/tmp/mid"
    val train_dwids = TFIDFProc.fitAndTransform(train_docs, mid_dir = mid_dir)
    val test_dwids = TFIDFProc.transform(test_docs, mid_dir = mid_dir)

    val res_train_pt = "/tmp/train2"
    val res_test_pt = "/tmp/test2"
    train_dwids.map(_.map(_.toString).mkString(" ")).saveAsTextFile(res_train_pt)
    test_dwids.map(_.map(_.toString).mkString(" ")).saveAsTextFile(res_test_pt)
  }

  def readDocs(sc: SparkContext, pt: String): RDD[String] = {
    sc.textFile(pt).map{ doc =>
      val Array(_, _, content) = doc.split("\t")
      content
    }
  }
}
