package easeml.util

import java.io._

import scala.io.Source
import easeml.Logging


/**
  * Local file read/write functions
  *
  * @author YanXiaohui on 2016-09-30.
  */
object IO extends Logging {
  /**
    * Read a file line by line
    *
    * @param pt       file path
    * @param dropHead if true, ignore the first line. Default is false.
    * @return Iterator[line: String]
    */
  def readLines(pt: String, dropHead: Boolean = false): Iterator[String] = {
    logInfo(s"read: $pt")
    val rf = Source.fromFile(pt)
    if (dropHead)
      rf.getLines().drop(1)
    else
      rf.getLines()
  }

  /**
    * Read file in resources directory
    *
    * @param pt resource file path, like "/resource/file"
    */
  def readResource(pt: String): Iterator[String] = {
    logInfo(s"read: $pt")
    val rf = getClass.getResourceAsStream(pt)
    try {
      Source.fromInputStream(rf).getLines()
    } catch {
      case e: NullPointerException =>
        logError(s"Cannot read file: $pt")
    }
  }

  /**
    * Write into a file line by line
    *
    * @param res_pt result file path
    * @param lines  lines to be written
    */
  def writeLines(res_pt: String, lines: TraversableOnce[String]) {
    logInfo(s"write: $res_pt")
    val wf = new PrintWriter(res_pt)
    lines.foreach { l => wf.write(l + '\n') }
    wf.close()
  }


  /**
    * Read file with columns separated by a fixed delimiter
    *
    * @param dml delimiter, default is " "
    * @return Iterator[Array(col, col, ...)]
    */
  def readDML(pt: String, dml: String = " "): Iterator[Array[String]] =
    readLines(pt).map { ln =>
      ln.trim.split(dml)
    }

  /** read file with columns separated by "," */
  def readCSV(pt: String): Iterator[Array[String]] = readDML(pt, ",")

  /** read file with columns separated by "\t" */
  def readTSV(pt: String): Iterator[Array[String]] = readDML(pt, "\t")


  /**
    * Read a two-element tuple list.
    *
    * Input format: key    value
    *
    * @return Iterator[(string, string)]
    */
  def readTuple2[K, V](pt: String, delim: Char = '\t'): Iterator[(String, String)] =
    readLines(pt).map { l =>
      val Array(k: String, v: String) = l.trim.split(delim)
      (k, v)
    }

  /**
    * Read a 3-element tuple list.
    *
    * Input format: s1    s2     s3
    *
    * @return Iterator[(string, string, string)]
    */
  def readTuple3(pt: String, delim: Char = '\t'): Iterator[(String, String, String)] =
    readLines(pt).map { l =>
      val Array(s1, s2, s3) = l.trim.split(delim)
      (s1, s2, s3)
    }

  /**
    * write a tuple (k, v) into a file
    * Output format: key    value
    */
  def writeTuple2[K, V](res_pt: String, ts: Iterator[(K, V)], delim: Char = '\t') =
    writeLines(res_pt, ts.map {
      case (k, v) => s"$k$delim$v"
    })

  /**
    * write a tuple (s1, s2, s3) into a file
    * Output format: s1    s2    s3
    */
  def writeTuple3[K1, K2, K3](res_pt: String, ts: Iterator[(K1, K2, K3)], delim: Char = '\t') =
    writeLines(res_pt, ts.map {
      case (s1, s2, s3) => s"$s1$delim$s2$delim$s3"
    })

  /**
    * Read a Map
    * Input format: key:value
    */
  def readMap(pt: String): Map[String, String] = readTuple2(pt).toMap

  /**
    * write a Map[String, String] to file
    *
    * @return Output format: key    value
    */
  def writeMap[K, V](res_pt: String, m: Map[K, V]) = writeTuple2(res_pt, m.toIterator)


  /** Serialize a object to file */
  def writeObject[T <: Serializable](res_pt: String, obj: T): Unit = {
    log.info(s"write object: $res_pt")
    val wf = new ObjectOutputStream(new FileOutputStream(res_pt))
    wf.writeObject(obj)
    wf.close()
  }

  /** Deserialize a object from a local file */
  def readObject[T <: Serializable](pt: String): T = {
    log.info(s"read object: $pt")
    val rf = new ObjectInputStream(new FileInputStream(pt))
    val v = rf.readObject()
    rf.close()
    v.asInstanceOf[T]
  }
}
