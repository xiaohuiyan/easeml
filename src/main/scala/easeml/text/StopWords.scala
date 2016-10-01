package easeml.text

import easeml.util.IO.{readLines, readResource}

object StopWords {

  def getEnglish: Set[String] = readResource("/stopWordsEN.txt").toSet

  def getChinese: Set[String] = readResource("/stopWordsCN.txt").toSet

  def getAll: Set[String] = getEnglish.union(getChinese)

  def getFromFile(pt: String) = readLines(pt).toSet
}
