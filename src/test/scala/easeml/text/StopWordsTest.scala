package easeml.text

import org.scalatest.FunSuite

class StopWordsTest extends FunSuite {

  test("get English stop words") {
    assert(StopWords.getEnglish.contains("am"))
  }

  test("get Chinese stop words") {
    assert(StopWords.getChinese.contains("如此"))
  }

  test("get All") {
    val sws = StopWords.getAll
    assert(sws.contains("am") && sws.contains("这样"))
  }
}
