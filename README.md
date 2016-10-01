EaseML is a spark lib designed to easy machine learning task development. It tries to simplify many tedious processes with single functions.

Please see the examples in the source.
 
Text preprocessing, including word segment, word filtering, word indexing, and tfidf transformation.

Transform documents into word id sequences:
```
// auto-generated vocabulary  
DwidProc.fitAndTransform(raw_docs: RDD[String], language: String = "en", mid_dir: String = "/tmp", stop_word_pt: String = ""): RDD[Array[Int]]
// using existing vocabulary in `mid_dir`
DwidProc.transform(raw_docs: RDD[String], language: String = "en", mid_dir: String = "/tmp"): RDD[Array[Int]]
```
Transform documents into tfidf representation:
```
// auto-generated vocabulary  
TFIDFProc.fitAndTransform(raw_docs: RDD[String], language: String = "en", mid_dir: String = "/tmp", stop_word_pt: String = "")
// using existing vocabulary in `mid_dir`
TFIDFProc.transform(raw_docs: RDD[String], language: String = "en", mid_dir: String = "/tmp", stop_word_pt: String = "")
```

