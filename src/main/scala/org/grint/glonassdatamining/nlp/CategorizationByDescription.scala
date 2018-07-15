package org.grint.glonassdatamining.nlp

import com.johnsnowlabs.nlp.annotator.{Normalizer, Tokenizer}
import com.johnsnowlabs.nlp.base.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StopWordsRemover, StringIndexer}
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.sql.{DataFrame, SparkSession}

class CategorizationByDescription(var trainingTable: DataFrame, sparkSession: SparkSession) {

    import sparkSession.implicits._

    private var lemmmatizerDictionaryPath: String = _

    def withLemmatizerDictionary(path: String): Unit = {
        lemmmatizerDictionaryPath = path
    }

    def tokenize: CategorizationByDescription = {
        // Johnsnowlabs NLP transformers
        val document = new DocumentAssembler()
            .setInputCol("description")
            .setOutputCol("document")
        val tokenizer = new Tokenizer()
            .setInputCols("document")
            .setOutputCol("token")
        val normalizer = new Normalizer()
            .setInputCols(Array("token"))
            .setOutputCol("normalized")
            .setLowercase(true)
        val finisher = new Finisher()
            .setInputCols("normalized")
            .setOutputCols("finished_normalized")

        val pipeline = new Pipeline()
            .setStages(Array(document, tokenizer, normalizer, finisher)).fit(trainingTable)
        val result = pipeline.transform(trainingTable)

        result.show(10, truncate = false)

        trainingTable = result
        this
    }

    def convertToWordBaseForm: CategorizationByDescription = {
        // core MLlib NLP transformers
        val stopWordsRemover = new StopWordsRemover()
            .setStopWords(StopWordsRemover.loadDefaultStopWords("russian"))
            .setInputCol("finished_normalized")
            .setOutputCol("without_stopwords")
        val stemmer = new Stemmer()
            .setInputCol("without_stopwords")
            .setOutputCol("stemmed")
            .setLanguage("Russian")
        //        val hashingTF = new HashingTF()
        //            .setNumFeatures(1000)
        //            .setInputCol(tokenizer.getOutputCol)
        //            .setOutputCol("features")

        val pipeline = new Pipeline()
            .setStages(Array(stopWordsRemover, stemmer)).fit(trainingTable)
        val stemmed = pipeline.transform(trainingTable.select("finished_normalized"))

        stemmed.show(10, false)

        trainingTable = stemmed
        this
    }

    def indexWordsByFrequency: CategorizationByDescription = {
        val indexer = new StringIndexer()
            .setInputCol("stemmed")
            .setOutputCol("categoryIndex")

        val flattedToWords = trainingTable.select("stemmed").rdd.flatMap(r => r.getSeq[String](0)).toDF("stemmed")
        val indexed = indexer.fit(flattedToWords).transform(flattedToWords)

        indexed.orderBy($"categoryIndex".asc).distinct().show(1000, false)

        trainingTable = indexed
        this
    }

    def getTable: DataFrame = trainingTable

}
