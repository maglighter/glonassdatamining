package org.grint.glonassdatamining.nlp

import com.johnsnowlabs.nlp.annotator.{Normalizer, Tokenizer}
import com.johnsnowlabs.nlp.base.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.sql.functions.{split, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
Tools to transform unstructured text using NLP methods
 */
class NLPTools(var trainingTable: DataFrame, sparkSession: SparkSession) {

    import sparkSession.implicits._

    private var lemmmatizerDictionaryPath: String = _

    case class ColumnsConfig(
                              documentColumn: String = "stemmed",
                              categoryColumn: String = "category", //doc-id
                              keywordColumn: String = "keyword",
                              tfColumn: String = "tf",
                              dfColumn: String = "df",
                              idfColumn: String = "idf",
                              tfIdfColumn: String = "tf-idf"
                          )

    private var config: ColumnsConfig = ColumnsConfig()

    def withLemmatizerDictionary(path: String): Unit = {
        lemmmatizerDictionaryPath = path
    }

    def tokenize: NLPTools = {
        trainingTable = trainingTable.filter($"description".isNotNull)
        // Johnsnowlabs NLP transformers
        val document = new DocumentAssembler()
            .setInputCol("description")
            .setOutputCol("document")
        val tokenizer = new Tokenizer()
            .setInputCols(document.getOutputCol)
            .setOutputCol("token")
        val normalizer = new Normalizer()
            .setInputCols(Array(tokenizer.getOutputCol))
            .setOutputCol("normalized")
            .setLowercase(true)
        val finisher = new Finisher()
            .setInputCols(normalizer.getOutputCol)
            .setOutputCols("finished_normalized")

        val pipeline = new Pipeline()
            .setStages(Array(document, tokenizer, normalizer, finisher))
            .fit(trainingTable)
        val result = pipeline.transform(trainingTable)

        result.show(10, truncate = false)

        trainingTable = result
        this
    }

    def convertTokensToBaseForm(convertCategory: Boolean = true): NLPTools = {
        // core MLlib NLP transformers
        val stopWordsRemover = new StopWordsRemover()
            .setStopWords(StopWordsRemover.loadDefaultStopWords("russian"))
            .setInputCol("finished_normalized")
            .setOutputCol("without_stopwords")
        val stemmer = new Stemmer()
            .setInputCol(stopWordsRemover.getOutputCol)
            .setOutputCol(config.documentColumn)
            .setLanguage("Russian")
        val countVectorizer = new CountVectorizer()
            .setInputCol(stemmer.getOutputCol)
            .setOutputCol("features")
            .setVocabSize(20000)
            .setMinDF(5)
        val labelStringIdx = new StringIndexer()
            .setInputCol(config.categoryColumn)
            .setOutputCol("label")
        var stages = Array(stopWordsRemover, stemmer, countVectorizer)
        var select = Array("clusterId", "description", "finished_normalized")
        if (convertCategory) {
            stages = stages :+ labelStringIdx
            select = select :+ config.categoryColumn
        }
        val pipeline = new Pipeline()
            .setStages(stages)
            .fit(trainingTable)
        val stemmed = pipeline.transform(trainingTable.select("id", select: _*))

        stemmed.show(10, truncate = false)

        trainingTable = stemmed
        this
    }

    def indexWordsByFrequency: NLPTools = {
        val indexer = new StringIndexer()
            .setInputCol(config.documentColumn)
            .setOutputCol("categoryIndex")

        val flattedToWords = trainingTable.select(config.documentColumn).
            rdd.flatMap(r => r.getSeq[String](0))
            .toDF(config.documentColumn)
        val indexed = indexer.fit(flattedToWords).transform(flattedToWords)

        indexed.orderBy($"categoryIndex".asc).distinct().show(1000, truncate = false)

        trainingTable = indexed
        this
    }

    def tfIdfByDocuments: NLPTools = {
        val hashingTF = new HashingTF()
            .setNumFeatures(20000)
            .setInputCol(config.documentColumn)
            .setOutputCol("features")

        val idfStage = new IDF()
            .setInputCol("features")
            .setOutputCol("tf-idf")

        val flattedToWords = trainingTable.select("id", config.documentColumn)

        flattedToWords.show()
        val tf = hashingTF.transform(flattedToWords)
        tf.cache()
        val idf = idfStage.fit(tf)
        val tfIdf = idf.transform(tf)

        tfIdf.orderBy($"tf-idf".desc).distinct().show(20, truncate = false)

        trainingTable = tfIdf
        this
    }

    def getTopKeywordsSortedByDfIdf(count: Int): NLPTools = {
        val customAggregation = new CustomAggregation()
        val documentsByCategory = trainingTable.withColumn("id", split(col("id"), ",")
            .cast("array<string>")
            .alias("id"))
            .withColumn("clusterId", split(col("clusterId"), ",")
                .cast("array<string>")
                .alias("clusterId"))
        var documentsByCategoryUnfolded = unfoldDocs(documentsByCategory, config.documentColumn)
        documentsByCategoryUnfolded = documentsByCategoryUnfolded.withColumn(config.keywordColumn, split(col(config.keywordColumn), ",")
            .cast("array<string>")
            .alias(config.keywordColumn))
        val documentsCount = 6
        //documentsByCategory.count()
        val docsWithId = documentsByCategoryUnfolded.groupBy(config.categoryColumn)
            .agg(customAggregation(col(config.keywordColumn)) as config.keywordColumn,
                customAggregation(col("id")) as "id",
                customAggregation(col("clusterId")) as "clusterId")

        val unfoldedDocs = unfoldDocs2(docsWithId)
        val tokensWithTf = addTf(unfoldedDocs)
        val tokensWithDf = addDf(unfoldedDocs)
        val tokensWithDfIdf = addIdf(tokensWithDf, documentsCount)
        val tfIdf = joinTfIdf(tokensWithTf, tokensWithDfIdf)

        val topKeywords = tfIdf
            .select(config.keywordColumn, config.tfIdfColumn, config.categoryColumn)
            .orderBy($"tf-idf".desc)
            .rdd.map(r => (r.getString(0), r.getDouble(1), r.getString(2)))
            .groupBy(x => x._3)
            .mapValues(r => r.toList.sortWith(_._2 > _._2).take(count).map(_._1))
            .map(x => (x._2, x._1))
            .toDF("temp", config.categoryColumn)
        topKeywords.show(100, false)
        val frame = unfoldDocs(topKeywords, "temp").select(config.keywordColumn)
        //unfoldedDocs.filter("clusterId != 0")
        trainingTable = frame.join(unfoldedDocs, Seq(config.keywordColumn))
        trainingTable.show(600)
        this
    }

    protected def unfoldDocs(documents: DataFrame, columnName: String = config.keywordColumn): DataFrame = {
        val columns = documents.columns.map(col) :+
            (explode(col(columnName)) as config.keywordColumn)
        documents.select(columns: _*)
    }

    protected def unfoldDocs2(documents: DataFrame): DataFrame = {
        val zip = udf((as: Seq[String], bs: Seq[String], cs: Seq[String]) => {
            var d = as.zip(bs).zip(cs)
            d map {
                case ((a, b), c) => (a, b, c)
            }

        })
        val columns = documents.columns.map(col) :+
            (explode(zip(col("id"), col("clusterId"), col(config.keywordColumn))) as "vars")

        documents.select(columns: _*).select(
            $"vars._1".alias("id"),
            $"vars._2".alias("clusterId"),
            $"vars._3".alias(config.keywordColumn),
            $"category")
    }

    protected def addTf(unfoldedDocs: DataFrame): DataFrame =
        unfoldedDocs.groupBy(config.categoryColumn, config.keywordColumn)
            .agg(count(config.keywordColumn) as config.tfColumn)

    protected def addDf(unfoldedDocs: DataFrame): DataFrame =
        unfoldedDocs.groupBy(config.keywordColumn)
            .agg(countDistinct(config.categoryColumn) as config.dfColumn)

    protected def addIdf(tokensWithDf: DataFrame, docCount: Long): DataFrame = {
        val calcIdfUdf = udf { df: Long => NLPTools.calcIdf(docCount, df) }
        tokensWithDf.withColumn(config.idfColumn, calcIdfUdf(col(config.dfColumn)))
    }

    protected def joinTfIdf(tokensWithTf: DataFrame, tokensWithDfIdf: DataFrame): DataFrame =
        tokensWithTf
            .join(tokensWithDfIdf, Seq(config.keywordColumn), "left")
            .withColumn(config.tfIdfColumn, col(config.tfColumn) * col(config.idfColumn))

    def getTable: DataFrame = trainingTable

}

object NLPTools {

    def calcIdf(docCount: Long, df: Long): Double =
        math.log((docCount.toDouble + 1) / (df.toDouble + 1))

}

//def getTopKeywordsSortedByDfIdf(count: Int): Set[String] = {
//    val customAggregation = new CustomAggregation()
//
//    //        val documentsByCategory = trainingTable
//    //            .select($"*", explode($"stemmed") as "stemmed_list")
//    //            .select($"*", explode($"id") as "id_list")
//    //            .groupBy("category")
//    //            .agg(
//    //                collect_list("stemmed_list") as "stemmed",
//    //                collect_list("id_list") as "id")
//
//    val documentsByCategory = trainingTable.groupBy("category")
//    .agg(customAggregation(trainingTable("stemmed")) as "stemmed")//.show(150, false)
//    val documentsCount = 6//documentsByCategory.count()
//    val docsWithId = documentsByCategory//addDocId(trainingTable)
//    val unfoldedDocs = unfoldDocs(docsWithId)
//    val tokensWithTf = addTf(unfoldedDocs)
//    val tokensWithDf = addDf(unfoldedDocs)
//    val tokensWithDfIdf = addIdf(tokensWithDf, documentsCount)
//    val tfIdf = joinTfIdf(tokensWithTf, tokensWithDfIdf)
//
//    //        trainingTable = tfIdf.join(docsWithId, Seq(config.docIdColumn), "left")
//
//    tfIdf.orderBy($"tf-idf".desc)//.select(config.tokenColumn, "tf-idf").groupBy(config.tokenColumn).agg(sum("tf-idf"))
//    .show(500, truncate = false)
//
//    val result = tfIdf.select(config.tokenColumn, config.tfIdfColumn, config.docIdColumn).orderBy($"tf-idf".desc).rdd.map(r  => (r.getString(0), r.getDouble(1), r.getString(2)))
//    .groupBy(x => x._3).mapValues(r => r.toList.sortWith(_._2 > _._2).take(5)).flatMap(x => x._2).map(_._1).collect().toSet
//    //.foreach(println)
//
//    result
//}