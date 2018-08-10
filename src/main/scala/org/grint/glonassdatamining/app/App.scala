package org.grint.glonassdatamining.app

import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.grint.glonassdatamining.clusterization.SpatioTemporalPointDbscan
import org.grint.glonassdatamining.datahelper.{Clustered, DataHelper, OutputCsv}
import org.grint.glonassdatamining.nlp.NLPTools

/**
  * Main file of the project
  *
  * Run with:
  * spark-submit --class org.grint.glonassdatamining.app.App --master local[4] \
  * target/glonass-data-mining-1.0-SNAPSHOT.jar \
  * --output output.csv --epsilonSpatial 0.003 --epsilonTemporal=3600 --minPts 2
  */
object App {

    private var dataHelper: DataHelper = _

    private val sparkSession: SparkSession = SparkSession
        .builder()
        .appName("GLONASS+112 data mining")
        .master("local[4]")
        .config("spark.extraListeners","com.groupon.sparklint.SparklintListener")
        .getOrCreate()

    import sparkSession.implicits._

    private var configuration: Config = _

    def main(args: Array[String]): Unit = {
        parseParams(args)

        println(("Params: spatial epsilon %s, temporal epsilon %s minutes, "
            + "minimum points %s (\"-1\" - all), output file path %s")
            .format(configuration.epsilonSpatial, configuration.epsilonTemporal / 1000 / 60,
                configuration.minPts, configuration.output))

        // loading data from different sources (csv, postgres, etc...)
        dataHelper = new DataHelper(sparkSession)
        dataHelper.load(configuration.limit)

        //cluster(true)

        val input = dataHelper.dbscanResults
            .select("id", "clusterId", "timestamp")
            .join(dataHelper.cards.select("id", "description"), "id")
            .join(dataHelper.forTraining.select("id", "category"), "id")
            .limit(500)
//            .withColumn("description", regexp_replace(col("description"), "[\\r\\n]", " "))
            .toDF()
//        dataHelper.write(input, java.net.URI.create("./output"))

        input.show(500, false)
//        input.coalesce(1).write
//            .format("com.databricks.spark.csv")
//            .mode("overwrite")
//            .option("header", "true")
//            .option("delimiter", ",")
//            .option("nullValue", "")
//            .save("input")
        val nlp = findTopKeywords(input)

        findAssociationRulesByDescription(Option(nlp))
    }

    def cluster(saveNeeded: Boolean = true): RDD[OutputCsv] = {
        val dbTable = dataHelper.addressesWithGps.join(dataHelper.cards, "id")

        // clustering input rdd
        val dbscan = new SpatioTemporalPointDbscan(configuration, sparkSession)
        dbscan.prepareInput(dbTable)
        val clusters = dbscan.run()

        val model = clusters.map { case (stObject, (clusterId, payload)) => OutputCsv(
            clusterId, payload.id, payload.latitude,
            payload.longitude, stObject.time.get.start.value,
            payload.description, payload.gisexgroup)
        }

        // saving results of clusterization to a CSV file
        if (saveNeeded) {
            dataHelper.write(model, configuration.output)
        }

        model
    }

    def findTopKeywords(descriptionsTable: DataFrame): DataFrame = {
        println("dataset size " + descriptionsTable.count())

        val nlp = new NLPTools(descriptionsTable, sparkSession)
            .tokenize
            .convertTokensToBaseForm()
            .getTopKeywordsSortedByDfIdf(configuration.topKeywordsNumber)
            .getTable

        nlp
    }

    def findAssociationRulesByCategory(computedClustersTable: Option[DataFrame] = None): RDD[AssociationRules.Rule[String]] = {
                val model: RDD[OutputCsv] = {
                    if (computedClustersTable.isDefined) {
                        computedClustersTable.get
                            .select("clusterId", "id", "longitude",
                                "latitude", "timestamp", "description", "gisexgroup")
                            .map(row => {
                                OutputCsv(row(0).asInstanceOf[Int],
                                    row(1).toString,
                                    row.getDouble(2),
                                    row.getDouble(3),
                                    row.getLong(4),
                                    row.getString(5),
                                    row.getString(6))
                            }).rdd
                    } else {
                        cluster()
                    }
                }

                val eventsByCategories = dataHelper.eventsByCategories //.rdd.map(r => (r(0).toString, r(1).toString))

                println(eventsByCategories.distinct().count())

                val transactions = model
                    .filter(f => f.clusterId != 0)
                    .toDF("clusterId", "id", "longitude", "latitude", "timestamp", "description", "gisexgroup")
                    .join(eventsByCategories, "id")
                    .select("clusterId", "category")
                    .rdd.map(row => (row(0).asInstanceOf[Int], row(1).toString))
                    .groupByKey()
                    .map(i => i._2.toArray.distinct)
                    .filter(i => i.length > 1)
        transactions.foreach(i => println(i.mkString(" ")))
        println(transactions.count())

        // var o=eventsByCategories.map(a => (model.filter(m => m.id == a._1), a._2)).count()//.map(k => (k._1.first().clusterId, k._2)).groupByKey().map(i => i._2).count

        val fpg = new FPGrowth()
            .setMinSupport(configuration.minSupport)
        val mod = fpg.run(transactions)
        mod.freqItemsets.collect().foreach { itemset =>
            println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
        }

        val associationRules = mod.generateAssociationRules(configuration.minConfidence)
        associationRules.collect().foreach { rule =>
            println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
                s"${rule.consequent.mkString("[", ",", "]")},${rule.confidence}")
        }

        println("Number of clusters: " + model.groupBy(_.clusterId).count())

        associationRules
    }

    def findAssociationRulesByDescription(computedClustersTable: Option[DataFrame] = None): RDD[AssociationRules.Rule[String]] = {

        val transactions = computedClustersTable.get.select("clusterId", "keyword")
                    .rdd.map(row => (row.getString(0).toInt, row(1).toString))
                    .groupByKey()
                    .map(i => i._2.toArray.distinct)
                    .filter(i => i.length >= 1)
        transactions.foreach(i => println(i.mkString(" ")))
        println(transactions.count())

        // var o=eventsByCategories.map(a => (model.filter(m => m.id == a._1), a._2)).count()//.map(k => (k._1.first().clusterId, k._2)).groupByKey().map(i => i._2).count

        val fpg = new FPGrowth()
            .setMinSupport(configuration.minSupport)
        val mod = fpg.run(transactions)
        mod.freqItemsets.collect().foreach { itemset =>
            println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
        }

        val associationRules = mod.generateAssociationRules(configuration.minConfidence)
        associationRules.collect().foreach { rule =>
            println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
                s"${rule.consequent.mkString("[", ",", "]")},${rule.confidence}")
        }

        associationRules
    }

    def parseParams(args: Array[String]): Unit = {
        // parsing program arguments
        configuration = Config()
        val parser = new scopt.OptionParser[Config]("glonass112-config") {
            head("glonass112-config", "0.1")
            opt[Double]("epsilonSpatial") action { (x, c) => c.copy(epsilonSpatial = x) } text "spatial epsilon parameter for DBSCAN (0.003 by default)"
            opt[Long]("epsilonTemporal") action { (x, c) => c.copy(epsilonTemporal = x) } text "temporal epsilon parameter for DBSCAN in seconds (60 min by default)"
            opt[Int]("minPts") action { (x, c) => c.copy(minPts = x) } text "minimum number of points in cluster parameter for DBSCAN (2 by default)"
            opt[Int]("limit") action { (x, c) => c.copy(limit = x) } text "maximum number of rows in input table"
            opt[Int]("top") action { (x, c) => c.copy(topKeywordsNumber = x) } text "number of top words that will be selected from each category based on IF-IDF"
            opt[Double]("confidence") action { (x, c) => c.copy(minConfidence = x) } text "minimum confidence for association rules finding"
            opt[Double]("support") action { (x, c) => c.copy(minSupport = x) } text "minimum support for association rules finding"
            opt[java.net.URI]('o', "output") action { (x, c) => c.copy(output = x) } text "output is the result file"
            help("help") text "prints this usage text"
        }
        parser.parse(args, Config()) match {
            case Some(c) =>
                configuration = Config(c.epsilonSpatial,
                    c.epsilonTemporal * 1000,
                    c.minPts,
                    c.limit,
                    c.topKeywordsNumber,
                    c.minConfidence,
                    c.minSupport,
                    if (c.output.toString == ".")
                        java.net.URI.create("./output_%seps_%smin.csv"
                            .format(c.epsilonSpatial, c.epsilonTemporal / 60))
                    else
                        configuration.output
                )
            case None =>
            // arguments are bad, error message will have been displayed
        }
    }

}