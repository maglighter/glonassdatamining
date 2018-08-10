package org.grint.glonassdatamining.datahelper

import java.io.{File, FileOutputStream, OutputStream}
import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

/**
  * Utills for loading/saving data from/to different sources
  *
  * @param sparkSession Spark session instance
  */
class DataHelper(sparkSession: SparkSession) {
    import sparkSession.implicits._

    private var _addressesWithGps: DataFrame = _

    private var _dbscanResults: DataFrame = _

    private var _cards: DataFrame = _


    private var _eventsByCategories: DataFrame = _

    private var _sufferers: DataFrame = _

    private var _forTraining: DataFrame = _

    private var _addressesWithGpsSchema = StructType(Array(
        StructField("id", IntegerType, nullable = true),
        StructField("address", StringType, nullable = true),
        // StructField("translated_address", StringType, nullable = true),
        StructField("longitude", DoubleType, nullable = true),
        StructField("latitude", DoubleType, nullable = true)))


    private var _dbscanSchema = StructType(Array(
        StructField("clusterId", IntegerType, nullable = true),
        StructField("id", StringType, nullable = true),
        StructField("longitude", DoubleType, nullable = true),
        StructField("latitude", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true),
        StructField("description", StringType, nullable = true),
        StructField("gisexgroup", StringType, nullable = true)))

    /**
      * Load tables from CSV and Postgres
      *
      * @param limit maximum rows number
      */
    def load(limit: Long = -1): Unit = {
        _addressesWithGps = sparkSession.read
            .format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .option("nullValue", "")
            .option("treatEmptyValuesAsNulls", "true")
            .schema(_addressesWithGpsSchema)
            .load(getPath("/addreses_with_gps_coordinates.csv"))

        _dbscanResults = sparkSession.read
            .format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .option("nullValue", "")
            .option("treatEmptyValuesAsNulls", "true")
            .schema(_dbscanSchema)
            .load(getPath("/output_0.003eps_3600min_250000rows.csv"))

        _cards = sparkSession.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost/glonas?user=postgres&password=postgres")
            .option("dbtable", "(SELECT createddatetime::TIMESTAMP, id, description, gisexgroup::text "
                + "FROM callcenter.cards WHERE addresstext like '%Казань,%' "
                + " %s) AS t"
                .format(if (limit == -1) "" else "limit %s".format(limit)))
            .load()

        _eventsByCategories = sparkSession.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost/glonas?user=postgres&password=postgres")
            .option("dbtable", "(select id, category from (select callcenter.cards.id, addresstext, addresshouse, case when id in (select cardid from callcenter.cardcriminals where callcenter.cards.id= callcenter.cardcriminals.cardid) then 'criminals' when id in (select cardid from callcenter.carddeads where callcenter.cards.id= callcenter.carddeads.cardid) then 'deads' when id in (select cardid from callcenter.cardsufferers where callcenter.cards.id= callcenter.cardsufferers.cardid) then 'sufferers' when id in (select cardid from callcenter.cardfires where callcenter.cards.id= callcenter.cardfires.cardid) then 'fires' when id in (select cardid from callcenter.cardaccidents where callcenter.cards.id= callcenter.cardaccidents.cardid) then 'accidents' when id in (select cardid from callcenter.cardterrors where callcenter.cards.id= callcenter.cardterrors.cardid) then 'terrors' else '' end as category from callcenter.cards where addresstext like '%Казань,%' and addresshouse is not null order by id) as t1 where category != '') AS t")
            .load()

        _sufferers = sparkSession.read
            .format(source = "jdbc")
            .option("url", "jdbc:postgresql://localhost/glonas?user=postgres&password=postgres")
            .option("dbtable", "(SELECT cards.id, cards.description"
                + " FROM callcenter.cards INNER JOIN callcenter.cardsufferers ON cards.id = cardsufferers.cardid) AS t")
            .load()

        _forTraining = sparkSession.read
            .format(source = "jdbc")
            .option("url", "jdbc:postgresql://localhost/glonas?user=postgres&password=postgres")
            .option("dbtable",
                "(select id, description, category from (select callcenter.cards.id, description, addresshouse, "
                    + "case when id in (select cardid from callcenter.cardcriminals where callcenter.cards.id= callcenter.cardcriminals.cardid) then 'criminals' "
                    + "when id in (select cardid from callcenter.carddeads where callcenter.cards.id= callcenter.carddeads.cardid) then 'deads' "
                    + "when id in (select cardid from callcenter.cardsufferers where callcenter.cards.id= callcenter.cardsufferers.cardid) then 'sufferers' "
                    + "when id in (select cardid from callcenter.cardfires where callcenter.cards.id= callcenter.cardfires.cardid) then 'fires' "
                    + "when id in (select cardid from callcenter.cardaccidents where callcenter.cards.id= callcenter.cardaccidents.cardid) then 'accidents' "
                    + "when id in (select cardid from callcenter.cardterrors where callcenter.cards.id= callcenter.cardterrors.cardid) then 'terrors' "
                    + "else '' end as category "
                    + "from callcenter.cards "
                    + "where addresstext like '%Казань,%' "
                    + "order by id) as k where category != '') as t")
            .load()

    }

    def write(model:RDD[OutputCsv], outputPath:java.net.URI): Unit = {
        model.toDF.coalesce(1).write
            .format("com.databricks.spark.csv")
            .mode("overwrite")
            .option("header", "true")
            .option("delimiter", ",")
            .option("nullValue", "")
            .save(outputPath.toString)
    }

    def write(model:DataFrame, outputPath:java.net.URI): Unit = {
        model.coalesce(1).write
            .format("com.databricks.spark.csv")
            .mode("overwrite")
            .option("header", "true")
            .option("delimiter", ",")
            .option("nullValue", "")
            .save(outputPath.toString)
    }

    def cards: DataFrame = _cards

    def addressesWithGps: DataFrame = _addressesWithGps

    def dbscanResults: DataFrame = _dbscanResults

    def eventsByCategories: DataFrame = _eventsByCategories

    def sufferers: DataFrame = _sufferers

    def forTraining: DataFrame = _forTraining

    private def getPath(resourceName: String): String =  {
        var path = ""
        try {
            val input = getClass.getResourceAsStream(resourceName)

            val file = File.createTempFile(resourceName, ".tmp")

            val out: OutputStream = new FileOutputStream(file)
            var read: Int = 0
            val bytes: Array[Byte] = new Array[Byte](1024)
            while ({ read = input.read(bytes); read } != -1) {
                out.write(bytes, 0, read)
            }

            file.deleteOnExit()
            path = file.getPath
        } catch {
            case e: Exception => e.printStackTrace()
        }
        path
    }

}
