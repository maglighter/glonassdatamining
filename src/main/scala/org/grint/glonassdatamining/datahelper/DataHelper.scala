package org.grint.glonassdatamining.datahelper

import java.io.{File, FileOutputStream, OutputStream}

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

    private var _cards: DataFrame = _

    private var _addressesWithGpsSchema = StructType(Array(
        StructField("id", IntegerType, true),
        StructField("address", StringType, true),
        StructField("translated_address", StringType, true),
        StructField("longitude", DoubleType, true),
        StructField("latitude", DoubleType, true)))

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

        _cards = sparkSession.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost/glonas?user=postgres&password=postgres")
            .option("dbtable", "(SELECT createddatetime::TIMESTAMP, id, description, gisexgroup::text "
                + "FROM callcenter.cards WHERE addresstext like '%Казань,%' and "
                + "applicantlocation is not null %s) AS t"
                .format(if (limit == -1) "" else "limit %s".format(limit)))
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

    def cards: DataFrame = _cards

    def addressesWithGps: DataFrame = _addressesWithGps

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