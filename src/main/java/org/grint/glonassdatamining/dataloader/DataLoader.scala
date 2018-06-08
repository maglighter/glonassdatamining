package org.grint.glonassdatamining.dataloader

import java.io.{File, FileOutputStream, OutputStream}

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types._

object DataLoader {

    private var _addressesWithGps: DataFrame = _

    private var _cards: DataFrame = _

    private var _testData: DataFrame = _

    private var _addressesWithGpsSchema = StructType(Array(
        StructField("id", IntegerType, true),
        StructField("address", StringType, true),
        StructField("translated_address", StringType, true),
        StructField("latitude", DoubleType, true),
        StructField("longitude", DoubleType, true)))

    private var _testDataSchema = StructType(Array(
        StructField("createddatetime", LongType, true),
        StructField("id", IntegerType, true),
        StructField("latitude", DoubleType, true),
        StructField("longitude", DoubleType, true)))

    def load(sparkSession: SparkSession): Unit = {
//        uncomment for acces to database (postgres service should be running)
//        _addressesWithGps = sparkSession.read
//            .format("csv")
//            .option("header", "true")
//            .option("delimiter", ",")
//            .option("nullValue", "")
//            .option("treatEmptyValuesAsNulls", "true")
//            .schema(_addressesWithGpsSchema)
//            .load(getPath("/addreses_with_gps_coordinates.csv"))

        _testData = sparkSession.read
            .format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .option("nullValue", "")
            .option("treatEmptyValuesAsNulls", "true")
            .schema(_testDataSchema)
            .load(getPath("/test_data.csv"))

        _cards = sparkSession.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost/glonas?user=postgres&password=postgres")
            .option("dbtable", "(SELECT * FROM callcenter.cards WHERE addresstext like '%Казань,%' and applicantlocation is not null limit 20000) AS t")
//            .option("dbtable", "callcenter.cardcriminals")
            .load()


    }

    def cards: DataFrame = _cards

    def testData: DataFrame = _testData

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
