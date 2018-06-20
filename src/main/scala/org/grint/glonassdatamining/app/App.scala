package org.grint.glonassdatamining.app

import java.sql.Timestamp

import dbis.stark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.grint.glonassdatamining.datahelper.{DataHelper, OutputCsv}
import org.grint.glonassdatamining.clusterization.{Payload, SpatioTemporalPointDbscan}

/**
  * Format of the input RDD for clustering
  *
  * @param timestamp point datetime value in milliseconds since 1970
  * @param id point id
  * @param longitude point longitude
  * @param latitude point longitude latitude
  * @param wktPoint point in WKT format
  * @param description text description of the point
  * @param gisexgroup category
  */
case class Input(timestamp: Timestamp,
                 id: String,
                 longitude: Double,
                 latitude: Double,
                 wktPoint: String,
                 description: String,
                 gisexgroup: String)

/**
  * Main file of the project
  *
  * Run with:
  * spark-submit --class org.grint.glonassdatamining.app.App --master local[4] \
  * target/target/glonass-data-mining-1.0-SNAPSHOT.jar \
  * --output output.csv --epsilonSpatial 0.003 --epsilonTemporal=3600 --minPts 2
  */
object App {

    def main(args: Array[String]): Unit = {
        // initializing SparkSession
        val sparkSession = SparkSession
            .builder()
            .appName("GLONASS+112 data mining")
            .master("local[4]")
            .getOrCreate()
        import sparkSession.implicits._

        // parsing program arguments
        var configuration = Config()
        val parser = new scopt.OptionParser[Config]("glonass112-config") {
            head("glonass112-config", "0.1")
            opt[Double]("epsilonSpatial") action { (x, c) => c.copy(epsilonSpatial = x) } text "spatial epsilon parameter for DBSCAN (0.003 by default"
            opt[Long]("epsilonTemporal") action { (x, c) => c.copy(epsilonTemporal = x) } text "temporal epsilon parameter for DBSCAN in seconds (60 min by default"
            opt[Int]("minPts") action { (x, c) => c.copy(minPts = x) } text "minimum number of points in cluster parameter for DBSCAN (2 by default)"
            opt[Int]("limit") action { (x, c) => c.copy(limit = x) } text "maximum number of rows in input table"
            opt[java.net.URI]('o', "output") action { (x, c) => c.copy(output = x) } text "output is the result file"
            help("help") text "prints this usage text"
        }
        parser.parse(args, Config()) match {
            case Some(c) =>
                configuration = Config(c.epsilonSpatial,
                    c.epsilonTemporal * 1000,
                    c.minPts,
                    c.limit,
                    if (c.output.toString == ".")
                        java.net.URI.create("./output_%seps_%smin_%srows.csv"
                        .format(c.epsilonSpatial, c.epsilonTemporal, c.limit))
                    else
                        configuration.output
                )
            case None =>
                // arguments are bad, error message will have been displayed
                return
        }
        println("Params: spatial epsilon %s, temporal epsilon %s, minimum points %s, output file path %s"
            .format(configuration.epsilonSpatial, configuration.epsilonTemporal/1000, configuration.minPts, configuration.output))

        // loading data from different sources (csv, postgres, etc...)
        val dataHelper = new DataHelper(sparkSession)
        dataHelper.load(configuration.limit)

        // join table containing GPS coordinates of addresses and table 'cards' by id
        val dbTable = dataHelper.addressesWithGps.join(dataHelper.cards, "id")

        // creating input rdd from the resulting table for clustering
        val input: RDD[Input] = dbTable
            .select("createddatetime", "id", "longitude", "latitude", "description", "gisexgroup")
            .map(row => {
                Input(row(0).asInstanceOf[Timestamp],
                    row(1).toString,
                    row.getDouble(2),
                    row.getDouble(3),
                    "POINT(" + row(2) + " " + row(3) + ")",
                    (if (row.isNullAt(4)) "" else row.getString(4)).replaceAll("\n", ""),
                    if (row.isNullAt(5)) "" else row.getString(5))
            }).rdd

        println("Input rows count: " + input.count())
        val spatialTemporalRDD = input.keyBy(_.wktPoint).map { case(wktPoint, inpt) =>
            (STObject(wktPoint, inpt.timestamp.getTime),
                Payload(inpt.id,
                    inpt.latitude,
                    inpt.longitude,
                    inpt.description,
                    inpt.gisexgroup)) }

        // clustering input rdd
        val clusters = SpatioTemporalPointDbscan.run(configuration, spatialTemporalRDD)

        val model = clusters.map {case (stObject, (clusterId, payload)) => OutputCsv(
            clusterId, payload.id, payload.latitude,
            payload.longitude, stObject.time.get.start.value,
            payload.description, payload.gisexgroup)}

        println("Number of clusters: " + model.groupBy(_.clusterId).count())

        // saving results of clusterization to CSV file
        dataHelper.write(model, configuration.output)
    }

}