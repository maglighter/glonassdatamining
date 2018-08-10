package org.grint.glonassdatamining.clusterization

import java.sql.Timestamp
import dbis.stark._
import org.apache.spark.rdd.RDD
import dbis.stark.spatial.SpatialRDD._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.grint.glonassdatamining.app.Config
import org.grint.glonassdatamining.datahelper.Input

/**
  * Spatio-temporal clustering using DBSCAN algorithm on points (modified STARK realisation)
  *
  * @param configuration      configuration parameters for algorithm
  */
class SpatioTemporalPointDbscan(configuration: Config, sparkSession: SparkSession) {
    import sparkSession.implicits._
    /**
      * input rdd with spatio-temporal data
      */
    private var _input: RDD[(STObject, Payload)] = _

    /**
      * Creating input rdd from the resulting table for clustering
      *
      * @param inputTable imput table
      */
    def prepareInput(inputTable: DataFrame): Unit = {
        val input: RDD[Input] = inputTable
            .select("createddatetime", "id", "longitude",
                "latitude", "description", "gisexgroup")
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
        _input = input.keyBy(_.wktPoint).map { case (wktPoint, inpt) =>
            (STObject(wktPoint, inpt.timestamp.getTime),
                    Payload(inpt.id,
                            inpt.latitude,
                            inpt.longitude,
                            inpt.description,
                            inpt.gisexgroup))
        }
        println("Partit " + _input.partitions.size)
    }

    /**
      * Run DBSCAN clustering. Separates points on clusters, where
      * points are close to each other in space and time
      *
      * @return separated on clusters dataset
      */
    def run(): RDD[(STObject, (Int, Payload))] = {
        val clusters = _input.cluster(
            epsilon = configuration.epsilonSpatial,
            timeEpsilon = configuration.epsilonTemporal,
            minPts = configuration.minPts,
            keyExtractor = {
                case (g, v) => v.id
            })
        clusters
    }

}
