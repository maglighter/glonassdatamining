package org.grint.glonassdatamining.clusterization

import dbis.stark._
import dbis.stark.spatial.SpatialRDD._
import org.apache.spark.rdd.RDD
import org.grint.glonassdatamining.app.Config

/**
  * Spatio-temporal clustering using DBSCAN algorithm on points (modified STARK realisation)
  */
object SpatioTemporalPointDbscan {

    /**
      * Run DBSCAN clustering, should separate points on clusters where
      * points are close to each other at space and time
      *
      * @param configuration configuration parameters for algorithm
      * @param spatialTemporalRDD input rdd with spatio-temporal data
      * @return separated on clusters dataset
      */
    def run(configuration: Config, spatialTemporalRDD: RDD[(STObject, Payload)]): RDD[(STObject, (Int, Payload))] = {
        val clusters = spatialTemporalRDD.cluster(
            epsilon = configuration.epsilonSpatial,
            timeEpsilon = configuration.epsilonTemporal,
            minPts = configuration.minPts,
            keyExtractor = {
                case (g, v) => v.id
            })
        clusters
    }

}
