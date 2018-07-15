package org.grint.glonassdatamining.app

/**
  *
  * Default configuration for application
  *
  * @param epsilonSpatial spatial epsilon parameter for DBSCAN
  * @param epsilonTemporal temporal epsilon parameter for DBSCAN in seconds
  * @param minPts minimum number of points in cluster parameter for DBSCAN
  * @param limit maximum number of rows in input table
  * @param output result file path
  */
case class Config(epsilonSpatial: Double = 0.003,
                  epsilonTemporal: Long = 60 * 60, //60 min
                  minPts: Int = 2,
                  limit: Int = 1500, // '-1' - no limit
                  output: java.net.URI = java.net.URI.create("."))
