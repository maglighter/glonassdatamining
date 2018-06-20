package org.grint.glonassdatamining.app

/**
  *
  * Default configuration for application
  *
  * @param epsilonSpatial spatial epsilon parameter for DBSCAN
  * @param epsilonTemporal temporal epsilon parameter for DBSCAN
  * @param minPts minimum number of points in cluster parameter for DBSCAN
  * @param limit maximum number of rows in input table
  * @param output result file path
  */
case class Config(epsilonSpatial: Double = 0.003,
                  epsilonTemporal: Long = 60 * 60, //30 min
                  minPts: Int = 2,
                  limit: Int = 1500,
                  output: java.net.URI = java.net.URI.create("."))
