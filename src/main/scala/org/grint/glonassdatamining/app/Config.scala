package org.grint.glonassdatamining.app

/**
  *
  * Default configuration for application
  *
  * @param epsilonSpatial spatial epsilon parameter for DBSCAN
  * @param epsilonTemporal temporal epsilon parameter for DBSCAN in seconds
  * @param minPts minimum number of points in cluster parameter for DBSCAN
  * @param limit maximum number of rows in input table
  * @param topKeywordsNumber number of top words that will be selected from each category based on IF-IDF
  * @param minConfidence minimum confidence for association rules finding
  * @param minSupport minimum support for association rules finding
  * @param output result file path
  */
case class Config(epsilonSpatial: Double = 0.003,
                  epsilonTemporal: Long = 60 * 60, //60 min
                  minPts: Int = 2,
                  limit: Int = -1, // '-1' - no limit
                  topKeywordsNumber: Int = 20,
                  minConfidence: Double = 0.8,
                  minSupport: Double = 0.1,
                  output: java.net.URI = java.net.URI.create("."))
