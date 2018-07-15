package org.grint.glonassdatamining.datahelper

import java.sql.Timestamp

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
