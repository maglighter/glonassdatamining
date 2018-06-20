package org.grint.glonassdatamining.datahelper

/**
  * Output csv columns format
  *
  * @param clusterId id of a cluster containing the point
  * @param id point id
  * @param longitude point longitude
  * @param latitude point longitude latitude
  * @param timestamp point datetime value in milliseconds since 1970
  * @param description text description of the point
  * @param gisexgroup category
  */
case class OutputCsv(clusterId:Int,
                     id:String,
                     longitude:Double,
                     latitude:Double,
                     timestamp:Long,
                     description:String,
                     gisexgroup:String)