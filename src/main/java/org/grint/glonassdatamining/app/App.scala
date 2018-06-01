package org.grint.glonassdatamining.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.grint.glonassdatamining.dataloader.DataLoader
import dbis.stark._
import dbis.stark.spatial.SpatialRDD._
import org.apache.spark.rdd.RDD
import org.apache.spark

object App {

    def main(args: Array[String]): Unit = {
        //Initialize SparkSession
        val sparkSession = SparkSession
            .builder()
            .appName("GLONASS+112 data mining")
            .master("local[*]")
            .getOrCreate()

        import sparkSession.implicits._

        DataLoader.load(sparkSession)

        val testTable = DataLoader.addressesWithGps.join(DataLoader.cards, "id")
        println(testTable.count())

        val raw: RDD[(String, String, Double, Double, String)] = testTable
            .select("createddatetime", "id", "longitude", "latitude", "address")
            .map(arr => (arr(0).toString, arr(1).toString, arr(2).asInstanceOf[Double],
                arr(3).asInstanceOf[Double], "POINT(" + arr(2) + " " + arr(3) + ")") ).rdd

        val spatialRDD = raw.keyBy(_._5).map{case (k,v) => (STObject(k), v)}

        val clusters = spatialRDD.cluster(20, 0.5, {case (g,v) => v })
        println(clusters.count())
    }

}