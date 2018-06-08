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

//        val testTable = DataLoader.addressesWithGps.join(DataLoader.cards, "id")
        val testTable = DataLoader.testData
        println(testTable.count())

        //arr(2).asInstanceOf[Double]
        val raw: RDD[(Long, String, Double, Double, String)] = testTable
            .select("createddatetime", "id", "longitude", "latitude")
            .map(arr => (arr(0).asInstanceOf[Long], arr(1).toString, arr.getDouble(2),
                arr.getDouble(3), "POINT(" + arr(2) + " " + arr(3) + ")") ).rdd

        raw.foreach(f => println(f))

        val spatialRDD = raw.keyBy(_._5).map{case (k,v) => (STObject(k), v)}

        // run DBSCAN. first parmeter - minPts, second - epsilon
        // should separaite points on clusters (clusterId shoulld be different)
        val clusters = spatialRDD.cluster(3, 0.5, {case (g,v) => v._2 })
        println(clusters.count())
        clusters.foreach(f => println(f))
    }

}