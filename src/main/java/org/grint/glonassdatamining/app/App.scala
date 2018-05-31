package org.grint.glonassdatamining.app

import org.apache.spark.sql.SparkSession
import org.grint.glonassdatamining.dataloader.DataLoader

object App {

    def main(args: Array[String]): Unit = {
        //Initialize SparkSession
        val sparkSession = SparkSession
            .builder()
            .appName("GLONASS+112 data mining")
            .master("local[*]")
            .getOrCreate()

        DataLoader.load(sparkSession)

        println(DataLoader.addressesWithGps.join(DataLoader.cards, "id").count())
    }

}