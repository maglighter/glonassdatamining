package org.grint.glonassdatamining.nlp

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import collection.JavaConverters._

class CustomAggregation() extends UserDefinedAggregateFunction {

    // Input Data Type Schema
    def inputSchema: StructType = StructType(Array(StructField("col5", ArrayType(StringType))))

    // Intermediate Schema
    def bufferSchema = StructType(Array(
        StructField("col5_collapsed", ArrayType(StringType))))

    // Returned Data Type .
    def dataType: DataType = ArrayType(StringType)

    // Self-explaining
    def deterministic = true

    // This function is called whenever key changes
    def initialize(buffer: MutableAggregationBuffer) = {
        buffer(0) = Array.empty[String] // initialize array
    }

    // Iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row) = {
        buffer(0) =
            if (!input.isNullAt(0))
                buffer.getList[String](0).toArray ++ input.getList[String](0).toArray
            else
                buffer.getList[String](0).toArray
    }

    // Merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
        buffer1(0) = buffer1.getList[String](0).toArray ++ buffer2.getList[String](0).toArray
    }

    // Called after all the entries are exhausted.
    def evaluate(buffer: Row) = {
        buffer.getList[String](0).asScala.toList
    }
}

