package com.warren_r.sparkutils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class SnapshotTestTest extends AnyFlatSpec with SnapshotTest {

  val schema: StructType = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("val1", StringType, nullable = false),
    StructField("val2", IntegerType, nullable = false)
  ))
  val goodDf: DataFrame = sparkSession.read.schema(schema).csv(getClass.getResource("/testdata.csv").getPath)
  val badDf: DataFrame =  sparkSession.read.schema(schema).csv(getClass.getResource("/badtestdata.csv").getPath)

  "snapshot testing" should "validate snapshots" in {
    assertSnapshot("gooddata", goodDf, List("id"))
  }

//  it should "invalidate invalid snapshots" in {
//
//  }
}
