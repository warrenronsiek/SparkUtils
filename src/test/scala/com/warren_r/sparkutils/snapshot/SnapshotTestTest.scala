package com.warren_r.sparkutils.snapshot

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec


class SnapshotTestTest extends AnyFlatSpec with SnapshotTest {
  val sparkConf: SparkConf = new SparkConf()
  val sparkSession: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("RunningTests")
    .config(sparkConf)
    .getOrCreate()

  val schema: StructType = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("val1", StringType, nullable = false),
    StructField("val2", IntegerType, nullable = false)
  ))
  val goodDf: DataFrame = sparkSession.read.schema(schema).csv(getClass.getResource("/testdata.csv").getPath)
  val badDf: DataFrame = sparkSession.read.schema(schema).csv(getClass.getResource("/badtestdata.csv").getPath)

  "snapshot path creation" should "create correct paths" in {
    assert(snapshotPath("testpath") ==
      List(System.getProperty("user.dir"), "src", "test", "resources", "com", "warren_r", "sparkutils", "snapshot",
        "snapshottesttest", "testpath").mkString("/"))
  }

  "snapshot testing" should "validate snapshots" in {
    assertSnapshot("gooddata", goodDf, List("id"))
  }

//  it should "invalidate invalid snapshots" in {
//    assert(!compareSnapshot(badDf, goodDf, List("id")))
//  }
}
