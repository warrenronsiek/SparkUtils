package com.warren_r.sparkutils.snapshot

import com.warren_r.sparkutils.snapshot.SnapshotFailures.{EmptyData, MismatchedColumns, MismatchedData}
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
  val goodData: DataFrame = sparkSession.read.schema(schema).option("header", "true")
    .csv(getClass.getResource("/testdata.csv").getPath)

  "snapshot path creation" should "create correct paths" in {
    assert(snapshotPath("testpath") ==
      List(System.getProperty("user.dir"), "src", "test", "resources", "com", "warren_r", "sparkutils", "snapshot",
        "snapshottesttest", "testpath").mkString("/"))
  }

  "schema testing" should "validate schemas" in {
    assertSchema("gooddata", schema)
  }

  "snapshot testing" should "validate snapshots" in {
    assertSnapshot("gooddata", goodData, List("id"))
  }

  it should "detect mismatched data" in {
    val mismatchedData: DataFrame = sparkSession.read.schema(schema).option("header", "true")
      .csv(getClass.getResource("/mismatchedData.csv").getPath)
    assertResult(Some(MismatchedData())) {
      compareSnapshot(mismatchedData, goodData, "id")
    }
  }

  it should "detect mismatched columns" in {
    val badSchema: StructType = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("val1", StringType, nullable = false),
      StructField("val2", IntegerType, nullable = false),
      StructField("val3", StringType, nullable = false)
    ))
    val mismatchedData: DataFrame = sparkSession.read.schema(badSchema).option("header", "true")
      .csv(getClass.getResource("/mismatchedColumns.csv").getPath)
    val mismatch = MismatchedColumns(mismatchedData.columns, goodData.columns)
    val comparison = compareSnapshot(mismatchedData, goodData, "id").get
    assert(comparison.message == mismatch.message)
  }

  it should "detect joincol diffs" in {
    val mismatchedJoin: DataFrame = sparkSession.read.schema(schema).option("header", "true")
      .csv(getClass.getResource("/mismatchedJoin.csv").getPath)
    assertResult(Some(MismatchedData())) {
      compareSnapshot(mismatchedJoin, goodData, "id")
    }
  }

  it should "detect empty data" in {
    val emptyData: DataFrame = sparkSession.read.schema(schema).option("header", "true")
      .csv(getClass.getResource("/emptyData.csv").getPath)
    assertResult(Some(EmptyData())) {
      compareSnapshot(emptyData, goodData, "id")
    }
  }
}
