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

  val goodSchema: StructType = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("val1", StringType, nullable = false),
    StructField("val2", IntegerType, nullable = false)
  ))
  val badSchema: StructType = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("val1", StringType, nullable = false),
    StructField("val2", IntegerType, nullable = false),
    StructField("val3", StringType, nullable = false)
  ))
  val goodData: DataFrame = sparkSession.read.schema(goodSchema).option("header", "true")
    .csv(getClass.getResource("/testdata.csv").getPath)
  val badDataExtraColumn: DataFrame = sparkSession.read.schema(badSchema).option("header", "true")
    .csv(getClass.getResource("/mismatchedColumns.csv").getPath)

  "snapshot path creation" should "create correct paths" in {
    assert(snapshotPath("testpath") ==
      List(System.getProperty("user.dir"), "src", "test", "resources", "com", "warren_r", "sparkutils", "snapshot",
        "snapshottesttest", "testpath").mkString("/"))
  }

  "schema testing" should "validate schemas" in {
    assertSchema("gooddata", goodData, goodSchema)
  }

  it should "invalidate invalid schemas" in {
    assert(!schemaValidation("gooddata", goodData, badSchema))
  }

  it should "detect bad data" in {
    assert(!schemaValidation("goodData", badDataExtraColumn, goodSchema))
  }

  "snapshot testing" should "validate snapshots" in {
    assertSnapshot("gooddata", goodData, List("id"))
  }

  it should "detect mismatched data" in {
    val mismatchedData: DataFrame = sparkSession.read.schema(goodSchema).option("header", "true")
      .csv(getClass.getResource("/mismatchedData.csv").getPath)
    assertResult(Some(MismatchedData())) {
      compareSnapshot(mismatchedData, goodData, "id")
    }
  }

  it should "detect mismatched columns" in {
    val mismatch = MismatchedColumns(badDataExtraColumn.columns, goodData.columns)
    val comparison = compareSnapshot(badDataExtraColumn, goodData, "id").get
    assert(comparison.message == mismatch.message)
  }

  it should "detect joincol diffs" in {
    val mismatchedJoin: DataFrame = sparkSession.read.schema(goodSchema).option("header", "true")
      .csv(getClass.getResource("/mismatchedJoin.csv").getPath)
    assertResult(Some(MismatchedData())) {
      compareSnapshot(mismatchedJoin, goodData, "id")
    }
  }

  it should "detect empty data" in {
    val emptyData: DataFrame = sparkSession.read.schema(goodSchema).option("header", "true")
      .csv(getClass.getResource("/emptyData.csv").getPath)
    assertResult(Some(EmptyData())) {
      compareSnapshot(emptyData, goodData, "id")
    }
  }
}
