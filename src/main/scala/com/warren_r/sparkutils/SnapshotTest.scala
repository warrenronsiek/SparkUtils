package com.warren_r.sparkutils

import java.io.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.StructType
import org.scalatest.Assertion
import org.scalatest.Assertions._

import scala.reflect.io.Directory
import scala.util.{Failure, Success, Try}

trait SnapshotTest extends LazyLogging {
  val sparkConf: SparkConf = new SparkConf()
  val sparkSession: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("RunningTests")
    .config(sparkConf)
    .getOrCreate()
  implicit val sparkSessionImpl: SparkSession = sparkSession
  implicit val sqlImpl: SQLContext = sparkSession.sqlContext

  import sparkSession.sqlContext.implicits._

  private[sparkutils] def snapshotPath(snapshotName: String): String = {
    val testResources: String = List(System.getProperty("user.dir"), "src", "test", "resources").mkString("/")
    val resourcePath: String = this.getClass.getName.toLowerCase().replace('.', '/')
    Array(testResources, resourcePath, snapshotName).mkString("/")
  }

  private[sparkutils] def saveSnapshot(snapshotName: String, dataFrame: DataFrame): Unit = {
    val path: String = snapshotPath(snapshotName)
    val dir = new Directory(new File(path))
    if (dir.exists) {
      dir.deleteRecursively()
    }
    dataFrame.write.parquet(path)
  }

  private[sparkutils] def compareSnapshot(newDF: DataFrame, snapshotDF: DataFrame, sortBy: List[String]): Boolean = {
    if (newDF.columns.toSet != snapshotDF.columns.toSet) {
      val nc: String = newDF.columns.toSet.diff(snapshotDF.columns.toSet).toArray.mkString(", ")
      val oc: String = snapshotDF.columns.toSet.diff(newDF.columns.toSet).toArray.mkString(", ")
      logger.error("Non-overlapping columns.")
      logger.info("Columns in passed dataframe missing in snapshot: " + nc)
      logger.info("Columns in snapshot missing in passed dataframe: " + oc)
      return false
    }
    if (newDF.isEmpty) {
      logger.error("New dataframe is empty. Cannot snapshot an empty dataframe.")
      return false
    }
    val hd :: tail = sortBy
    val newSorted = newDF.sort(hd, tail: _*).withColumn("index", monotonically_increasing_id())
      .alias("new")
    val snapshotSorted = snapshotDF.sort(hd, tail: _*).withColumn("index", monotonically_increasing_id())
      .alias("snap")
    val joined = snapshotSorted.join(newSorted, Seq("index"), "outer").cache()
    val mismatchedCols: Array[Dataset[Row]] = newSorted.columns.toSet.union(snapshotSorted.columns.toSet).toArray
      .filter(colname => colname != "index")
      .map(colname =>
        joined
          .select($"index",
            col(s"new.$colname"),
            col(s"snap.$colname"))
          .where(col(s"new.$colname") =!= col(s"snap.$colname"))
      ).filter(dataset => dataset.count() > 0)
    if (mismatchedCols.length > 0) {
      logger.error("ERROR: snapshot matching failure")
      logger.info("New data: ")
      newSorted.show(100)
      logger.info("Snapshot: ")
      snapshotSorted.show(100)
      logger.info("Mismatched data: ")
      mismatchedCols.foreach(dataset => dataset.show())
      return false
    }
    true
  }

  def assertSnapshot(snapshotName: String, dataFrame: DataFrame, sortBy: List[String]): Assertion = {
    assert(
      Try {
        val snapshot = sparkSession.read.parquet(snapshotPath(snapshotName))
        compareSnapshot(dataFrame, snapshot, sortBy)
      } match {
        case Success(b) => b
        case Failure(ex) if ex.getMessage.contains("Path does not exist") =>
          logger.info("Snapshot does not exist, creating it.")
          saveSnapshot(snapshotName, dataFrame)
          true
        case Failure(ex) =>
          logger.error(ex.getMessage)
          false
      })
  }

  def assertSchema(snapshotName: String, schema: StructType, sortBy: List[String]): Assertion = {
    assert(
      Try {
        val snapshot = sparkSession.read.parquet(snapshotPath(snapshotName))
        val snapshot2 = sparkSession.read.schema(schema).parquet(snapshotPath(snapshotName))
        compareSnapshot(snapshot2, snapshot, sortBy)
      } match {
        case Success(b) => b
        case Failure(ex) =>
          logger.error(ex.getMessage)
          false
      }
    )
  }
}
