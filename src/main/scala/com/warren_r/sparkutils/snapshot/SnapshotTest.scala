package com.warren_r.sparkutils.snapshot

import com.typesafe.scalalogging.LazyLogging
import com.warren_r.sparkutils.snapshot.SnapshotFailures._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalatest.Assertion
import org.scalatest.Assertions.assert

import java.io.File
import scala.reflect.io.Directory
import scala.util.{Failure, Success, Try}

trait SnapshotTest extends LazyLogging {
  val sparkConf: SparkConf
  val sparkSession: SparkSession
  lazy implicit val sparkSessionImpl: SparkSession = sparkSession
  lazy implicit val sqlImpl: SQLContext = sparkSession.sqlContext

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

  private[sparkutils] def compareSnapshot(newDF: DataFrame, snapshotDF: DataFrame, joinOn: String*): Option[SnapshotFailure] =
    compareSnapshot(newDF, snapshotDF, joinOn.toList)

  private[sparkutils] def compareSnapshot(newDF: DataFrame, snapshotDF: DataFrame, joinOn: List[String]): Option[SnapshotFailure] = {
    if (newDF.columns.toSet != snapshotDF.columns.toSet) {
      return Some(MismatchedColumns(newDF.columns, snapshotDF.columns))
    }
    if (newDF.isEmpty) {
      return Some(EmptyData())
    }
    val joined = snapshotDF.alias("snap").join(newDF.alias("new"), joinOn, "outer").cache()
    val mismatchedCols: Array[Dataset[Row]] = newDF.columns.toSet.union(snapshotDF.columns.toSet).toArray
      .filter(colname => !joinOn.toSet.contains(colname))
      .map(colname =>
        joined
          .select(col(s"snap.$colname") :: col(s"new.$colname") :: joinOn.map(s => col(s)).reverse: _*)
          .where(!(col(s"new.$colname") <=> col(s"snap.$colname")))
      ).filter(dataset => dataset.count() > 0)
    if (mismatchedCols.length > 0) {
      mismatchedCols.foreach(dataset => dataset.show())
      return Some(MismatchedData())
    }
    None
  }

  def assertSnapshot(snapshotName: String, dataFrame: DataFrame, sortBy: List[String]): Assertion = {
    assert(
      Try {
        val snapshot = sparkSession.read.parquet(snapshotPath(snapshotName))
        compareSnapshot(dataFrame, snapshot, sortBy)
      } match {
        case Success(None) => true
        case Success(Some(snapFailure)) =>
          logger.info(snapFailure.message)
          false
        case Failure(ex) if ex.getMessage.contains("Path does not exist") =>
          logger.info("Snapshot does not exist, creating it.")
          saveSnapshot(snapshotName, dataFrame)
          true
        case Failure(ex) =>
          logger.error(ex.getMessage)
          false
      }
    )
  }

  def assertSchema(snapshotName: String, schema: StructType): Assertion = {
    assert(
      Try {
        val snapshot: DataFrame = sparkSession.read.parquet(snapshotPath(snapshotName))
        snapshot.schema == schema
      } match {
        case Success(b) => b
        case Failure(ex) =>
          logger.error(ex.getMessage)
          false
      }
    )
  }
}
