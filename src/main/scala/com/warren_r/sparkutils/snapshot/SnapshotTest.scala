package com.warren_r.sparkutils.snapshot

import com.typesafe.scalalogging.LazyLogging
import com.warren_r.sparkutils.snapshot.SnapshotFailures._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
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

  private[sparkutils] def compareSnapshot(newDF: DataFrame, snapshotDF: DataFrame, sortBy: String*): Option[SnapshotFailure] =
    compareSnapshot(newDF, snapshotDF, sortBy.toList)

  private[sparkutils] def compareSnapshot(newDF: DataFrame, snapshotDF: DataFrame, sortBy: List[String]): Option[SnapshotFailure] = {
    if (newDF.columns.toSet != snapshotDF.columns.toSet) {
      return Some(MismatchedColumns(newDF.columns, snapshotDF.columns))
    }
    if (newDF.isEmpty) {
      return Some(EmptyData())
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
      mismatchedCols.foreach(dataset => dataset.show())
      return Some(MismatchedData())
    }
    None
  }

  private def matchSnapshotFailure(snapshotFailure: Option[SnapshotFailure]): Boolean = snapshotFailure match {
    case None => true
    case Some(sf: SnapshotFailure) =>
      logger.info(sf.message)
      false
  }

  def assertSnapshot(snapshotName: String, dataFrame: DataFrame, sortBy: List[String]): Assertion = {
    assert(
      Try {
        val snapshot = sparkSession.read.parquet(snapshotPath(snapshotName))
        compareSnapshot(dataFrame, snapshot, sortBy)
      } match {
        case Success(b) => matchSnapshotFailure(b)
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
        case Success(b) => matchSnapshotFailure(b)
        case Failure(ex) =>
          logger.error(ex.getMessage)
          false
      }
    )
  }
}
