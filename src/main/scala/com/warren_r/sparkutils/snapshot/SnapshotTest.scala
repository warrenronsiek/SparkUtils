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

  /**
   * Creates the paths to the locations of snapshots inside of the test/resources/ directory.
   * @param snapshotName whatever you want to call the snapshot, should be unique amongst snapshots in the same testsuite
   * @return the fully qualified path of the snapshot
   */
  private[snapshot] def snapshotPath(snapshotName: String): String = {
    val testResources: String = List(System.getProperty("user.dir"), "src", "test", "resources").mkString("/")
    val resourcePath: String = this.getClass.getName.toLowerCase().replace('.', '/')
    Array(testResources, resourcePath, snapshotName).mkString("/")
  }

  /**
   * If the snapshot doesn't exist, recursively generate container directories and then write the snapshot.
   * @param snapshotName whatever you want to call the snapshot, should be unique amongst snapshots in the same testsuite
   * @param dataFrame the dataframe to be saved as a snapshot
   */
  private[snapshot] def saveSnapshot(snapshotName: String, dataFrame: DataFrame): Unit = {
    val path: String = snapshotPath(snapshotName)
    val dir = new Directory(new File(path))
    if (dir.exists) {
      dir.deleteRecursively()
    }
    dataFrame.write.parquet(path)
  }

  private[snapshot] def compareSnapshot(newDF: DataFrame, snapshotDF: DataFrame, joinOn: String*): Option[SnapshotFailure] =
    compareSnapshot(newDF, snapshotDF, joinOn.toList)

  /**
   * Joins the snapshot to the passed new data, and then does a column-by-column diff, and prints the results. Returns
   * a case class representing the failure type that gets translated later into a failed assertion/error.
   * @param newDF the dataframe to compare to the snapshot
   * @param snapshotDF the snapshot as read from the resources directory
   * @param joinOn a set of columns to join the snapshot to the passed newDF
   * @return an optional failure
   */
  private[snapshot] def compareSnapshot(newDF: DataFrame, snapshotDF: DataFrame, joinOn: List[String]): Option[SnapshotFailure] = {
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

  def assertSnapshot(snapshotName: String, dataFrame: DataFrame, joinOn: String*): Assertion =
    assertSnapshot(snapshotName, dataFrame, joinOn.toList)

  /**
   * Asserts that there are no diffs between the snapshot and the passed dataframe. This is the main API.
   * @param snapshotName whatever you want to call the snapshot, should be unique amongst snapshots in the same testsuite
   * @param dataFrame the dataframe you want to compare to the snapshot
   * @param joinOn a set of columns to join the snapshot to the passed newDF
   * @return an assertion that fails if there are any diffs between the passed data and the snapshot
   */
  def assertSnapshot(snapshotName: String, dataFrame: DataFrame, joinOn: List[String]): Assertion = {
    assert(
      Try {
        val snapshot = sparkSession.read.parquet(snapshotPath(snapshotName))
        compareSnapshot(dataFrame, snapshot, joinOn)
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

  private[snapshot] def schemaValidation(snapshotName: String, schema:StructType): Boolean = {
    Try {
      val snapshot: DataFrame = sparkSession.read.parquet(snapshotPath(snapshotName))
      // the map here is because parquet doesn't seem to store the nullable component of StructFields
      snapshot.schema.map(sf => (sf.name, sf.dataType)) == schema.map(sf => (sf.name, sf.dataType))
    } match {
      case Success(b) => b
      case Failure(ex) =>
        logger.error(ex.getMessage)
        false
    }
  }

  /**
   * Asserts that the passed schema matches that of the snapshot.
   * @param snapshotName the name of the snapshot's schema you want to compare agaisnt
   * @param schema the schema you want to validate against the snapshot
   * @return
   */
  def assertSchema(snapshotName: String, schema: StructType): Assertion =
    assert(schemaValidation(snapshotName, schema))
}
