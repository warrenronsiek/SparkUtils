# SparkUtils
Generic spark utilities. Right now only has dataframe SnapshotTest 
util, but will add more stuff as necessary.

## SnapshotTest
Snapshot tests store copies of data in parquet in your `test/resources` dir.
When calling the `assertSnapshot("snapName", newDf, "joinCol1", "joinCol2")`
the library will read the stored snapshot as a dataframe, join it to the provided
dataframe on the joinColumns, and then test the dataframes for equality
along every column. If it finds any diffs, it will print them and fail 
the test. If it doesn't find any diffs, or if a snapshot doesn't exist,
it will succeed. 

### Example Usage
```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import com.warren_r.sparkutils.snapshot.SnapshotTest

class SnapshotTestTest extends AnyFlatSpec with SnapshotTest {
  val sparkConf: SparkConf = new SparkConf()
  val sparkSession: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("RunningTests")
    .config(sparkConf)
    .getOrCreate()
  
  val df = sparkSession.sparkContext.parallelize(Seq((1, "a"), (2, "b")))
  
  "snapshots" should "pass" in {
    assertSnapshot("demoSnapShot", df, "_c1")
  }
}
```