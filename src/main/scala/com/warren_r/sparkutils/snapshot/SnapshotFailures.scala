package com.warren_r.sparkutils.snapshot

private[snapshot] object SnapshotFailures {

  abstract class SnapshotFailure {
    def message: String
  }

  case class MismatchedColumns(newCols: Array[String], snapCols: Array[String]) extends SnapshotFailure {
    def message: String = "cols in new data, not in snapshot:\n" +
      newCols.toSet.diff(snapCols.toSet).toArray.mkString(", ") +
      "\ncols in snapshot, not in new data:\n" +
      snapCols.toSet.diff(newCols.toSet).toArray.mkString(", ")
  }

  case class EmptyData() extends SnapshotFailure {
    def message: String = "The assertion data is empty. You cannot snapshot compare an empty dataframe."
  }

  case class MismatchedData() extends SnapshotFailure {
    def message: String = "Data in snapshot did not match provided data. See stdout for details."
  }
}
