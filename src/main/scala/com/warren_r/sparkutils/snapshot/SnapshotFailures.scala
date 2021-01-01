package com.warren_r.sparkutils.snapshot

object SnapshotFailures {

  abstract class SnapshotFailure {
    def message: String
  }

  case class MismatchedColumns(newCols: Array[String], snapCols: Array[String]) extends SnapshotFailure {
    def message: String = "Cols in new data, not in snapshot: " + newCols.toSet.diff(snapCols.toSet).toArray.mkString(", ") +
      "\n cols in snapshot, not in new data: " + snapCols.toSet.diff(newCols.toSet).toArray.mkString(", ")
  }

  case class EmptyData() extends SnapshotFailure {
    def message: String = "The assertion data is empty. You cannot snapshot compare an empty dataframe."
  }

  case class MismatchedData() extends SnapshotFailure {
    def message: String = "Data in snapshot did not match provided data. See stdout for details."
  }
}
