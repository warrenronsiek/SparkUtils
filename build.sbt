name := "SparkUtils"

version := "0.1.5"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.scalatest" %% "scalatest" % "3.2.17",
  "ch.qos.logback" % "logback-classic" % "1.3.11" % Test,
  "ch.qos.logback" % "logback-core" % "1.3.11" % Test
)

