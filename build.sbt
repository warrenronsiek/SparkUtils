name := "SparkUtils"

version := "0.1"

scalaVersion := "2.12.12"

organization := "com.warren-r"
homepage := Some(url("https://github.com/warrenronsiek/SparkUtils"))
scmInfo := Some(ScmInfo(url("https://github.com/warrenronsiek/SparkUtils"), "git@github.com:warrenronsiek/SparkUtils.git"))
developers := List(Developer("warrenronsiek", "Warren Ronsiek", "warren@warren-r.com",
  url("https://github.com/warrenronsiek")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided"
)

