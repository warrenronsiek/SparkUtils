organization := "com.warren-r"
licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
publishMavenStyle := true
publishTo := sonatypePublishToBundle.value
sonatypeProfileName := "com.warren-r"

import xerial.sbt.Sonatype._

homepage := Some(url("https://github.com/warrenronsiek/SparkUtils"))
sonatypeProjectHosting := Some(GitHubHosting("warrenronsiek", "SparkUtils", "warrenronsiek@gmail.com"))
scmInfo := Some(ScmInfo(url("https://github.com/warrenronsiek/SparkUtils"),
  "git@github.com:warrenronsiek/SparkUtils.git"))
developers := List(Developer(
  id = "warrenronsiek",
  name = "Warren Ronsiek",
  email = "warrenronsiek@gmail.com",
  url = url("https://github.com/warrenronsiek"))
)
// needs to be put in ~/.sbt/(sbt version)/sonatype.sbt
// credentials += Credentials("Sonatype Nexus Repository Manager",
//  "oss.sonatype.org",
//  "warrenronsiek",
//  sys.env.getOrElse("SONATYPE_PASSWORD", ""))
