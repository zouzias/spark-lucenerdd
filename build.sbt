name := "spark-lucenerdd"
version := "0.0.1"
organization := "org.zouzias"
scalaVersion := "2.11.7"
licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

publishMavenStyle := false

pomExtra := (
  <url>https://github.com/zouzias/spark-lucenerdd</url>
  <scm>
    <url>git@github.com:amplab/spark-lucenerdd.git</url>
    <connection>scm:git:git@github.com:zouzias/spark-lucenerdd.git</connection>
  </scm>
  <developers>
    <developer>
      <id>zouzias</id>
      <name>Anastasios Zouzias</name>
      <url>https://github.com/zouzias</url>
    </developer>
  </developers>
)

val sparkVersion = "1.5.2"

val spark_core                = "org.apache.spark"               %% "spark-core"               % sparkVersion
val spark_sql                 = "org.apache.spark"               %% "spark-sql"                % sparkVersion


libraryDependencies ++= Seq(
  spark_core % "provided",
  spark_sql % "provided",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"
)
