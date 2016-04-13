name := "spark-lucenerdd"
version := "0.0.2-SNAPSHOT"
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

val specs2_core               = "org.specs2"                     %% "specs2-core"             % "2.3.11" % "test"
val scala_check               = "org.scalacheck"                 %% "scalacheck"              % "1.12.2" % "test"

val scalatest                 = "org.scalatest"                  %% "scalatest"                % "2.2.6"  % "test"
val spark_testing_base        = "com.holdenkarau"                %% "spark-testing-base"       % s"${sparkVersion}_0.3.1" % "test" intransitive()


libraryDependencies ++= Seq(
  spark_core % "provided",
  spark_sql % "provided",
  "com.gilt" %% "lib-lucene-sugar" % "0.2.3",
  specs2_core,
  scalatest,
  spark_testing_base
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value
(compile in Compile) <<= (compile in Compile) dependsOn compileScalastyle


parallelExecution in Test := false

