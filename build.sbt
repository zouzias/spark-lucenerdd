/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "spark-lucenerdd"
organization := "org.zouzias"
scalaVersion := "2.11.11"
crossScalaVersions := Seq("2.11.11")
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
homepage := Some(url("https://github.com/zouzias/spark-lucenerdd"))

scalacOptions ++= Seq("-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-language:implicitConversions")

javacOptions ++= Seq("-Xlint",
  "-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled"
)

// Add jcenter repo
resolvers += Resolver.jcenterRepo
resolvers += "Apache Repos" at "https://repository.apache.org/content/repositories/releases"

releaseCrossBuild := false
releasePublishArtifactsAction := PgpKeys.publishSigned.value

publishMavenStyle := true

sonatypeProfileName := "org.zouzias"

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  }
  else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <scm>
    <url>git@github.com:zouzias/spark-lucenerdd.git</url>
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

val luceneV = "7.0.0"

spName := "zouzias/spark-lucenerdd"
sparkVersion := "2.2.0"
spShortDescription := "Spark RDD with Lucene's query capabilities"
sparkComponents ++= Seq("core", "sql", "mllib")
spAppendScalaVersion := true
// This is necessary because of how we explicitly specify Spark dependencies
// for tests rather than using the sbt-spark-package plugin to provide them.
spIgnoreProvided := true

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)


// scalastyle:off
val scalactic                 = "org.scalactic"                  %% "scalactic" % "3.0.4"
val scalatest                 = "org.scalatest"                  %% "scalatest"                % "3.0.4" % "test"

val joda_time                 = "joda-time"                      % "joda-time"                 % "2.9.9"
val algebird                  = "com.twitter"                    %% "algebird-core"            % "0.13.2"
val joda_convert              = "org.joda"                       % "joda-convert"              % "1.9.2"
val spatial4j                 = "org.locationtech.spatial4j"     % "spatial4j"                 % "0.6"

val typesafe_config           = "com.typesafe"                   % "config"                    % "1.3.1"

val lucene_facet              = "org.apache.lucene"              % "lucene-facet"              % luceneV
val lucene_analyzers          = "org.apache.lucene"              % "lucene-analyzers-common"   % luceneV
val lucene_query_parsers      = "org.apache.lucene"              % "lucene-queryparser"        % luceneV
val lucene_expressions        = "org.apache.lucene"              % "lucene-expressions"        % luceneV
val lucene_spatial            = "org.apache.lucene"              % "lucene-spatial"            % luceneV
val lucene_spatial_extras     = "org.apache.lucene"              % "lucene-spatial-extras"     % luceneV

val jts                       = "com.vividsolutions"             % "jts"                       % "1.13"
// scalastyle:on


libraryDependencies ++= Seq(
  algebird,
  lucene_facet,
  lucene_analyzers,
  lucene_expressions,
  lucene_query_parsers,
  typesafe_config,
  lucene_spatial,
  lucene_spatial_extras,
  spatial4j,
  jts,
  joda_time,
  joda_convert, // To avoid warning: Class org.joda.convert.ToString not found
  scalactic,  // scalactic is recommended, see http://www.scalatest.org/install
  scalatest
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" force(),
  "com.holdenkarau"  %% "spark-testing-base" % s"${sparkVersion.value}_0.7.4" % "test" intransitive(),
  "org.scala-lang"    % "scala-library" % scalaVersion.value % "compile"
)

// Read version in code from build.sbt
lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    // See https://github.com/sbt/sbt-buildinfo#buildinfooptionbuildtime
    buildInfoOptions += BuildInfoOption.BuildTime,
    // https://github.com/sbt/sbt-buildinfo#buildinfooptiontomap
    buildInfoOptions += BuildInfoOption.ToMap,
    buildInfoPackage := "org.zouzias.spark.lucenerdd"
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value
(compile in Compile) <<= (compile in Compile) dependsOn compileScalastyle


parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}
