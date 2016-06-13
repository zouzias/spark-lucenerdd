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
scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")
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

javacOptions ++= Seq("-Xlint")

// Add jcenter repo
resolvers += Resolver.jcenterRepo

releaseCrossBuild := true
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

val luceneV = "5.5.1"


spName := "zouzias/spark-lucenerdd"
sparkVersion := "1.6.1"
spShortDescription := "Spark RDD with Lucene's query capabilities"
sparkComponents ++= Seq("core", "sql")
spAppendScalaVersion := true
// This is necessary because of how we explicitly specify Spark dependencies
// for tests rather than using the sbt-spark-package plugin to provide them.
spIgnoreProvided := true

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)


// scalastyle:off
val specs2_core               = "org.specs2"                     %% "specs2-core"             % "2.3.11" % "test"
val scala_check               = "org.scalacheck"                 %% "scalacheck"              % "1.12.2" % "test"
val scalatest                 = "org.scalatest"                  %% "scalatest"                % "2.2.6" % "test"

val algebird                  = "com.twitter"                    %% "algebird-core"            % "0.12.0"

val typesafe_config           = "com.typesafe"                   % "config"                    % "1.2.1"

val lucene_facet              = "org.apache.lucene"              % "lucene-facet"              % luceneV
val lucene_analyzers          = "org.apache.lucene"              % "lucene-analyzers-common"   % luceneV
val lucene_query_parsers      = "org.apache.lucene"              % "lucene-queryparser"        % luceneV
val lucene_expressions        = "org.apache.lucene"              % "lucene-expressions"        % luceneV
val lucene_spatial            = "org.apache.lucene"              % "lucene-spatial"            % luceneV

val jts                       = "com.vividsolutions"             % "jts"                       % "1.13"
// scalastyle:on


libraryDependencies ++= Seq(
  algebird,
  lucene_facet,
  lucene_analyzers,
  lucene_expressions,
  lucene_query_parsers,
  lucene_spatial,
  jts,
  specs2_core,
  scalatest
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" force(),
  "com.holdenkarau"  %% "spark-testing-base" % s"${testSparkVersion.value}_0.3.3" % "test" intransitive(),
  "org.scala-lang"    % "scala-library" % scalaVersion.value % "compile"
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value
(compile in Compile) <<= (compile in Compile) dependsOn compileScalastyle


parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}
