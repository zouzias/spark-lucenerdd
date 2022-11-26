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
scalaVersion := "2.12.5"
crossScalaVersions := Seq("2.12.5")
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

Test / publishArtifact := false

pomIncludeRepository := { _ => false }

pomExtra := <scm>
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

val luceneV = "9.4.2"
val sparkVersion = "3.3.1"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")


// scalastyle:off

val scalactic                 = "org.scalactic"                  %% "scalactic"                % "3.2.14"
val scalatest                 = "org.scalatest"                  %% "scalatest"                % "3.2.14" % "test"


val joda_time                 = "joda-time"                      % "joda-time"                 % "2.12.1"
val algebird                  = "com.twitter"                    %% "algebird-core"            % "0.13.9"
val joda_convert              = "org.joda"                       % "joda-convert"              % "2.2.2"
val spatial4j                 = "org.locationtech.spatial4j"     % "spatial4j"                 % "0.8"

val typesafe_config           = "com.typesafe"                   % "config"                    % "1.3.4"

val lucene_facet              = "org.apache.lucene"              % "lucene-facet"              % luceneV
val lucene_analyzers          = "org.apache.lucene"              % "lucene-analyzers-common"   % luceneV
val lucene_query_parsers      = "org.apache.lucene"              % "lucene-queryparser"        % luceneV
val lucene_expressions        = "org.apache.lucene"              % "lucene-expressions"        % luceneV
val lucene_spatial_extras     = "org.apache.lucene"              % "lucene-spatial-extras"     % luceneV

val jts                       = "org.locationtech.jts"           % "jts-core"                  % "1.18.2"
// scalastyle:on


libraryDependencies ++= Seq(
  algebird,
  lucene_facet,
  lucene_analyzers,
  lucene_expressions,
  lucene_query_parsers,
  typesafe_config,
  lucene_spatial_extras,
  spatial4j,
  jts,
  joda_time,
  joda_convert, // To avoid warning: Class org.joda.convert.ToString not found
  scalactic,  // scalactic is recommended, see http://www.scalatest.org/install
  scalatest
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "com.holdenkarau"  %% "spark-testing-base" % s"3.2.0_1.1.1" % "test" intransitive(),
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
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

Test / parallelExecution := false

// Skip tests during assembly
assembly / test := {}

// To avoid merge issues
assembly / assemblyMergeStrategy := {
    case PathList("module-info.class", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
