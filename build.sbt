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
val luceneV = "5.5.0"

// scalastyle:off
val spark_core                = "org.apache.spark"               %% "spark-core"               % sparkVersion
val spark_sql                 = "org.apache.spark"               %% "spark-sql"                % sparkVersion

val specs2_core               = "org.specs2"                     %% "specs2-core"             % "2.3.11" % "test"
val scala_check               = "org.scalacheck"                 %% "scalacheck"              % "1.12.2" % "test"

val scalatest                 = "org.scalatest"                  %% "scalatest"                % "2.2.6" % "test"
val spark_testing_base        = "com.holdenkarau"                %% "spark-testing-base"       % s"${sparkVersion}_0.3.1" % "test" intransitive()

val algebird                  = "com.twitter"                    %% "algebird-core"            % "0.12.0"

val typesafe_config           = "com.typesafe"                   % "config"                    % "1.2.1"

val lucene_facet              = "org.apache.lucene"              % "lucene-facet"              % luceneV
val lucene_analyzers          = "org.apache.lucene"              % "lucene-analyzers-common"   % luceneV
val lucene_expressions        = "org.apache.lucene"              % "lucene-expressions"        % luceneV
// scalastyle:on


libraryDependencies ++= Seq(
  spark_core % "provided",
  spark_sql % "provided",
  algebird,
  lucene_facet,
  lucene_analyzers,
  lucene_expressions,
  specs2_core,
  scalatest,
  spark_testing_base
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value
(compile in Compile) <<= (compile in Compile) dependsOn compileScalastyle


parallelExecution in Test := false

