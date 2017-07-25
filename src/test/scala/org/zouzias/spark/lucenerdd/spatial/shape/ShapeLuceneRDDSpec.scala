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
package org.zouzias.spark.lucenerdd.spatial.shape

import java.io.StringWriter

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.locationtech.spatial4j.distance.DistanceUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.testing.LuceneRDDTestUtils
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.spatial.shape.context.ContextLoader

class ShapeLuceneRDDSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext
  with ContextLoader
  with LuceneRDDTestUtils {

  val k = 6

  val Radius: Double = 5D

  var pointLuceneRDD: ShapeLuceneRDD[_, _] = _

  override def afterEach() {
    pointLuceneRDD.close()
  }

  override val conf = ShapeLuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  "ShapeLuceneRDD.circleSearch" should "return correct results" in {
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    // Bern, Laussanne and Zurich is within 300km
    val results = pointLuceneRDD.circleSearch(Bern._1, 300, k).collect()

    results.size should equal(3)

    results.exists(x => docTextFieldEq(x.doc, "_1", Bern._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Zurich._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Laussanne._2)) should equal(true)

    results.exists(x => docTextFieldEq(x.doc, "_1", Milan._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Athens._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Toronto._2)) should equal(false)
  }

  "ShapeLuceneRDD.spatialSearch(circle)" should "return correct results" in {
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val point = ctx.makePoint(Bern._1._1, Bern._1._2)
    val circle = ctx.makeCircle(point,
      DistanceUtils.dist2Degrees(300, DistanceUtils.EARTH_MEAN_RADIUS_KM))

    val writer = new StringWriter()
    shapeWriter.write(writer, circle)
    val circleWKT = writer.getBuffer.toString

    // Bern, Laussanne and Zurich is within 300km
    val results = pointLuceneRDD.spatialSearch(circleWKT, k).collect()

    results.size should equal(3)

    results.exists(x => docTextFieldEq(x.doc, "_1", Bern._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Zurich._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Laussanne._2)) should equal(true)

    results.exists(x => docTextFieldEq(x.doc, "_1", Milan._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Athens._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Toronto._2)) should equal(false)
  }

  "ShapeLuceneRDD.bboxSearch((Double, Double), Double)" should "return correct results" in {
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val x = Bern._1._1
    val y = Bern._1._2
    val radius = 150.0

    // Bern, Laussanne and Zurich is within 300km
    val results = pointLuceneRDD.bboxSearch((x, y), radius, k).collect()

    results.size should equal(3)

    results.exists(x => docTextFieldEq(x.doc, "_1", Bern._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Zurich._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Laussanne._2)) should equal(true)

    results.exists(x => docTextFieldEq(x.doc, "_1", Milan._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Athens._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Toronto._2)) should equal(false)
  }

  "ShapeLuceneRDD.bboxSearch(lowerLeft, upperRight)" should "return correct results" in {
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val x = Bern._1._1
    val y = Bern._1._2
    val width = DistanceUtils.dist2Degrees(150, DistanceUtils.EARTH_MEAN_RADIUS_KM)

    // Bern, Laussanne and Zurich is within 300km
    val results = pointLuceneRDD.bboxSearch((x - width, y - width), (x + width, y + width), k, "Intersects")
      .collect()

    results.length should equal(3)

    results.exists(x => docTextFieldEq(x.doc, "_1", Bern._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Zurich._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Laussanne._2)) should equal(true)

    results.exists(x => docTextFieldEq(x.doc, "_1", Milan._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Athens._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Toronto._2)) should equal(false)
  }

  "ShapeLuceneRDD.apply(DF, shapeField)" should
    "instantiate ShapeLuceneRDD from DataFrame and shape field name" in {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._
    val capitals = sparkSession.read.parquet("data/capitals.parquet")
    pointLuceneRDD = ShapeLuceneRDD(capitals, "shape")

    pointLuceneRDD.count() > 0 should equal(true)
  }

  "ShapeLuceneRDD.version" should "return project sbt build information" in {
    val map = LuceneRDD.version()
    map.contains("name") should equal(true)
    map.contains("builtAtMillis") should equal(true)
    map.contains("scalaVersion") should equal(true)
    map.contains("version") should equal(true)
    map.contains("sbtVersion") should equal(true)
    map.contains("builtAtString") should equal(true)
  }
}
