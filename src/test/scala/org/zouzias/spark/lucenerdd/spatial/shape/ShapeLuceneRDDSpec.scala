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

import java.io.{Serializable, StringWriter}

import com.holdenkarau.spark.testing.SharedSparkContext
import com.spatial4j.core.distance.DistanceUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.models.SparkDoc
import org.zouzias.spark.lucenerdd.spatial.ContextLoader
import org.zouzias.spark.lucenerdd.testing.LuceneRDDTestUtils
import org.zouzias.spark.lucenerdd._

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

  // Check if sequence is sorted in descending order
  def sortedDesc(seq : Seq[Float]) : Boolean = {
    if (seq.isEmpty) true else seq.zip(seq.tail).forall(x => x._1 >= x._2)
  }

  "ShapeLuceneRDD.knnSearch" should "return k-nearest neighbors (knn)" in {

    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val results = pointLuceneRDD.knnSearch(Bern._1, k)

    results.size should equal(k)

    // Closest is Bern and fartherst is Toronto
    docTextFieldEq(results.head.doc, "_1", Bern._2) should equal(true)
    docTextFieldEq(results.last.doc, "_1", Toronto._2) should equal(true)

    // Distances must be sorted
    val revertedDists = results.map(_.score).toList.reverse
    sortedDesc(revertedDists) should equal(true)
  }

  "ShapeLuceneRDD.linkByKnn" should "link correctly k-nearest neighbors (knn)" in {

    val citiesRDD = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(citiesRDD)

    val linker = (x: ((Double, Double), String)) => x._1

    val linkage = pointLuceneRDD.linkByKnn(citiesRDD, linker, k)

    linkage.count() should equal(cities.size)

    linkage.collect().foreach{ case (city, knnResults) =>

      // top result should be query result
      city._2 should equal(knnResults.head.doc.textField("_1").get.head)

      // Return only k results
      knnResults.length should be <=(k)

      // Distances must be sorted
      val revertedDists = knnResults.map(_.score).reverse
      sortedDesc(revertedDists) should equal(true)
    }
  }

  private def docTextFieldEq(doc: SparkDoc, fieldName: String, fieldValue: String): Boolean = {
    doc.textField(fieldName).forall(_.contains(fieldValue))
  }

  "ShapeLuceneRDD.circleSearch" should "return correct results" in {
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    // Bern, Laussanne and Zurich is within 300km
    val results = pointLuceneRDD.circleSearch(Bern._1, 300, k)

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
    val results = pointLuceneRDD.spatialSearch(circleWKT, k)

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
    val results = pointLuceneRDD.bboxSearch((x, y), radius, k)

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

    results.size should equal(3)

    results.exists(x => docTextFieldEq(x.doc, "_1", Bern._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Zurich._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Laussanne._2)) should equal(true)

    results.exists(x => docTextFieldEq(x.doc, "_1", Milan._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Athens._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Toronto._2)) should equal(false)
  }

  "ShapeLuceneRDD.spatialSearch((Double, Double))" should "return a single point intersection" in {
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val point = ctx.makePoint(Bern._1._1, Bern._1._2)
    val results = pointLuceneRDD.spatialSearch( (Bern._1._1, Bern._1._2), k, "Intersects")

    results.size should equal(1)

    results.exists(x => docTextFieldEq(x.doc, "_1", Bern._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Zurich._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Laussanne._2)) should equal(false)

    results.exists(x => docTextFieldEq(x.doc, "_1", Milan._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Athens._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Toronto._2)) should equal(false)
  }



  "ShapeLuceneRDD.spatialSearch(Rectangle)" should "return correct results" in {
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val x = Bern._1._1
    val y = Bern._1._2
    val eps = DistanceUtils.dist2Degrees(150, DistanceUtils.EARTH_MEAN_RADIUS_KM)
    val rectangle = ctx.makeRectangle(x - eps, x + eps, y - eps, y + eps)

    val writer = new StringWriter()
    shapeWriter.write(writer, rectangle)
    val rectangleWKT = writer.getBuffer.toString

    // Bern, Laussanne and Zurich is within 300km
    val results = pointLuceneRDD.spatialSearch(rectangleWKT, k)

    results.size should equal(3)

    results.exists(x => docTextFieldEq(x.doc, "_1", Bern._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Zurich._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Laussanne._2)) should equal(true)

    results.exists(x => docTextFieldEq(x.doc, "_1", Milan._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Athens._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Toronto._2)) should equal(false)
  }

  "ShapeLuceneRDD.spatialSearch(Polygon)" should "return correct results" in {
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val x = Bern._1._1
    val y = Bern._1._2
    val eps = DistanceUtils.dist2Degrees(150, DistanceUtils.EARTH_MEAN_RADIUS_KM)

    val polygon = arrayPolygonToShape(convertToPolygon(Bern, eps)._1)

    val writer = new StringWriter()
    shapeWriter.write(writer, polygon)
    val polygonAsString = writer.getBuffer.toString

    // Bern, Laussanne and Zurich is within 300km
    val results = pointLuceneRDD.spatialSearch(polygonAsString, k)

    results.size should equal(3)

    results.exists(x => docTextFieldEq(x.doc, "_1", Bern._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Zurich._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Laussanne._2)) should equal(true)

    results.exists(x => docTextFieldEq(x.doc, "_1", Milan._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Athens._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Toronto._2)) should equal(false)
  }
}
