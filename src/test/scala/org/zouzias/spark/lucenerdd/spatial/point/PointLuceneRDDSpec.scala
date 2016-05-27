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
package org.zouzias.spark.lucenerdd.spatial.point

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.implicits.LuceneRDDImplicits._
import org.zouzias.spark.lucenerdd.models.SparkDoc
import org.zouzias.spark.lucenerdd.spatial.core.SpatialLuceneRDD
import org.zouzias.spark.lucenerdd.spatial.implicits.PointLuceneRDDImplicits._

class PointLuceneRDDSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  val Bern = ( (7.45, 46.95), "Bern")
  val Zurich = ( (8.55, 47.366667), "Zurich")
  val Laussanne = ( (6.6335, 46.519833), "Laussanne")
  val Athens = ((23.716667, 37.966667), "Athens")
  val Toronto = ((-79.4, 43.7), "Toronto")
  val k = 5

  var pointLuceneRDD: SpatialLuceneRDD[_, _] = _

  override def afterEach() {
    pointLuceneRDD.close()
  }

  // Check if sequence is sorted in descending order
  def sortedDesc(seq : Seq[Float]) : Boolean = {
    if (seq.isEmpty) true else seq.zip(seq.tail).forall(x => x._1 >= x._2)
  }

  "PointLuceneRDD.knn" should "return k-nearest neighbors (knn)" in {

    val cities = Array(Bern, Zurich, Laussanne, Athens, Toronto)
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = PointLuceneRDD(rdd)

    val results = pointLuceneRDD.knnSearch(Bern._1, k)

    // Closest is Bern and fartherst is Toronto
    docTextFieldEq(results.head.doc, "_1", Bern._2) should equal(true)
    docTextFieldEq(results.last.doc, "_1", Toronto._2) should equal(true)

    // Distances must be sorted
    val revertedDists = results.map(_.score).toList.reverse
    sortedDesc(revertedDists)
  }

  def docTextFieldEq(doc: SparkDoc, fieldName: String, fieldValue: String): Boolean = {
    doc.textField(fieldName).forall(_.contains(fieldValue))
  }

  "PointLuceneRDD.radiusSearch" should "return radius search" in {
    val cities = Array(Bern, Zurich, Laussanne, Athens, Toronto)
    val rdd = sc.parallelize(cities)
    pointLuceneRDD = PointLuceneRDD(rdd)

    // Bern, Laussanne and Zurich is within 300km
    val results = pointLuceneRDD.circleSearch(Bern._1, 300, k)

    results.exists(x => docTextFieldEq(x.doc, "_1", Bern._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Zurich._2)) should equal(true)
    results.exists(x => docTextFieldEq(x.doc, "_1", Laussanne._2)) should equal(true)

    results.exists(x => docTextFieldEq(x.doc, "_1", Athens._2)) should equal(false)
    results.exists(x => docTextFieldEq(x.doc, "_1", Toronto._2)) should equal(false)
  }
}
