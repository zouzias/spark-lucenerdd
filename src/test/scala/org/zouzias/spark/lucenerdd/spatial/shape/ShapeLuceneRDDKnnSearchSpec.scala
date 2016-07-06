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


import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.spatial.ContextLoader
import org.zouzias.spark.lucenerdd.testing.LuceneRDDTestUtils

class ShapeLuceneRDDKnnSearchSpec extends FlatSpec
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

  "ShapeLuceneRDD.knnSearch" should "return k-nearest neighbors (knn)" in {

    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val results = pointLuceneRDD.knnSearch(Bern._1, k, "*:*")

    results.size should equal(k)

    // Closest is Bern and fartherst is Toronto
    docTextFieldEq(results.head.doc, "_1", Bern._2) should equal(true)
    docTextFieldEq(results.last.doc, "_1", Toronto._2) should equal(true)

    // Distances must be sorted
    val revertedDists = results.map(_.score).toList.reverse
    sortedDesc(revertedDists) should equal(true)
  }

  "ShapeLuceneRDD.knnSearch" should "return k-nearest neighbors (prefix search)" in {

    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val results = pointLuceneRDD.knnSearch(Bern._1, k, "_1:mil*")

    results.size should be <= k

    // Closest is Bern and farthest is Toronto
    docTextFieldEq(results.head.doc, "_1", Milan._2) should equal(true)

    // Distances must be sorted
    val revertedDists = results.map(_.score).toList.reverse
    sortedDesc(revertedDists) should equal(true)
  }

  "ShapeLuceneRDD.knnSearch" should "return k-nearest neighbors (fuzzy search)" in {

    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val results = pointLuceneRDD.knnSearch(Bern._1, k, "_1:milan~1")

    results.size should be <= k

    // Closest is Bern and farthest is Toronto
    docTextFieldEq(results.head.doc, "_1", Milan._2) should equal(true)

    // Distances must be sorted
    val revertedDists = results.map(_.score).toList.reverse
    sortedDesc(revertedDists) should equal(true)
  }

  "ShapeLuceneRDD.knnSearch" should "return k-nearest neighbors (term query)" in {

    val rdd = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(rdd)

    val results = pointLuceneRDD.knnSearch(Bern._1, k, "_1:Milan")

    results.size should be <= k

    // Closest is Milan (due to filtering)
    docTextFieldEq(results.head.doc, "_1", Milan._2) should equal(true)

    // Distances must be sorted
    val revertedDists = results.map(_.score).toList.reverse
    sortedDesc(revertedDists) should equal(true)
  }
}
