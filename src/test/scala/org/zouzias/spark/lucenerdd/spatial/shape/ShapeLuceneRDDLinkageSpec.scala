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
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.spatial.shape.context.ContextLoader
import org.zouzias.spark.lucenerdd.testing.LuceneRDDTestUtils

// Required for implicit Document conversion
import org.zouzias.spark.lucenerdd._

case class City(name: String, x: Double, y: Double)

class ShapeLuceneRDDLinkageSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext
  with ContextLoader
  with LuceneRDDTestUtils {

  val k = 6

  val Radius: Double = 5D

  var pointLuceneRDD: ShapeLuceneRDD[_, _] = _

  override val conf = ShapeLuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  override def afterEach() {
    pointLuceneRDD.close()
  }

  "ShapeLuceneRDD.linkByKnn" should "link correctly k-nearest neighbors (knn)" in {

    val citiesRDD = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(citiesRDD)

    val linker = (x: ((Double, Double), String)) => x._1

    val linkage = pointLuceneRDD.linkByKnn(citiesRDD, linker, k)

    linkage.count() should equal(cities.size)

    linkage.collect().foreach{ case (city, knnResults) =>

      // top result should be linked with its query result
      city._2 should equal(knnResults.head.doc.textField("_1").head)

      // Must return only at most k results
      knnResults.length should be <= k

      // Distances must be sorted
      val revertedDists = knnResults.map(_.score).reverse
      sortedDesc(revertedDists) should equal(true)
    }
  }

  "ShapeLuceneRDD.linkDataFrameByKnn" should "link correctly k-nearest neighbors (knn)" in {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val citiesRDD = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(citiesRDD)

    val citiesDF = citiesRDD.map(x => City(x._2, x._1._1, x._1._2)).toDF
    val linker = (x: Row) => (x.getDouble(1), x.getDouble(2))

    val linkage = pointLuceneRDD.linkDataFrameByKnn(citiesDF, linker, k)

    linkage.count() should equal(cities.size)

    linkage.collect().foreach { case (city, knnResults) =>

      // top result should be linked with its query result
      city.getString(0) should equal(knnResults.head.doc.textField("_1").head)

      // Must return only at most k results
      knnResults.length should be <= k

      // Distances must be sorted
      val revertedDists = knnResults.map(_.score).reverse
      sortedDesc(revertedDists) should equal(true)
    }

  }

}
