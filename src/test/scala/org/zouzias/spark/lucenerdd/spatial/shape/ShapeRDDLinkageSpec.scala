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
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.spatial.shape.context.ContextLoader
import org.zouzias.spark.lucenerdd.spatial.shape.rdds.{ShapeRDD, ShapeRDDKryoRegistrator}
import org.zouzias.spark.lucenerdd.testing.LuceneRDDTestUtils

class ShapeRDDLinkageSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext
  with ContextLoader
  with LuceneRDDTestUtils {

  val k = 6

  val Radius: Double = 5D

  var pointRDD: ShapeRDD[_, _] = _

  override val conf = ShapeRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  override def afterEach() {
    pointRDD.close()
  }

  "ShapeRDD.linkByKnn" should "link correctly k-nearest neighbors (knn)" in {

    val citiesRDD = sc.parallelize(cities)
    pointRDD = ShapeRDD(citiesRDD)
    pointRDD.cache()

    val linker = (x: ((Double, Double), String)) => x._1

    val linkage = pointRDD.linkByKnn(citiesRDD, linker, k)

    linkage.count() should equal(cities.length)

    linkage.collect().foreach{ case (city, knnResults) =>

      // Must return only at most k results
      knnResults.length should be <= k

      // Distances must be sorted
      val revertedDists = knnResults.map(_.score).reverse
      sortedDesc(revertedDists) should equal(true)
    }
  }

  "ShapeRDD.linkByRadius" should "link correctly countries with capitals" in {

    val Radius = 50.0
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._
    val countriesRDD: Dataset[(String, String)] = sparkSession.read
      .parquet("data/countries-poly.parquet")
      .select("name", "shape")
      .map(row => (row.getString(1), row.getString(0)))

    pointRDD = ShapeRDD(countriesRDD)
    pointRDD.cache()

    val capitals = sparkSession.read.parquet("data/capitals.parquet")
      .select("name", "shape")
      .map(row => (row.getString(1), row.getString(0)))

    /**
     * Convert WKT Point to (Double, Double)
     * @param city
     * @return
     */
    def coords(city: (String, String)): (Double, Double) = {
      val str = city._1
      val nums = str.dropWhile(x => x.compareTo('(') != 0).drop(1).dropRight(1)
      val coords = nums.split(" ").map(_.trim)
      (coords(0).toDouble, coords(1).toDouble)
    }

    val linkage = pointRDD.postLinker(pointRDD.linkByRadius(capitals.rdd, coords, Radius))
      .collect()


    linkage.exists{ case ((_, capital), (_, country)) =>
      capital.compareToIgnoreCase("dublin") == 0 && country
        .toString.compareToIgnoreCase("Ireland") == 0} should equal(true)

    linkage.exists{ case ((_, capital), (_, country)) =>
      capital.compareToIgnoreCase("madrid") == 0 && country
        .toString.compareToIgnoreCase("spain") == 0} should equal(true)
  }

  "ShapeRDD.linkDataFrameByKnn" should "link correctly k-nearest neighbors (knn)" in {

    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._
    val citiesRDD = sc.parallelize(cities)
    pointRDD = ShapeRDD(citiesRDD)
    pointRDD.cache()

    val citiesDF = citiesRDD.map(x => City(x._2, x._1._1, x._1._2)).toDF
    val linker = (x: Row) => (x.getDouble(1), x.getDouble(2))

    val linkage = pointRDD.linkDataFrameByKnn(citiesDF, linker, k)

    linkage.count() should equal(cities.length)

    linkage.collect().foreach { case (city, knnResults) =>

      // Must return only at most k results
      knnResults.length should be <= k

      // Distances must be sorted
      val revertedDists = knnResults.map(_.score).reverse
      sortedDesc(revertedDists) should equal(true)
    }

  }
}
