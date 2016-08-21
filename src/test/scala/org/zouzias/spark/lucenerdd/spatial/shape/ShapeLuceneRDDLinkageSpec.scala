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
    pointLuceneRDD.cache()

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

  "ShapeLuceneRDD.linkByRadius" should "link correctly countries with capitals" in {

    val Radius = 50.0
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val countriesRDD = sqlContext.read.parquet("data/countries-poly.parquet")
      .select("name", "shape")
      .map(row => (row.getString(1), row.getString(0)))

    pointLuceneRDD = ShapeLuceneRDD(countriesRDD)
    pointLuceneRDD.cache()

    val capitals = sqlContext.read.parquet("data/capitals.parquet")
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

    val linkage = pointLuceneRDD.linkByRadius(capitals, coords, Radius).collect()

    linkage.size should equal(capitals.count)

    linkage.exists{case (cap, results) =>
      cap._2 == "Bern" && docTextFieldEq(results, "_1", "Switzerland")} should equal(true)
    linkage.exists{case (cap, results) =>
      cap._2 == "Berlin" && docTextFieldEq(results, "_1", "Germany")} should equal(true)
    linkage.exists{case (cap, results) =>
      cap._2 == "Ottawa" && docTextFieldEq(results, "_1", "Canada")} should equal(true)
    linkage.exists{case (cap, results) =>
      cap._2 == "Paris" && docTextFieldEq(results, "_1", "France")} should equal(true)

  }

  "ShapeLuceneRDD.linkDataFrameByRadius" should "link correctly countries with capitals" in {

    val Radius = 50.0
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val countriesRDD = sqlContext.read.parquet("data/countries-poly.parquet")
      .select("name", "shape")
      .map(row => (row.getString(1), row.getString(0)))

    pointLuceneRDD = ShapeLuceneRDD(countriesRDD)
    pointLuceneRDD.cache()

    val capitals = sqlContext.read.parquet("data/capitals.parquet").select("name", "shape")

    /**
     * Convert Row to (Double, Double)
     * @param row
     * @return
     */
    def rowToCoords(row: Row): (Double, Double) = {
      val str = row.getString(1)
      val nums = str.dropWhile(x => x.compareTo('(') != 0).drop(1).dropRight(1)
      val coords = nums.split(" ").map(_.trim)
      (coords(0).toDouble, coords(1).toDouble)
    }

    val linkage = pointLuceneRDD.linkDataFrameByRadius(capitals, rowToCoords, Radius).collect()

    linkage.size should equal(capitals.count)

    // scalastyle:off
    linkage.exists{case (cap, results) =>
      cap.getString(cap.fieldIndex("name")) == "Bern" && docTextFieldEq(results, "_1", "Switzerland")} should equal(true)
    linkage.exists{case (cap, results) =>
      cap.getString(cap.fieldIndex("name")) == "Berlin" && docTextFieldEq(results, "_1", "Germany")} should equal(true)
    linkage.exists{case (cap, results) =>
      cap.getString(cap.fieldIndex("name")) == "Ottawa" && docTextFieldEq(results, "_1", "Canada")} should equal(true)
    linkage.exists{case (cap, results) =>
      cap.getString(cap.fieldIndex("name")) == "Paris" && docTextFieldEq(results, "_1", "France")} should equal(true)
    // scalastyle: on
  }


  "ShapeLuceneRDD.linkDataFrameByKnn" should "link correctly k-nearest neighbors (knn)" in {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val citiesRDD = sc.parallelize(cities)
    pointLuceneRDD = ShapeLuceneRDD(citiesRDD)
    pointLuceneRDD.cache()

    val citiesDF = citiesRDD.map(x => City(x._2, x._1._1, x._1._2)).toDF
    val linker = (x: Row) => (x.getDouble(1), x.getDouble(2))

    val linkage = pointLuceneRDD.linkDataFrameByKnn(citiesDF, linker, k)

    linkage.count() should equal(cities.size)

    linkage.collect().foreach { case (city, knnResults) =>

      // top result should be linked with its query result
      docTextFieldEq(knnResults, "_1", city.getString(0)) should equal(true)

      // Must return only at most k results
      knnResults.length should be <= k

      // Distances must be sorted
      val revertedDists = knnResults.map(_.score).reverse
      sortedDesc(revertedDists) should equal(true)
    }

  }

}
