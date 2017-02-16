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
package org.zouzias.spark.lucenerdd.spatial.shape.implicits

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.spatial.shape._
import org.zouzias.spark.lucenerdd.testing.LuceneRDDTestUtils
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.spatial.shape.context.ContextLoader
import org.zouzias.spark.lucenerdd.spatial.shape.rdds.ShapeLuceneRDD

class ShapeLuceneRDDImplicitsSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext
  with ContextLoader
  with LuceneRDDTestUtils {

  val Radius: Double = 5D

  override val conf = ShapeLuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  "ShapeLuceneRDDImplicits" should "implicitly convert to point" in {

    val rdd = sc.parallelize(cities)
    val shapeRDD = ShapeLuceneRDD(rdd)

    shapeRDD.count should equal(cities.length)
  }

  "ShapeLuceneRDDImplicits" should "implicitly convert to circle" in {

    val circleCities: Array[(((Double, Double), Double), String)]
    = cities.map(convertToCircle)
    val rdd = sc.parallelize(circleCities)
    val shapeRDD = ShapeLuceneRDD(rdd)

    shapeRDD.count should equal(circleCities.length)
  }

  "ShapeLuceneRDDImplicits" should "implicitly convert to rectangle" in {

    val rectangleCities = cities.map(convertToRectangle)
    val rdd = sc.parallelize(rectangleCities)
    val shapeRDD = ShapeLuceneRDD(rdd)

    shapeRDD.count should equal(rectangleCities.length)
  }

  "ShapeLuceneRDDImplicits" should "implicitly convert POINTS from WKT" in {
    val sparkSession = SparkSession.builder().getOrCreate()
    val citiesDF = sparkSession.read.parquet("data/world-cities-points.parquet")
    import sparkSession.implicits._
    val citiesRDD = citiesDF.map(row =>
      (row.getString(2), (row.getString(0), row.getString(1))))

    val total = citiesDF.count()
    total > 0 should equal(true)

    val shapeRDD = ShapeLuceneRDD(citiesRDD)

    shapeRDD.count > 0 should equal(true)
  }

  "ShapeLuceneRDDImplicits" should "implicitly convert BBOX from WKT" in {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val countriesDF = sparkSession.read.parquet("data/countries-bbox.parquet")
    val citiesRDD = countriesDF.map(row =>
      (row.getString(2), (row.getString(0), row.getString(1))))

    val total = countriesDF.count()
    total > 0 should equal(true)

    val shapeRDD = ShapeLuceneRDD(citiesRDD)

    shapeRDD.count > 0 should equal(true)
  }

  "ShapeLuceneRDDImplicits" should "implicitly convert to polygon" in {

    val polygonCities = cities.map(convertToPolygon(_, Radius))
    val rdd = sc.parallelize(polygonCities)
    val shapeRDD = ShapeLuceneRDD(rdd)

    shapeRDD.count should equal(polygonCities.length)
  }

}
