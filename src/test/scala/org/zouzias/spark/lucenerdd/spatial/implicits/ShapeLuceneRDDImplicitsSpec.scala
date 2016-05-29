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
package org.zouzias.spark.lucenerdd.spatial.implicits

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.spatial.ContextLoader
import org.zouzias.spark.lucenerdd.implicits.LuceneRDDImplicits._
import org.zouzias.spark.lucenerdd.spatial.implicits.ShapeLuceneRDDImplicits._
import org.zouzias.spark.lucenerdd.spatial.shape.ShapeLuceneRDD


class ShapeLuceneRDDImplicitsSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext
  with ContextLoader {


  val Bern = ((7.45, 46.95), "Bern")
  val Zurich = ( (8.55, 47.366667), "Zurich")
  val Laussanne = ( (6.6335, 46.519833), "Laussanne")
  val Athens = ((23.716667, 37.966667), "Athens")
  val Toronto = ((-79.4, 43.7), "Toronto")

  val Radius = 5

  def convertToCircle(city: ((Double, Double), String)): (((Double, Double), Double), String) = {
    ((city._1, Radius), city._2)
  }

  def convertToRectangle(city: ((Double, Double), String))
  : ((Double, Double, Double, Double), String) = {
    val x = city._1._1
    val y = city._1._2

    ((x - Radius, x + Radius, y - Radius, y + Radius), city._2)
  }


  "ShapeLuceneRDDImplicits" should "implicitly convert to point" in {

    val cities = Array(Bern, Zurich, Laussanne, Athens, Toronto)
    val rdd = sc.parallelize(cities)
    val shapeRDD = ShapeLuceneRDD(rdd)

    shapeRDD.count should equal(cities.length)
  }

  "ShapeLuceneRDDImplicits" should "implicitly convert to circle" in {

    val cities: Array[(((Double, Double), Double), String)]
    = Array(Bern, Zurich, Laussanne, Athens, Toronto).map(convertToCircle)
    val rdd = sc.parallelize(cities)
    val shapeRDD = ShapeLuceneRDD(rdd)

    shapeRDD.count should equal(cities.length)
  }

  "ShapeLuceneRDDImplicits" should "implicitly convert to rectangle" in {

    val cities = Array(Bern, Zurich, Laussanne, Athens, Toronto).map(convertToRectangle)
    val rdd = sc.parallelize(cities)
    val shapeRDD = ShapeLuceneRDD(rdd)

    shapeRDD.count should equal(cities.length)
  }

}
