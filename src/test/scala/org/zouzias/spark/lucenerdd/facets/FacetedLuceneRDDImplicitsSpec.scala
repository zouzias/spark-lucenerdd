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
package org.zouzias.spark.lucenerdd.facets

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.zouzias.spark.lucenerdd.testing.FavoriteCaseClass
import org.zouzias.spark.lucenerdd.{LuceneRDD, LuceneRDDKryoRegistrator}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import matchers.should._


class FacetedLuceneRDDImplicitsSpec  extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  var luceneRDD: LuceneRDD[_] = _


  override val conf = LuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  override def afterEach() {
    luceneRDD.close()
  }


  val elem = Array("fear", "death", "water", "fire", "house")
    .zipWithIndex.map{ case (str, index) =>
    FavoriteCaseClass(str, index, 10L, 12.3F, s"${str}@gmail.com")}


  "FacetedLuceneRDD(case class).count" should "return correct number of elements" in {
    val rdd = sc.parallelize(elem)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = FacetedLuceneRDD(df)
    luceneRDD.count should equal (elem.size)
  }

  "FacetedLuceneRDD(case class).fields" should "return all fields" in {
    val rdd = sc.parallelize(elem)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = FacetedLuceneRDD(df)

    luceneRDD.fields().size should equal(5)
    luceneRDD.fields().contains("name") should equal(true)
    luceneRDD.fields().contains("age") should equal(true)
    luceneRDD.fields().contains("myLong") should equal(true)
    luceneRDD.fields().contains("myFloat") should equal(true)
    luceneRDD.fields().contains("email") should equal(true)
  }

  "FacetedLuceneRDD(case class).termQuery" should "correctly search with TermQueries" in {
    val rdd = sc.parallelize(elem)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = FacetedLuceneRDD(df)

    val results = luceneRDD.termQuery("name", "water")
    results.count() should equal(1)
  }
}
