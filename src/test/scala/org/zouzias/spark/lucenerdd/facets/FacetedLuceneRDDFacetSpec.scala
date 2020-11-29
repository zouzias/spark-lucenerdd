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
import org.zouzias.spark.lucenerdd.{LuceneRDD, LuceneRDDKryoRegistrator}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import matchers.should._


class FacetedLuceneRDDFacetSpec extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  override val conf = LuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  // Check if sequence is sorted in descending order
  def sortedDesc(seq : Seq[Long]) : Boolean = {
    if (seq.isEmpty) true else seq.zip(seq.tail).forall(x => x._1 >= x._2)
  }

  "FacetedLuceneRDD.facetQuery" should "compute facets correctly" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = FacetedLuceneRDD(rdd)
    val facetResults = luceneRDD.facetQuery("*:*", "_1")._2

    facetResults.facets.size should equal (3)
    facetResults.facets.contains("aaa") should equal (true)
    facetResults.facets.get("aaa")
      .foreach(value => value should equal (4))

    luceneRDD.close()
  }

  "FacetedLuceneRDD.facetQuery" should "compute facets correctly with ints" in {
    val words = Array(10, 10, 10, 10, 22, 22, 22, 33, 33)
    val rdd = sc.parallelize(words)
    val luceneRDD = FacetedLuceneRDD(rdd)
    val facetResults = luceneRDD.facetQuery("*:*", "_1")._2

    facetResults.facets.size should equal (3)
    facetResults.facets.contains("10") should equal (true)
    facetResults.facets.contains("22") should equal (true)
    facetResults.facets.contains("33") should equal (true)
    facetResults.facets.get("10").foreach(value => value should equal (4))
    facetResults.facets.get("33").foreach(value => value should equal (2))

    luceneRDD.close()
  }

  "FacetedLuceneRDD.facetQuery" should "compute facets correctly with doubles" in {
    val words = Array(10.5D, 10.5D, 10.5D, 10.5D, 22.2D, 22.2D, 22.2D, 33.2D, 33.2D)
    val rdd = sc.parallelize(words)
    val luceneRDD = FacetedLuceneRDD(rdd)
    val facetResults = luceneRDD.facetQuery("*:*", "_1")._2

    facetResults.facets.size should equal (3)
    facetResults.facets.contains("10.5") should equal (true)
    facetResults.facets.contains("22.2") should equal (true)
    facetResults.facets.contains("33.2") should equal (true)
    facetResults.facets.get("10.5").foreach(value => value should equal (4))
    facetResults.facets.get("33.2").foreach(value => value should equal (2))

    luceneRDD.close()
  }

  "FacetedLuceneRDD.facetQueries" should "compute facets correctly" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = FacetedLuceneRDD(rdd)

    val facetResults = luceneRDD.facetQueries("*:*", Seq("_1"))._2

    facetResults.contains("_1") should equal(true)
    facetResults.foreach(_._2.facets.size should equal (3))
    facetResults.foreach(_._2.facets.contains("aaa") should equal (true))
    facetResults.foreach(_._2.facets.get("aaa").foreach(value => value should equal (4)))

    luceneRDD.close()
  }

  "FacetedLuceneRDD.sortedFacets" should "return facets sorted by decreasing order" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = FacetedLuceneRDD(rdd)

    val sortedFacetCounts = luceneRDD.facetQuery("*:*", "_1")._2.sortedFacets().map(_._2)
    sortedDesc(sortedFacetCounts) should equal(true)

    luceneRDD.close()
  }

  "FacetedLuceneRDD.facetQuery" should "compute facets with prefix search" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = FacetedLuceneRDD(rdd)
    val results = luceneRDD.facetQuery("_1:aa*", "_1")
    val facetResults = results._2

    facetResults.facets.size should equal (1)
    facetResults.facets.contains("aaa") should equal (true)
    facetResults.facets.get("aaa")
      .foreach(value => value should equal (4))

    luceneRDD.close()
  }

  "FacetedLuceneRDD.facetQuery" should "compute facets with term search" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "aaaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = FacetedLuceneRDD(rdd)
    val results = luceneRDD.facetQuery("_1:aaa", "_1")
    val facetResults = results._2

    facetResults.facets.size should equal (1)
    facetResults.facets.contains("aaa") should equal (true)
    facetResults.facets.contains("bb") should equal (false)
    facetResults.facets.contains("cc") should equal (false)
    facetResults.facets.get("aaa") should equal (Some(4))

    val resultsB = luceneRDD.facetQuery("_1:bb", "_1")
    val facetResultsB = resultsB._2

    facetResultsB.facets.contains("bb") should equal (true)
    facetResultsB.facets.get("bb") should equal (Some(3))

    luceneRDD.close()
  }

  "FacetedLuceneRDD.facetQuery" should "compute facets with term search in Tuple2" in {
    val words = Array(("aaa", "aaa1"), ("aaa", "aaa2"), ("aaa", "aaa3"), ("aaa", "aaa3"),
      ("aaaa", "aaa3"), ("bb", "cc1"), ("bb", "cc1"), ("bb", "cc1"), ("cc", "cc2"), ("cc", "cc2"))
    val rdd = sc.parallelize(words)
    val luceneRDD = FacetedLuceneRDD(rdd)
    val results = luceneRDD.facetQuery("_1:aaa", "_2")
    val facetResults = results._2

    facetResults.facets.size should equal (3)
    facetResults.facets.contains("aaa1") should equal (true)
    facetResults.facets.contains("aaa2") should equal (true)
    facetResults.facets.contains("aaa3") should equal (true)
    facetResults.facets.get("aaa1") should equal (Some(1))
    facetResults.facets.get("aaa2") should equal (Some(1))
    facetResults.facets.get("aaa3") should equal (Some(2))

    luceneRDD.close()
  }

  "FacetedLuceneRDD.version" should "return project sbt build information" in {
    val map = LuceneRDD.version()
    map.contains("name") should equal(true)
    map.contains("builtAtMillis") should equal(true)
    map.contains("scalaVersion") should equal(true)
    map.contains("version") should equal(true)
    map.contains("sbtVersion") should equal(true)
    map.contains("builtAtString") should equal(true)
  }

}
