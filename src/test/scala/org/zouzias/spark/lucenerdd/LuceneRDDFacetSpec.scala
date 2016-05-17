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
package org.zouzias.spark.lucenerdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.implicits.LuceneRDDImplicits._

class LuceneRDDFacetSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  // Check if sequence is sorted in descending order
  def sortedDesc(seq : Seq[Long]) : Boolean = {
    if (seq.isEmpty) true else seq.zip(seq.tail).forall(x => x._1 >= x._2)
  }

  /*
  "LuceneRDD.facetQuery" should "compute facets correctly" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    val facetResults = luceneRDD.facetQuery("*:*", "_1")._2

    facetResults.facets.size should equal (3)
    facetResults.facets.contains("aaa") should equal (true)
    facetResults.facets.get("aaa")
      .foreach(value => value should equal (4))

    luceneRDD.close()
  }

  "LuceneRDD.facetQueries" should "compute facets correctly" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)

    val facetResults = luceneRDD.facetQueries("*:*", Seq("_1"))._2

    facetResults.contains("_1") should equal(true)
    facetResults.foreach(_._2.facets.size should equal (3))
    facetResults.foreach(_._2.facets.contains("aaa") should equal (true))
    facetResults.foreach(_._2.facets.get("aaa").foreach(value => value should equal (4)))

    luceneRDD.close()
  }

  "LuceneRDD.sortedFacets" should "return facets sorted by decreasing order" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)

    val sortedFacetCounts = luceneRDD.facetQuery("*:*", "_1")._2.sortedFacets().map(_._2)
    sortedDesc(sortedFacetCounts) should equal(true)

    luceneRDD.close()
  }

  "LuceneRDD.facetQuery" should "compute facets with prefix search" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.facetQuery("_1:aa*", "_1")
    val facetResults = results._2

    facetResults.facets.size should equal (1)
    facetResults.facets.contains("aaa") should equal (true)
    facetResults.facets.get("aaa")
      .foreach(value => value should equal (4))

    luceneRDD.close()
  }

  "LuceneRDD.facetQuery" should "compute facets with term search" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "aaaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
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

  "LuceneRDD.facetQuery" should "compute facets with term search in Tuple2" in {
    val words = Array(("aaa", "aaa1"), ("aaa", "aaa2"), ("aaa", "aaa3"), ("aaa", "aaa3"),
      ("aaaa", "aaa3"), ("bb", "cc1"), ("bb", "cc1"), ("bb", "cc1"), ("cc", "cc2"), ("cc", "cc2"))
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
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
  */
}
