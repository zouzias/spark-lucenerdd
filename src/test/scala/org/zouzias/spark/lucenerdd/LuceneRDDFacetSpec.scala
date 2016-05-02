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


  "LuceneRDD.facetQuery" should "compute facets correctly" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.facetQuery("*:*", "_1_facet").facets.size should equal (3)
    luceneRDD.facetQuery("*:*", "_1_facet").facets.contains("aaa") should equal (true)
    luceneRDD.facetQuery("*:*", "_1_facet").facets.get("aaa")
      .foreach(value => value should equal (4))

    luceneRDD.close()
  }

  "LuceneRDD.facetQuery" should "compute facets with prefix search" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.facetQuery("_1:aa*", "_1_facet").facets.size should equal (1)
    luceneRDD.facetQuery("_1:aa*", "_1_facet").facets.contains("aaa") should equal (true)
    luceneRDD.facetQuery("_1:aa*", "_1_facet").facets.get("aaa")
      .foreach(value => value should equal (4))

    luceneRDD.close()
  }

  "LuceneRDD.facetQuery" should "compute facets with term search" in {
    val words = Array("aaa", "aaa", "aaa", "aaa", "aaaa", "bb", "bb", "bb", "cc", "cc")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.facetQuery("_1:aaa", "_1_facet").facets.size should equal (1)
    luceneRDD.facetQuery("_1:aaa", "_1_facet").facets.contains("aaa") should equal (true)
    luceneRDD.facetQuery("_1:aaa", "_1_facet").facets.get("aaa")
      .foreach(value => value should equal (4))

    luceneRDD.close()
  }

}
