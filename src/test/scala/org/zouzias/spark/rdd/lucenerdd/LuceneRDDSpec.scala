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
package org.zouzias.spark.rdd.lucenerdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{ FlatSpec, Matchers }
import org.zouzias.spark.rdd.lucenerdd.impl.RamLuceneRDDPartition


class LuceneRDDSpec extends FlatSpec with Matchers with SharedSparkContext {

  def randomString(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString
  val array = (1 to 24).map(randomString(_))

  "LuceneRDD" should "return correct number of elements" in {
    val rdd = sc.parallelize(array)
    val luceneRDD = LuceneRDD(rdd, RamLuceneRDDPartition.stringConversion)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "correctly search with TermQueries" in {
    val rdd = sc.parallelize(array)
    val StringConv = RamLuceneRDDPartition.stringConversion
    val luceneRDD = LuceneRDD(rdd, StringConv)
    val results = luceneRDD.termQuery(StringConv.defaultField,
      array(scala.util.Random.nextInt(array.size)))
    results.size should be (1)
  }

  "LuceneRDD" should "correctly search with PrefixQueries" in {

    val prefices = Array("aaaabcd", "aaadcb", "aaz", "az", "qwerty")
    val rdd = sc.parallelize(prefices)
    val StringConv = RamLuceneRDDPartition.stringConversion
    val luceneRDD = LuceneRDD(rdd, StringConv)

    luceneRDD.prefixQuery(StringConv.defaultField, "a").size should be (4)
    luceneRDD.prefixQuery(StringConv.defaultField, "aa").size should be (3)
    luceneRDD.prefixQuery(StringConv.defaultField, "aaa").size should be (2)
    luceneRDD.prefixQuery(StringConv.defaultField, "aaaa").size should be (1)
  }

  "LuceneRDD" should "correctly search with FuzzyQuery" in {

    val prefices = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(prefices)
    val StringConv = RamLuceneRDDPartition.stringConversion
    val luceneRDD = LuceneRDD(rdd, StringConv)

    luceneRDD.fuzzyQuery(StringConv.defaultField, "aaaaa", 1).size should be (4)
    luceneRDD.fuzzyQuery(StringConv.defaultField, "qwert", 1).size should be (1)
    luceneRDD.fuzzyQuery(StringConv.defaultField, "werty", 1).size should be (1)
  }

  /*
  "LuceneRDD" should "correctly compute faceted counts" in {

    val docs = Array("tassos", "tassos", "costas", "maria")
    val rdd = sc.parallelize(docs)
    val StringConv = RamLuceneRDDPartition.stringConversion
    val luceneRDD = LuceneRDD(rdd, StringConv)

    luceneRDD.facetedQuery(StringConv.defaultField, 10).size should be (3)
  } */

}