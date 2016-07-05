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

class LuceneRDDSearchSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  var luceneRDD: LuceneRDD[_] = _

  override def afterEach() {
    luceneRDD.close()
  }

  val First = "_1"

  val array = List("fear", "death", " apologies", "romance", "tree", "fashion", "fascism")

  "LuceneRDD.query" should "use phrase query syntax" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.query("_1:aadaa").nonEmpty should equal (true)
    luceneRDD.query("_1:aa*").size should equal (4)
    luceneRDD.query("_1:q*").size should equal (1)
  }

  "LuceneRDD.count" should "return correct number of elements" in {
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should equal (array.size)
  }

  "LuceneRDD.termQuery" should "correctly search with TermQueries" in {
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.termQuery(First,
      array(scala.util.Random.nextInt(array.size)))
    results.size should equal (1)
  }

  "LuceneRDD.prefixQuery" should "correctly search with PrefixQueries" in {

    val prefices = Array("aaaabcd", "aaadcb", "aaz", "az", "qwerty")
    val rdd = sc.parallelize(prefices)
    luceneRDD = LuceneRDD(rdd)

    luceneRDD.prefixQuery(First, "a").size should equal (4)
    luceneRDD.prefixQuery(First, "aa").size should equal(3)
    luceneRDD.prefixQuery(First, "aaa").size should equal (2)
    luceneRDD.prefixQuery(First, "aaaa").size should equal (1)
  }

  "LuceneRDD.fuzzyQuery" should "correctly search with FuzzyQuery" in {
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)

    luceneRDD.fuzzyQuery(First, "fear", 1).size should equal (1)
    luceneRDD.fuzzyQuery(First, "fascsm", 1).size should equal(1)
    luceneRDD.fuzzyQuery(First, "dath", 1).size should equal (1)
    luceneRDD.fuzzyQuery(First, "tree", 1).size should equal (1)
  }

  "LuceneRDD.phraseQuery" should "correctly search with PhraseQuery" in {
    val phrases = Array("hello world", "the company name was", "highlight lucene")
    val rdd = sc.parallelize(phrases)
    luceneRDD = LuceneRDD(rdd)

    luceneRDD.phraseQuery(First, "company name", 10).size should equal (1)
    luceneRDD.phraseQuery(First, "hello world", 10).size should equal (1)
    luceneRDD.phraseQuery(First, "highlight lucene", 10).size should equal(1)
  }
}