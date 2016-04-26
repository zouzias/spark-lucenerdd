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
import org.scalatest.{FlatSpec, Matchers}
import org.zouzias.spark.rdd.lucenerdd.implicits.LuceneRDDImplicits._
import org.zouzias.spark.rdd.lucenerdd.model.LuceneText

class LuceneRDDSearchSpec extends FlatSpec with Matchers with SharedSparkContext {

  val First = "_1"

  def randomString(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString
  val array = (1 to 24).map(randomString(_))

  "LuceneRDD" should "return correct number of elements" in {
    val rdd = sc.parallelize(array)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "correctly search with TermQueries" in {
    val rdd = sc.parallelize(array)
    val luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.termQuery(First,
      array(scala.util.Random.nextInt(array.size)))
    results.size should be (1)
  }

  "LuceneRDD" should "correctly search with PrefixQueries" in {

    val prefices = Array("aaaabcd", "aaadcb", "aaz", "az", "qwerty")
    val rdd = sc.parallelize(prefices)
    val luceneRDD = LuceneRDD(rdd)

    luceneRDD.prefixQuery(First, "a").size should be (4)
    luceneRDD.prefixQuery(First, "aa").size should be (3)
    luceneRDD.prefixQuery(First, "aaa").size should be (2)
    luceneRDD.prefixQuery(First, "aaaa").size should be (1)
  }

  "LuceneRDD" should "correctly search with FuzzyQuery" in {
    val prefices = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(prefices)
    val luceneRDD = LuceneRDD(rdd)

    luceneRDD.fuzzyQuery(First, "aaaaa", 1).size should be (4)
    luceneRDD.fuzzyQuery(First, "qwert", 1).size should be (1)
    luceneRDD.fuzzyQuery(First, "werty", 1).size should be (1)
  }

  "LuceneRDD" should "correctly search with PhraseQuery" in {
    val phrases = Array("hello world", "how are you", "my name is Tassos").map(LuceneText(_))
    val rdd = sc.parallelize(phrases)
    val luceneRDD = LuceneRDD(rdd)

    luceneRDD.phraseQuery(First, "how are", 10).size should be (1)
    luceneRDD.phraseQuery(First, "name is", 10).size should be (1)
    luceneRDD.phraseQuery(First, "hello world", 10).size should be (1)
    luceneRDD.phraseQuery(First, "are", 10).size should be (1)
    luceneRDD.phraseQuery(First, "not", 10).size should be (0)
  }

}