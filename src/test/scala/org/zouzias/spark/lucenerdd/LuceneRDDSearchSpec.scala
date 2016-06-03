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
import org.apache.lucene.index.Term
import org.apache.lucene.search.{PrefixQuery, Query}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.implicits.LuceneRDDImplicits._
import org.zouzias.spark.lucenerdd.models.LuceneText

import scala.io.Source

class LuceneRDDSearchSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  var luceneRDD: LuceneRDD[_] = _

  override def afterEach() {
    luceneRDD.close()
  }

  val First = "_1"

  def randomString(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString
  val array = (1 to 24).map(randomString(_))

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
    val prefices = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(prefices)
    luceneRDD = LuceneRDD(rdd)

    luceneRDD.fuzzyQuery(First, "aaaaa", 1).size should equal (4)
    luceneRDD.fuzzyQuery(First, "qwert", 1).size should equal(1)
    luceneRDD.fuzzyQuery(First, "werty", 1).size should equal (1)
  }

  "LuceneRDD.link" should "correctly link with query parser (prefix)" in {
    val leftCountries = Array("gree", "germa", "spa", "ita")
    implicit val mySC = sc
    val leftCountriesRDD = sc.parallelize(leftCountries)

    val countries = sc.parallelize(Source.fromFile("src/test/resources/countries.txt").getLines()
      .map(_.toLowerCase()).toSeq)

    luceneRDD = LuceneRDD(countries)

    def linker(country: String): String = {
      s"_1:${country}*"
    }

    val linked = luceneRDD.link(leftCountriesRDD, linker, 10)
    linked.count() should equal(leftCountries.size)
    // Greece and Greenland should appear
    linked.filter(link => link._1 == "gree" && link._2.length == 2)
    // Italy should appear
    linked.filter(link => link._1 == "ita" && link._2.length == 1)
  }

  "LuceneRDD.linkByQuery" should "correctly link with prefix query" in {
    val leftCountries = Array("gree", "germa", "spa", "ita")
    implicit val mySC = sc
    val leftCountriesRDD = sc.parallelize(leftCountries)

    val countries = sc.parallelize(Source.fromFile("src/test/resources/countries.txt").getLines()
      .map(_.toLowerCase()).toSeq)

    luceneRDD = LuceneRDD(countries)

    def linker(country: String): Query = {
      val term = new Term("_1", country)
      new PrefixQuery(term)
    }

    val linked = luceneRDD.linkByQuery(leftCountriesRDD, linker, 10)
    linked.count() should equal(leftCountries.size)
    // Greece and Greenland should appear
    linked.filter(link => link._1 == "gree" && link._2.length == 2)
    // Italy should appear
    linked.filter(link => link._1 == "ita" && link._2.length == 1)
  }


  "LuceneRDD.phraseQuery" should "correctly search with PhraseQuery" in {
    val phrases = Array("hello world", "how are you", "my name is Tassos").map(LuceneText(_))
    val rdd = sc.parallelize(phrases)
    luceneRDD = LuceneRDD(rdd)

    luceneRDD.phraseQuery(First, "how are", 10).size should equal (1)
    luceneRDD.phraseQuery(First, "name is", 10).size should equal (1)
    luceneRDD.phraseQuery(First, "hello world", 10).size should equal (1)
    luceneRDD.phraseQuery(First, "are", 10).size should equal(1)
    luceneRDD.phraseQuery(First, "not", 10).size should equal (0)
  }
}