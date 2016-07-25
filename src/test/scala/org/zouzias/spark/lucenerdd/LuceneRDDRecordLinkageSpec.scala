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
import org.apache.lucene.search.{FuzzyQuery, PrefixQuery}
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.io.Source

case class Country(name: String)

class LuceneRDDRecordLinkageSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  var luceneRDD: LuceneRDD[_] = _

  override def afterEach() {
    luceneRDD.close()
  }

  val First = "_1"


  "LuceneRDD.linkByQuery" should "correctly link with prefix query" in {
    val leftCountries = Array("gree", "germa", "spa", "ita")
    implicit val mySC = sc
    val leftCountriesRDD = sc.parallelize(leftCountries)

    val countries = sc.parallelize(Source.fromFile("src/test/resources/countries.txt").getLines()
      .map(_.toLowerCase()).toSeq)

    luceneRDD = LuceneRDD(countries)

    val linker = (country: String) => {
      val term = new Term("_1", country)
      new PrefixQuery(term)
    }

    val linked = luceneRDD.linkByQuery(leftCountriesRDD, linker, 10)
    linked.count() should equal(leftCountries.size)
    // Greece and Greenland should appear
    linked.collect().exists(link => link._1 == "gree" && link._2.length == 2) should equal(true)
    // Italy should appear
    linked.collect().exists(link => link._1 == "ita" && link._2.length == 1) should equal(true)
  }

  "LuceneRDD.linkByQuery" should "correctly link with query parser (fuzzy)" in {
    val leftCountries = Array("gree", "germa", "spa", "ita")
    implicit val mySC = sc
    val leftCountriesRDD = sc.parallelize(leftCountries)

    val countries = sc.parallelize(Source.fromFile("src/test/resources/countries.txt").getLines()
      .map(_.toLowerCase()).toSeq)

    luceneRDD = LuceneRDD(countries)

    val fuzzyLinker = (country: String) => {
      val Fuzziness = 2
      val term = new Term("_1", country)
      new FuzzyQuery(term, Fuzziness)
    }

    val linked = luceneRDD.linkByQuery(leftCountriesRDD, fuzzyLinker, 10)
    linked.count() should equal(leftCountries.size)
    // Greece should appear only
    linked.collect().exists(link => link._1 == "gree" && link._2.length == 1) should equal(true)
    // Italy, Iraq and Iran should appear
    linked.collect().exists(link => link._1 == "ita" && link._2.length >= 3) should equal (true)
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
    linked.collect().exists(link => link._1 == "gree" && link._2.length == 2) should equal(true)
    // Italy should appear
    linked.collect().exists(link => link._1 == "ita" && link._2.length == 1) should equal(true)
  }

  "LuceneRDD.linkDataFrame" should "correctly link with query parser (prefix)" in {
    val leftCountries = Array("gree", "germa", "spa", "ita")
    implicit val mySC = sc
    val leftCountriesRDD = sc.parallelize(leftCountries)

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val countriesDF = leftCountriesRDD.map(Country(_)).toDF()

    val countries = sc.parallelize(Source.fromFile("src/test/resources/countries.txt").getLines()
      .map(_.toLowerCase()).toSeq)

    luceneRDD = LuceneRDD(countries)

    def linker(row: Row): String = {
      val country = Option(row.get("name"))
      country match {
        case Some(c) => s"_1:${c}*"
        case None => s"_1:*"
      }
    }

    val linked = luceneRDD.linkDataFrame(countriesDF, linker, 10)
    linked.count() should equal(leftCountries.size)
    // Greece and Greenland should appear
    linked.collect().exists(link => link._1.get("name") == "gree"
      && link._2.length == 2) should equal(true)
    // Italy should appear
    linked.collect().exists(link => link._1.get("name") == "ita"
      && link._2.length == 1) should equal(true)
  }

  "LuceneRDD.link" should "correctly link with query parser (fuzzy)" in {
    val leftCountries = Array("gree", "germa", "spa", "ita")
    implicit val mySC = sc
    val leftCountriesRDD = sc.parallelize(leftCountries)

    val countries = sc.parallelize(Source.fromFile("src/test/resources/countries.txt").getLines()
      .map(_.toLowerCase()).toSeq)

    luceneRDD = LuceneRDD(countries)

    def fuzzyLinker(country: String): String = {
      val Fuzziness = 2
      s"_1:${country}~${Fuzziness}"
    }

    val linked = luceneRDD.link(leftCountriesRDD, fuzzyLinker, 10)
    linked.count() should equal(leftCountries.size)
    // Greece should appear only
    linked.collect.exists(link => link._1 == "gree" && link._2.length == 1)  should equal(true)
    // At least Italy, Iraq and Iran should appear
    linked.collect.exists(link => link._1 == "ita" && link._2.length >= 3) should equal(true)
  }
}
