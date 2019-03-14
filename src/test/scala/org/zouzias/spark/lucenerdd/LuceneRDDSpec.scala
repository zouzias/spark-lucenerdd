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
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class LuceneRDDSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  var luceneRDD: LuceneRDD[_] = _


  override val conf: SparkConf = LuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  override def afterEach() {
    luceneRDD.close()
  }

  "LuceneRDD.exists(Map)" should "find elements that exist" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.exists(Map("_1" -> "aaaa")) should equal (true)
  }

  "LuceneRDD.exists(Map)" should "not find elements that don't exist" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.exists(Map("_1" -> "doNotExist")) should equal (false)
  }

  "LuceneRDD.exists(T)" should "find elements that exist" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val localLuceneRDD = LuceneRDD(rdd)
    localLuceneRDD.exists("aaaa") should equal (true)
    localLuceneRDD.close()
  }

  "LuceneRDD.exists(T)" should "not find elements that don't exist" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val localLuceneRDD = LuceneRDD(rdd)
    localLuceneRDD.exists("doNotExist") should equal (false)
    localLuceneRDD.close()
  }

  "LuceneRDD.count" should "count correctly the results" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should equal (5)
  }

  "LuceneRDD.count" should "count zero on empty RDD" in {
    val words = Array.empty[String]
    val rdd = sc.parallelize(words)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should equal (0)
  }

  "LuceneRDD.filter" should "filter correctly existing element" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.filter(x => x.startsWith("aaa")).count should equal (2)
  }

  "LuceneRDD.filter" should "not filter non existing elements" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.filter(x => x.startsWith("iDoNotExist")).count should equal (0)
  }

  "LuceneRDD.fields" should "return _1 as default field" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.fields().contains("_1") should equal(true)
  }

  "LuceneRDD.fields" should "correctly return field types" in {
    val words = Array(("a", 1.0F), ("b", 2.0F), ("c", 3.0F))
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.fields().contains("_1") should equal(true)
    luceneRDD.fields().contains("_2") should equal(true)
  }

  "LuceneRDD.fields" should "return correct fields with RDD[Map[String, String]]" in {
    val maps = List(Map( "a" -> "hello"), Map("b" -> "world"), Map("c" -> "how are you"))
    val rdd = sc.parallelize(maps)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.fields() should equal(Set("a", "b", "c"))
  }

  "LuceneRDD.saveIndexToHDFS" should "return save index to HDFS" in {
    val maps = List(Map( "a" -> "hello"), Map("b" -> "world"), Map("c" -> "how are you"))
    val rdd = sc.parallelize(maps)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.saveToHDFS("/test_location")
    true shouldBe true
  }

  "LuceneRDD.version" should "return project sbt build information" in {
    val map = LuceneRDD.version()
    map.contains("name") should equal(true)
    map.contains("builtAtMillis") should equal(true)
    map.contains("scalaVersion") should equal(true)
    map.contains("version") should equal(true)
    map.contains("sbtVersion") should equal(true)
    map.contains("builtAtString") should equal(true)
  }
}
