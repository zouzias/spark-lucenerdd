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

class LuceneRDDSpec extends FlatSpec with Matchers with SharedSparkContext {

  "LuceneRDD.exists(Map)" should "find elements that exist" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.exists(Map("_1" -> "aaaa")) should equal (true)
  }

  "LuceneRDD.exists(Map)" should "not find elements that don't exist" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.exists(Map("_1" -> "doNotExist")) should equal (false)
  }

  "LuceneRDD.exists(T)" should "find elements that exist" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.exists("aaaa") should equal (true)
  }

  "LuceneRDD.exists(T)" should "not find elements that don't exist" in {
    val words = Array("aabaa", "aaacaa", "aadaa", "aaaa", "qwerty")
    val rdd = sc.parallelize(words)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.exists("doNotExist") should equal (false)
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
    val luceneRDD = LuceneRDD(rdd)
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
}
