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
import org.zouzias.spark.lucenerdd.testing.Person

class LuceneRDDCustomCaseClassImplicitsSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  var luceneRDD: LuceneRDD[_] = _

  override def afterEach() {
    luceneRDD.close()
  }

  override val conf = LuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  val elem = Array("fear", "death", "water", "fire", "house")
    .zipWithIndex.map{ case (str, index) => Person(str, index, s"${str}@gmail.com")}

  "LuceneRDD(case class).count" should "handle nulls properly" in {
    val elemsWithNulls = Array("fear", "death", "water", "fire", "house")
      .zipWithIndex.map{ case (str, index) => Person(str, index, null)}
    val rdd = sc.parallelize(elemsWithNulls)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count() should equal (elemsWithNulls.length)
  }

  "LuceneRDD(case class).count" should "return correct number of elements" in {
    val rdd = sc.parallelize(elem)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count() should equal (elem.length)
  }

  "LuceneRDD(case class).fields" should "return all fields" in {
    val rdd = sc.parallelize(elem)
    luceneRDD = LuceneRDD(rdd)

    luceneRDD.fields().size should equal(3)
    luceneRDD.fields().contains("name") should equal(true)
    luceneRDD.fields().contains("age") should equal(true)
    luceneRDD.fields().contains("email") should equal(true)
  }

  "LuceneRDD(case class).termQuery" should "correctly search with TermQueries" in {
    val rdd = sc.parallelize(elem)
    luceneRDD = LuceneRDD(rdd)

    val results = luceneRDD.termQuery("name", "water")
    results.count() should equal(1)
  }
}
