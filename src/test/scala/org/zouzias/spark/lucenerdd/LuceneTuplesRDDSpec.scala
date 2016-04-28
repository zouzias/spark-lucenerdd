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
import org.scalatest.{ FlatSpec, Matchers }
import org.zouzias.spark.lucenerdd.implicits.LuceneRDDImplicits._

class LuceneTuplesRDDSpec extends FlatSpec with Matchers with SharedSparkContext {

  val First = "_1"
  val Second = "_2"

  def randomString(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString
  val array = (1 to 24).map(randomString(_))

  "LuceneRDD" should "work with Tuple2" in {
    val rdd = sc.parallelize(array).map(x => (x, x))
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should equal (array.size)
  }

  "LuceneRDD" should "work with Tuple3" in {
    val rdd = sc.parallelize(array).map(x => (x, x, x))
    val luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.termQuery(Second,
      array(scala.util.Random.nextInt(array.size)))
    results.size should equal (1)
  }

  "LuceneRDD" should "work with Tuple4" in {
    val rdd = sc.parallelize(array).map(x => (x, x, x, x))
    val luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.termQuery(Second,
      array(scala.util.Random.nextInt(array.size)))
    results.size should equal (1)
  }

  "LuceneRDD" should "work with Tuple5" in {
    val rdd = sc.parallelize(array).map(x => (x, x, x, x, x))
    val luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.termQuery(Second,
      array(scala.util.Random.nextInt(array.size)))
    results.size should equal (1)
  }

  "LuceneRDD" should "work with Tuple6" in {
    val rdd = sc.parallelize(array).map(x => (x, x, x, x, x, x))
    val luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.termQuery(Second,
      array(scala.util.Random.nextInt(array.size)))
    results.size should equal (1)
  }

  "LuceneRDD" should "work with Tuple7" in {
    val rdd = sc.parallelize(array).map(x => (x, x, 2.0d, 1.0d, x, 1, x))
    val luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.termQuery(First,
      array(scala.util.Random.nextInt(array.size)))
    results.size should equal (1)
  }

  "LuceneRDD" should "work with mixed types in Tuples" in {
    val rdd = sc.parallelize(array).map(x => (x, 1, x, 2L, x, 3.0F))
    val luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.termQuery(First,
      array(scala.util.Random.nextInt(array.size)))
    results.size should equal (1)
  }
}