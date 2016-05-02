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
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.implicits.LuceneRDDImplicits._

class LucenePrimitiveTypesSpec extends FlatSpec with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  def randomString(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString
  val array = (1 to 24).map(randomString(_))

  var luceneRDD: LuceneRDD[_] = _

  override def afterEach() {
    luceneRDD.close()
  }

  /**
   * Do not work with facets (multi-valued issue)

  "LuceneRDD" should "work with RDD[List[String]]" in {
    val array = Array(List("aaa", "aaa2"), List("bbb", "bbb2"),
      List("ccc", "ccc2"), List("ddd"), List("eee"))
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "work with RDD[Set[String]]" in {
    val array = Array(Set("aaa", "aaa2"), Set("bbb", "bbb2"),
      Set("ccc", "ccc2"), Set("ddd"), Set("eee"))
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

   */

  "LuceneRDD" should "work with RDD[String]" in {
    val array = Array("aaa", "bbb", "ccc", "ddd", "eee")
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "work with RDD[Int]" in {
    val array = (1 to 22)
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "work with RDD[Float]" in {
    val array: IndexedSeq[Float] = (1 to 22).map(_.toFloat)
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "work with RDD[Double]" in {
    val array: IndexedSeq[Double] = (1 to 22).map(_.toDouble)
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "work with RDD[Long]" in {
    val array: IndexedSeq[Long] = (1 to 22).map(_.toLong)
    val rdd = sc.parallelize(array)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should equal (array.size)
  }

  "LuceneRDD" should "work with RDD[Map[String, String]]" in {
    val maps = List(Map( "a" -> "hello"), Map("b" -> "world"), Map("c" -> "how are you"))
    val rdd = sc.parallelize(maps)
    luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (maps.size)
    luceneRDD.termQuery("a", "hello").nonEmpty should equal (true)
    luceneRDD.prefixQuery("b", "wor").nonEmpty should equal (true)
    luceneRDD.prefixQuery("a", "no").nonEmpty should equal (false)
  }
}