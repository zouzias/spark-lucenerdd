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

class LucenePrimitiveTypesSpec extends FlatSpec with Matchers with SharedSparkContext {

  def randomString(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString
  val array = (1 to 24).map(randomString(_))

  "LuceneRDD" should "work with RDD[Int]" in {
    val array = (1 to 22)
    val rdd = sc.parallelize(array)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "work with RDD[Float]" in {
    val array: IndexedSeq[Float] = (1 to 22).map(_.toFloat)
    val rdd = sc.parallelize(array)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "work with RDD[Double]" in {
    val array: IndexedSeq[Double] = (1 to 22).map(_.toDouble)
    val rdd = sc.parallelize(array)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should be (array.size)
  }

  "LuceneRDD" should "work with RDD[Long]" in {
    val array: IndexedSeq[Long] = (1 to 22).map(_.toLong)
    val rdd = sc.parallelize(array)
    val luceneRDD = LuceneRDD(rdd)
    luceneRDD.count should equal (array.size)
  }
}