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
import org.zouzias.spark.lucenerdd.testing.LuceneRDDTestUtils

class LuceneRDDTermVectorsSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with LuceneRDDTestUtils
  with SharedSparkContext {

  var luceneRDD: LuceneRDD[_] = _

  override def Radius: Double = 0

  override val conf: SparkConf = LuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  override def afterEach() {
    luceneRDD.close()
  }

  val First = "_1"


  "LuceneRDD.termVectors" should "return valid terms" in {

    val words = Array("To smile or not to smile smile",
      "Don't cry because it's over, smile because it happened",
      "So many books, so little time",
      "A room without books is like a body without a soul",
      "If you tell the truth, you don't have to remember anything")
    val rdd = sc.parallelize(words)


    luceneRDD = LuceneRDD(rdd)

    val terms = luceneRDD.termVectors(First).collect()

    terms.foreach(println)

    terms.exists(_.term.compareToIgnoreCase("time") == 0) should equal(true)
    terms.exists(_.term.compareToIgnoreCase("room") == 0) should equal(true)
    terms.exists(_.term.compareToIgnoreCase("soul") == 0) should equal(true)
    terms.exists(_.term.compareToIgnoreCase("smile") == 0) should equal(true)

    terms.exists(t => (t.term.compareToIgnoreCase("smile") == 0) && t.count == 3) should equal (3)
    terms.exists(t => (t.term.compareToIgnoreCase("becaus") == 0) && t.count == 2) should equal (3)
  }

}