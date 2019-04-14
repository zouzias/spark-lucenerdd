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
import scala.collection.JavaConverters._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.io.Source

class LuceneRDDMoreLikeThisSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  var luceneRDD: LuceneRDD[_] = _


  override val conf = LuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  override def afterEach() {
    luceneRDD.close()
  }

  "LuceneRDD.moreLikeThis" should "return relevant documents" in {
    val words: Seq[String] = Source.fromFile("src/test/resources/alice.txt")
      .getLines().map(_.toLowerCase).toSeq
    val rdd = sc.parallelize(words)
    luceneRDD = LuceneRDD(rdd)
    val results = luceneRDD.moreLikeThis("_1", "alice adventures wonderland", 1, 1).collect()

    results.length > 0 should equal(true)
    val firstDoc = results.head
    firstDoc.getList[String](firstDoc.fieldIndex("_1")).asScala
        .exists(x => x.contains("alice")
    && x.contains("wonderland")
    && x.contains("adventures")) should equal(true)

    val lastDoc = results.head
    lastDoc.getList[String](lastDoc.fieldIndex("_1")).asScala
    .exists(x => x.contains("alice")
      && !x.contains("wonderland")
      && !x.contains("adventures")) should equal(true)

  }
}
