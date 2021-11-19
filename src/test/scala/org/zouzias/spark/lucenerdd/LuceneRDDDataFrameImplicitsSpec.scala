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
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.testing.{FavoriteCaseClass, MultivalueFavoriteCaseClass}

class LuceneRDDDataFrameImplicitsSpec extends FlatSpec
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


  val elem: Array[FavoriteCaseClass] = Array("fear", "death", "water", "fire", "house")
    .zipWithIndex.map{ case (str, index) =>
    FavoriteCaseClass(str, index, 10L, 12.3F, s"${str}@gmail.com")}

  val multiValuesElems: Array[MultivalueFavoriteCaseClass] = Array("fear",
    "death", "water", "fire", "house")
    .zipWithIndex.map{ case (str, index) =>
    MultivalueFavoriteCaseClass(Array(str, str.reverse), index, List(index, index + 1, index + 2),
      10L, 12.3F, s"${str}@gmail.com")}

  "LuceneRDD(MultivalueFavoriteCaseClass).count" should "return correct number of elements" in {
    val rdd = sc.parallelize(multiValuesElems)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = LuceneRDD(df)
    luceneRDD.count should equal (elem.length)
  }

  "LuceneRDD(case class).count" should "return correct number of elements" in {
    val rdd = sc.parallelize(elem)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = LuceneRDD(df)
    luceneRDD.count should equal (elem.length)
  }

  "LuceneRDD(case class).fields" should "return all fields" in {
    val rdd = sc.parallelize(elem)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = LuceneRDD(df)

    luceneRDD.fields().size should equal(5)
    luceneRDD.fields().contains("name") should equal(true)
    luceneRDD.fields().contains("age") should equal(true)
    luceneRDD.fields().contains("myLong") should equal(true)
    luceneRDD.fields().contains("myFloat") should equal(true)
    luceneRDD.fields().contains("email") should equal(true)
  }

  "LuceneRDD(MultivalueFavoriteCaseClass).fields" should "return all fields" in {
    val rdd = sc.parallelize(multiValuesElems)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = LuceneRDD(df)

    luceneRDD.fields().size should equal(6)
    luceneRDD.fields().contains("names") should equal(true)
    luceneRDD.fields().contains("age") should equal(true)
    luceneRDD.fields().contains("ages") should equal(true)
    luceneRDD.fields().contains("myLong") should equal(true)
    luceneRDD.fields().contains("myFloat") should equal(true)
    luceneRDD.fields().contains("email") should equal(true)
  }

  "LuceneRDD(case class).termQuery" should "correctly search with TermQueries" in {
    val rdd = sc.parallelize(elem)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = LuceneRDD(df)

    val results = luceneRDD.termQuery("name", "water")
    results.count should equal(1)
  }

  "LuceneRDD(MultivalueFavoriteCaseClass).termQuery" should "correctly search with TermQueries" in {
    val rdd = sc.parallelize(multiValuesElems)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = LuceneRDD(df)

    val results = luceneRDD.termQuery("names", "retaw")
    results.count should equal(1)
  }

  "LuceneRDD(MultivalueFavoriteCaseClass).termQuery" should
    "correctly returns LuceneRDDResponse" in {
    val rdd = sc.parallelize(multiValuesElems)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = LuceneRDD(df)

    val results = luceneRDD.termQuery("names", "water")

    results.count() should equal(1)

    val lucenRDDResponseDF = results.toDF()(spark)

    lucenRDDResponseDF.schema.fields.map(_.name).contains("names") should equal(true)
    lucenRDDResponseDF.schema.fields.map(_.name).contains("age") should equal(true)
    lucenRDDResponseDF.schema.fields.map(_.name).contains("ages") should equal(true)
    lucenRDDResponseDF.schema.fields.map(_.name).contains("myLong") should equal(true)
    lucenRDDResponseDF.schema.fields.map(_.name).contains("myFloat") should equal(true)
    lucenRDDResponseDF.schema.fields.map(_.name).contains("email") should equal(true)
  }

  "LuceneRDD(MultivalueFavoriteCaseClass).termQuery" should
    "correctly returns List[Int] fields" in {
    val rdd = sc.parallelize(multiValuesElems)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = rdd.toDF()
    luceneRDD = LuceneRDD(df)

    val results = luceneRDD.termQuery("names", "water")

    val lucenRDDResponseDF = results.toDF()(spark)
    lucenRDDResponseDF.count() should equal(1)

    lucenRDDResponseDF.schema.fields.map(_.name).contains("ages") should equal(true)
    val ages = lucenRDDResponseDF.select($"ages")
      .rdd
      .map(x => x.getList[Int](x.fieldIndex("ages")))

    List(2, 3, 4).foreach{x =>
      ages.collect().head should contain (x)
    }
  }
}
