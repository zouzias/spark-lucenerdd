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
import org.apache.lucene.search.{Query, TermQuery}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import matchers.should._
import org.zouzias.spark.lucenerdd.testing.Person

class BlockingLinkageSpec extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  override val conf: SparkConf = LuceneRDDKryoRegistrator.registerKryoClasses(new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID))

  "LuceneRDD.blockEntityLinkage" should "deduplicate elements on unique elements" in {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val peopleLeft: Array[Person] = Array("fear", "death", "water", "fire", "house")
      .zipWithIndex.map { case (str, index) =>
      val email = if (index % 2 == 0) "yes@gmail.com" else "no@gmail.com"
      Person(str, index, email)
    }

    val peopleRight: Array[Person] = Array("fear", "death", "water", "fire", "house")
      .zipWithIndex.map { case (str, index) =>
      val email = if (index % 2 == 0) "yes@gmail.com" else "no@gmail.com"
      Person(str, index, email)
    }

    val leftDF = sc.parallelize(peopleLeft).repartition(2).toDF()
    val rightDF = sc.parallelize(peopleRight).repartition(3).toDF()

    // Define a Lucene Term linker
    val linker: Row => Query = { row =>
      val name = row.getString(row.fieldIndex("name"))
      val term = new Term("name", name)

      new TermQuery(term)
    }


    val linked = LuceneRDD.blockEntityLinkage(leftDF, rightDF, linker,
      Array("email"), Array("email"))

    val linkedCount, dfCount = (linked.count, leftDF.count())

    linkedCount should equal(dfCount)

    // Check for correctness
    // Age is a unique index
    linked.collect().foreach { case (row, results) =>
      val leftAge, rightAge = (row.getInt(row.fieldIndex("age")),
        results.headOption.map(x => x.getInt(x.fieldIndex("age"))))

      leftAge should equal(rightAge)

    }
  }
}
