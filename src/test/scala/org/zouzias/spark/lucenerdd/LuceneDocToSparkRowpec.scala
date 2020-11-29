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

import java.io.{Reader, StringReader}

import org.apache.lucene.document.{Document, DoublePoint, Field, FloatPoint, IntPoint, LongPoint, StoredField, TextField}
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc.{DocIdField, ScoreField, ShardField}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import matchers.should._


import scala.collection.JavaConverters._

class LuceneDocToSparkRowpec extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterEach {

  val (score: Float, docId: Int, shardIndex: Int) = (1.0f, 1, 2)
  val float: Float = 20.001f
  val double: Double = 10.1000000001D

  def generate_doc(): Document = {
    val doc = new Document()

    // Add long field
    doc.add(new LongPoint("longField", 10))
    doc.add(new StoredField("longField", 10))

    doc.add(new FloatPoint("floatField", float))
    doc.add(new StoredField("floatField", float))

    doc.add(new IntPoint("intField", 9))
    doc.add(new StoredField("intField", 9))

    doc.add(new DoublePoint("doubleField", double))
    doc.add(new StoredField("doubleField", double))

    doc.add(new TextField("textField", "hello world", Field.Store.NO))
    doc.add(new StoredField("textField", "hello world"))

    doc
  }

  private val doc: Document = generate_doc()

  val sparkScoreDoc = SparkScoreDoc(score, docId, shardIndex, doc)


  "SparkScoreDoc.toRow" should "return correct score" in {
    val row = sparkScoreDoc.toRow()
    row.getFloat(row.fieldIndex(ScoreField)) should equal(score)
  }

  "SparkScoreDoc.toRow" should "return correct docId" in {
    val row = sparkScoreDoc.toRow()
    row.getInt(row.fieldIndex(DocIdField)) should equal(docId)
  }

  "SparkScoreDoc.toRow" should "return correct shard number" in {
    val row = sparkScoreDoc.toRow()
    row.getInt(row.fieldIndex(ShardField)) should equal(shardIndex)
  }

  "SparkScoreDoc.toRow" should "return correct number of fields" in {
    val row = sparkScoreDoc.toRow()
    row.getFields().asScala.count(_.fieldType().stored()) should equal(8)
  }

  "SparkScoreDoc.toRow" should "set correctly DoublePoint" in {
    val row = sparkScoreDoc.toRow()
    row.getDouble(row.fieldIndex("doubleField")) should equal(double)
  }

  "SparkScoreDoc.toRow" should "set correctly FloatPoint" in {
    val row = sparkScoreDoc.toRow()
    row.getFloat(row.fieldIndex("floatField")) should equal(float)
  }
}
