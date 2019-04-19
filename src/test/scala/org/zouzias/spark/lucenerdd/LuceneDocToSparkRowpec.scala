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

import org.apache.lucene.document.{Document, DoublePoint, FloatPoint, IntPoint, LongPoint, StoredField, TextField}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc.{DocIdField, ScoreField, ShardField}

class LuceneDocToSparkRowpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach {

  val (score: Float, docId: Int, shardIndex: Int) = (1.0f, 1, 2)

  def generate_doc(): Document = {
    val doc = new Document()

    // Add long field
    doc.add(new LongPoint("longField", 10))
    doc.add(new StoredField("longField", 10))

    doc.add(new FloatPoint("floatField", 20.1f))
    doc.add(new StoredField("floatField", 20.1f))

    doc.add(new IntPoint("intField", 9))
    doc.add(new StoredField("intField", 9))

    doc.add(new DoublePoint("doubleField", 10.11D))
    doc.add(new StoredField("doubleField", 10.11D))

    doc
  }

  val doc = generate_doc()

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
    row.getFields().size() should equal(7)
  }

  "SparkScoreDoc.toRow" should "set correctly DoublePoint" in {
    val row = sparkScoreDoc.toRow()
    row.getDouble(row.fieldIndex("doubleField")) should equal(10.11D)
  }

  "SparkScoreDoc.toRow" should "set correctly FloatPoint" in {
    val row = sparkScoreDoc.toRow()
    row.getDouble(row.fieldIndex("doubleField")) should equal(20.1D)
  }
}
