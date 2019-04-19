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

import org.apache.lucene.document.Document
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc.{ScoreField, ShardField, DocIdField}

class LuceneDocToSparkRowpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach {

  val doc = new Document()
  val (score: Float, docId: Int, shardIndex: Int) = (1.0f, 1, 2)

  val sparkScoreDoc = SparkScoreDoc(score, docId, shardIndex, doc)

  "SparkScoreDoc.toRow" should "return correct score" in {
    val row = sparkScoreDoc.toRow()
    row.getFloat(row.fieldIndex(ScoreField)) should equal(score)
  }

  "SparkScoreDoc.toRow" should "return correct docId" in {
    val row = sparkScoreDoc.toRow()
    row.getInt(row.fieldIndex(DocIdField)) should equal(score)
  }

  "SparkScoreDoc.toRow" should "return correct shard number" in {
    val row = sparkScoreDoc.toRow()
    row.getInt(row.fieldIndex(ShardField)) should equal(score)
  }
}
