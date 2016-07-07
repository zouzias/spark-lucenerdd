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
package org.zouzias.spark.lucenerdd.models

import org.apache.lucene.search.{IndexSearcher, ScoreDoc}

/**
 * A scored [[SparkDoc]]
 *
 * @param score Score of document
 * @param docId Document id
 * @param shardIndex Shard index
 * @param doc Serialized Lucene document
 */
case class SparkScoreDoc(score: Float, docId: Int, shardIndex: Int, doc: SparkDoc)

object SparkScoreDoc extends Serializable {

  def apply(indexSearcher: IndexSearcher, scoreDoc: ScoreDoc): SparkScoreDoc = {
    SparkScoreDoc(scoreDoc.score, scoreDoc.doc, scoreDoc.shardIndex,
      SparkDoc(indexSearcher.doc(scoreDoc.doc)))
  }

  def apply(indexSearcher: IndexSearcher, scoreDoc: ScoreDoc, score: Float): SparkScoreDoc = {
    SparkScoreDoc(score, scoreDoc.doc, scoreDoc.shardIndex,
      SparkDoc(indexSearcher.doc(scoreDoc.doc)))
  }

  /**
   * Ordering by score (descending)
   */
  implicit val ScoreOrdered = new Ordering[SparkScoreDoc]{
    override def compare(x: SparkScoreDoc, y: SparkScoreDoc): Int = {
      if ( x.score > y.score) -1 else if (x.score == y.score) 0 else 1
    }
  }
}

