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
import org.apache.spark.sql.Row

/**
 * A scored [[SparkDoc]]
 *
 * @param score Score of document
 * @param docId Document id
 * @param shardIndex Shard index
 * @param doc Serialized Lucene document
 */
case class SparkScoreDoc(score: Float, docId: Int, shardIndex: Int, doc: SparkDoc) {

  override def toString: String = {
    val builder = new StringBuilder
    builder.append(s"[score: ${score}/")
    builder.append(s"docId: ${docId}/")
    builder.append(s"doc: ${doc}]")
    builder.result()
  }
}

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
  def descending: Ordering[Row] = new Ordering[Row]{
    override def compare(x: Row, y: Row): Int = {
      val xScore = x.getDouble(x.fieldIndex("score"))
      val yScore = y.getDouble(y.fieldIndex("score"))
      if ( xScore > yScore) {
        -1
      } else if (xScore == yScore) 0 else 1
    }
  }

  /**
   * Ordering by score (ascending)
   */
  def ascending: Ordering[Row] = new Ordering[Row]{
    override def compare(x: Row, y: Row): Int = {
      val xScore = x.getDouble(x.fieldIndex("score"))
      val yScore = y.getDouble(y.fieldIndex("score"))

      if ( xScore < yScore) -1 else if (xScore == yScore) 0 else 1
    }
  }
}


