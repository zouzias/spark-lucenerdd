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
package org.zouzias.spark.lucenerdd.matrices

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.zouzias.spark.lucenerdd.models.TermVectorEntry

/**
  * Term Document Matrix of a Lucene field
  *
  * Each term (row of matrix) is uniquely assigned an index
  */
class TermDocMatrix(triplets: RDD[TermVectorEntry]) extends Serializable {

  private lazy val docIdsPerShardMap = computeUniqueDocId()

  private lazy val indexedTerms = triplets.map(_.term).distinct().zipWithIndex().map(_.swap)
  private lazy val indexToTerm: Map[Long, String] = indexedTerms.collect().toMap
  private lazy val termToIndex: Map[String, Long] = indexToTerm.map(_.swap)

  private lazy val value_ = toMatrix()

  private lazy val nnz_ = value_.entries.count()

  /**
    * Row index to term map
    * @return
    */
  def rowIndexToTerm(): Map[Long, String] = indexToTerm

  def computeUniqueDocId(): Map[(Int, Long), Long] = {
    triplets.map(_.docIdPerShard).distinct().zipWithIndex()
      .collect().toMap
  }

  private def toMatrix(): CoordinateMatrix = {

    // Broadcast termToIndex Map
    val termToIndexB = triplets.sparkContext.broadcast(termToIndex)
    val docIdsPerShardMapB = triplets.sparkContext.broadcast(docIdsPerShardMap)

    val entries = triplets.map { case t =>
      val i = termToIndexB.value(t.term)
      val j = docIdsPerShardMapB.value(t.docIdPerShard)
      MatrixEntry(i, j, t.count)
    }

    new CoordinateMatrix(entries)
  }

  /**
    * Returns the number of non-zero entries
    * @return
    */
  def nnz(): Long = {
    nnz_
  }

  /**
    * Number of rows (terms)
    * @return
    */
  def numRows(): Long = value_.numRows()

  /**
    * Number of columns (documents)
    * @return
    */
  def numCols(): Long = value_.numCols()

  def value(): CoordinateMatrix = value_
}
