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

import org.apache.lucene.search.Query
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

import scala.reflect.ClassTag


/**
 *
 * @tparam T the type associated with each entry in the set.
 */
private[lucenerdd] abstract class AbstractLuceneRDDPartition[T] extends Serializable {

  protected implicit def kTag: ClassTag[T]

  def size: Long

  def iterator: Iterator[T]

  def isDefined(key: T): Boolean

  def multiTermQuery(docMap: Map[String, String], topK: Int): Iterable[SparkScoreDoc]

  def close(): Unit

  /**
   * Generic Lucene Query
   * @param q
   * @param topK
   * @return
   */
  def query(q: Query, topK: Int): Iterable[SparkScoreDoc]

  /**
   * Term Query
   * @param fieldName
   * @param query
   * @param topK
   * @return
   */
  def termQuery(fieldName: String, query: String, topK: Int): Iterable[SparkScoreDoc]

  /**
   * Prefix Query
   * @param fieldName
   * @param query
   * @param topK
   * @return
   */
  def prefixQuery(fieldName: String, query: String, topK: Int): Iterable[SparkScoreDoc]

  /**
   * Fuzzy Query
   * @param fieldName
   * @param query
   * @param maxEdits
   * @param topK
   * @return
   */
  def fuzzyQuery(fieldName: String, query: String,
                 maxEdits: Int, topK: Int): Iterable[SparkScoreDoc]

  /**
   * PhraseQuery
   * @param fieldName
   * @param query
   * @param topK
   * @return
   */
  def phraseQuery(fieldName: String, query: String, topK: Int): Iterable[SparkScoreDoc]

  /**
   * Restricts the entries to those satisfying a predicate
   * @param pred
   * @return
   */
  def filter(pred: T => Boolean): AbstractLuceneRDDPartition[T]
}
