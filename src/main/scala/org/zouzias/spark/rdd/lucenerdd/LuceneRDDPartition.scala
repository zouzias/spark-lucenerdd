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

package org.zouzias.spark.rdd.lucenerdd

import org.apache.lucene.document.Document
import org.apache.lucene.search.Query
import org.zouzias.spark.rdd.lucenerdd.utils.SerializedDocument

import scala.reflect.ClassTag


/**
 *
 * @tparam T the type associated with each entry in the set.
 */
private[lucenerdd] abstract class LuceneRDDPartition[T] extends Serializable {

  protected implicit def kTag: ClassTag[T]

  def size: Long

  def iterator: Iterator[T]

  def isDefined(key: T): Boolean

  def query(docMap: Map[String, String]): Boolean

  /**
   * Generic Lucene Query
   * @param q
   * @param topK
   * @return
   */
  def query(q: Query, topK: Int): Iterable[SerializedDocument]

  /**
   * Term Query
   * @param fieldName
   * @param query
   * @param topK
   * @return
   */
  def termQuery(fieldName: String, query: String, topK: Int): Iterable[SerializedDocument]

  /**
   * Prefix Query
   * @param fieldName
   * @param query
   * @param topK
   * @return
   */
  def prefixQuery(fieldName: String, query: String, topK: Int): Iterable[SerializedDocument]

  /**
   * Fuzzy Query
   * @param fieldName
   * @param query
   * @param maxEdits
   * @param topK
   * @return
   */
  def fuzzyQuery(fieldName: String, query: String,
                 maxEdits: Int, topK: Int): Iterable[SerializedDocument]

  /**
   * PhraseQuery
   * @param fieldName
   * @param query
   * @param topK
   * @return
   */
  def phraseQuery(fieldName: String, query: String, topK: Int): Iterable[SerializedDocument]

  /**
   * Restricts the entries to those satisfying a predicate
   * @param pred
   * @return
   */
  def filter(pred: T => Boolean): LuceneRDDPartition[T]
}