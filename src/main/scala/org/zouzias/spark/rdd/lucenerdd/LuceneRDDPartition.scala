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

  def query(q: Query, topK: Int): Iterable[SerializedDocument]

  def termQuery(fieldName: String, query: String, topK: Int): Iterable[SerializedDocument]

  def prefixQuery(fieldName: String, query: String, topK: Int): Iterable[SerializedDocument]

  def fuzzyQuery(fieldName: String, query: String,
                 maxEdits: Int, topK: Int): Iterable[SerializedDocument]

  def phraseQuery(fieldName: String, query: String, topK: Int): Iterable[SerializedDocument]

  /**
   * Restricts the entries to those satisfying the given predicate.
   */
  def filter(pred: T => Boolean): LuceneRDDPartition[T]

  /**
   * Intersects `this` and `other` and keeps only elements with differing values. For these
   * elements, keeps the values from `this`.
   */
  def diff(other: LuceneRDDPartition[T]): LuceneRDDPartition[T]

  /**
   * Intersects `this` and `other` and keeps only elements with differing values. For these
   * elements, keeps the values from `this`.
   */
  def diff(other: Iterator[T]): LuceneRDDPartition[T]
}
