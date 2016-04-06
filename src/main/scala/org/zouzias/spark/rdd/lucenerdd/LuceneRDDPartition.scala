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

import scala.reflect.ClassTag

/**
 * A map of key-value `(K, V)` pairs that enforces key uniqueness and pre-indexes the entries for
 * fast lookups, joins, and optionally updates. To construct an `IndexedRDDPartition`, use one of
 * the constructors in the [[org.zouzias.spark.rdd.lucenerdd.LuceneRDDPartition object]].
 *
 * @tparam T the key associated with each entry in the set.
 */
private[lucenerdd] abstract class LuceneRDDPartition[T] extends Serializable {

  protected implicit def kTag: ClassTag[T]

  def size: Long

  def iterator: Iterator[T]

  def isDefined(key: T): Boolean

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
