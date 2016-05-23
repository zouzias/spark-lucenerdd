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
package org.zouzias.spark.lucenerdd.spatial.point.partition

import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

import scala.reflect.ClassTag

abstract class AbstractPointLuceneRDDPartition[K, V] extends Serializable {

  protected implicit def kTag: ClassTag[K]
  protected implicit def vTag: ClassTag[V]

  def size: Long

  def iterator: Iterator[(K, V)]

  def isDefined(key: K): Boolean

  def close(): Unit

  /**
   * Generic Lucene Query using QueryParser
   *
   * @param searchString Lucene query string, i.e., textField:hello*
   * @param topK Number of documents to return
   * @return
   */
  def query(searchString: String, topK: Int): Iterable[SparkScoreDoc]

  /**
   * Restricts the entries to those satisfying a predicate
   *
   * @param pred
   * @return
   */
  def filter(pred: (K, V) => Boolean): AbstractPointLuceneRDDPartition[K, V]
}
