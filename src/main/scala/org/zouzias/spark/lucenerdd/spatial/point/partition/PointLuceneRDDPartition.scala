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

import scala.reflect._


private[lucenerdd] class PointLuceneRDDPartition[K, V]
  (private val iter: Iterator[(K, V)])
  (override implicit val kTag: ClassTag[K],
   override implicit val vTag: ClassTag[V])
  (implicit keyConversion: K => (Double, Double))
  extends AbstractPointLuceneRDDPartition[K, V]{

  override def size: Long = ???

  /**
   * Restricts the entries to those satisfying a predicate
   *
   * @param pred
   * @return
   */
  override def filter(pred: (K, V) => Boolean): AbstractPointLuceneRDDPartition[K, V] = ???

  override def isDefined(key: K): Boolean = ???


  override def close(): Unit = ???

  override def iterator: Iterator[(K, V)] = ???


  /**
   * Generic Lucene Query using QueryParser
   *
   * @param searchString Lucene query string, i.e., textField:hello*
   * @param topK         Number of documents to return
   * @return
   */
  override def query(searchString: String, topK: Int)
  : Iterable[SparkScoreDoc] = ???

}

object PointLuceneRDDPartition {
  def apply[K: ClassTag, V: ClassTag]
  (iter: Iterator[(K, V)])
  (implicit keyToPoint: K => (Double, Double)): PointLuceneRDDPartition[K, V] = {
    new PointLuceneRDDPartition[K, V](iter)(classTag[K], classTag[V])(keyToPoint)
  }
}
