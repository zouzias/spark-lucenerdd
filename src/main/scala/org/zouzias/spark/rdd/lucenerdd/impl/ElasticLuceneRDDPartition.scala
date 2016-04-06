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

package org.zouzias.spark.rdd.lucenerdd.impl

import org.apache.spark.Logging
import org.zouzias.spark.rdd.lucenerdd.LuceneRDDPartition

import scala.reflect.ClassTag

private[lucenerdd] class ElasticLuceneRDDPartition[K, V]
    (protected val map: Map[K, V])
    (override implicit val kTag: ClassTag[K],
     override implicit val vTag: ClassTag[V])
  extends LuceneRDDPartition[K, V] with Logging {

  override def size: Long = map.size.toLong

  override def apply(k: K): V = ???

  override def isDefined(k: K): Boolean = ???

  override def iterator: Iterator[(K, V)] = ???

  private def rawIterator: Iterator[(Array[Byte], V)] = ???

  override def multiget(ks: Iterator[K]): Iterator[(K, V)] =
    ks.flatMap { k => Option(this(k)).map(v => (k, v)) }

  override def mapValues[V2: ClassTag](f: (K, V) => V2): LuceneRDDPartition[K, V2] = ???

  override def filter(pred: (K, V) => Boolean): LuceneRDDPartition[K, V] = ???

  override def diff(other: LuceneRDDPartition[K, V]): LuceneRDDPartition[K, V] = ???

  override def diff(other: Iterator[(K, V)]): LuceneRDDPartition[K, V] =
    diff(ElasticLuceneRDDPartition(other))

  override def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: LuceneRDDPartition[K, V2])
      (f: (K, Option[V], Option[V2]) => W): LuceneRDDPartition[K, W] = ???

  override def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, Option[V], Option[V2]) => W): LuceneRDDPartition[K, W] =
    fullOuterJoin(ElasticLuceneRDDPartition(other))(f)

  override def join[U: ClassTag]
      (other: LuceneRDDPartition[K, U])
      (f: (K, V, U) => V): LuceneRDDPartition[K, V] = join(other.iterator)(f)

  override def join[U: ClassTag]
      (other: Iterator[(K, U)])
      (f: (K, V, U) => V): LuceneRDDPartition[K, V] = ???

  override def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: LuceneRDDPartition[K, V2])
      (f: (K, V, Option[V2]) => V3): LuceneRDDPartition[K, V3] = ???

  override def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, V, Option[V2]) => V3): LuceneRDDPartition[K, V3] =
    leftJoin(ElasticLuceneRDDPartition(other))(f)

  override def innerJoin[U: ClassTag, V2: ClassTag]
      (other: LuceneRDDPartition[K, U])
      (f: (K, V, U) => V2): LuceneRDDPartition[K, V2] = ???

  override def innerJoin[U: ClassTag, V2: ClassTag]
      (other: Iterator[(K, U)])
      (f: (K, V, U) => V2): LuceneRDDPartition[K, V2] =
    innerJoin(ElasticLuceneRDDPartition(other))(f)
}

private[lucenerdd] object ElasticLuceneRDDPartition {
  def apply[K: ClassTag, V: ClassTag]
      (iter: Iterator[(K, V)]) =
    apply[K, V, V](iter, (id, a) => a, (id, a, b) => b)

  def apply[K: ClassTag, U: ClassTag, V: ClassTag]
      (iter: Iterator[(K, U)], z: (K, U) => V, f: (K, V, U) => V): ElasticLuceneRDDPartition[K, V] = {
    val map = iter.map { case (key, value) =>
     key -> z(key, value)
    }.toMap[K, V]
    new ElasticLuceneRDDPartition[K, V](map)
  }
}
