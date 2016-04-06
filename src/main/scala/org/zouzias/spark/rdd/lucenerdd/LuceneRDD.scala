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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.zouzias.spark.rdd.lucenerdd.impl.ElasticLuceneRDDPartition

import scala.reflect.ClassTag

/**
 *
 * @tparam T
 */
class LuceneRDD[T: ClassTag](
    /** The underlying representation of the LuceneRDD as an RDD of partitions. */
    private val partitionsRDD: RDD[LuceneRDDPartition[T]])
  extends RDD[(T)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  override def setName(_name: String): this.type = {
    partitionsRDD.setName(_name)
    this
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[T] = {
    firstParent[LuceneRDDPartition[T]].iterator(part, context).next.iterator
  }

  /** Applies a function to each partition of this LuceneRDD. */
  private def mapIndexedRDDPartitions[T2: ClassTag](
      f: LuceneRDDPartition[T] => LuceneRDDPartition[T2]): LuceneRDD[T2] = {
    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    new LuceneRDD(newPartitionsRDD)
  }

  /**
   * Restricts the entries to those satisfying the given predicate. This operation preserves the
   * index for efficient joins with the original LuceneRDD and is implemented using soft deletions.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to the `RDD[(K, V)]`
   * interface
   */
  override def filter(pred: T => Boolean): LuceneRDD[T] = {
    val newPartitionRDD = partitionsRDD.mapPartitions(partition =>
      partition.map(_.filter(pred)), preservesPartitioning = true
    )
    new LuceneRDD(newPartitionRDD)
  }

  /**
   * Intersects `this` and `other` and keeps only elements with differing values. For these
   * elements, keeps the values from `this`.
   */
  def diff(other: RDD[T]): LuceneRDD[T] = ???

}

object LuceneRDD {
  /**
   * Constructs a LuceneRDD from an RDD of pairs, merging duplicate keys arbitrarily.
   */
  def apply[T: ClassTag]
      (elems: RDD[T]): LuceneRDD[T] = {

    val partitions = elems.mapPartitions[LuceneRDDPartition[T]](
      iter => Iterator(ElasticLuceneRDDPartition(iter)),
      preservesPartitioning = true)
    new LuceneRDD(partitions)
  }

  def apply[T: ClassTag]
  (elems: Iterable[T])(implicit sc: SparkContext): LuceneRDD[T] = {
    apply(sc.parallelize[T](elems.toSeq))
  }
}
