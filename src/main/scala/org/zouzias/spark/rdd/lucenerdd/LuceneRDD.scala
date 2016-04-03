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
 * A lucene based RDD of key-value `(K, V)` pairs that pre-indexes the entries for fast lookups, joins, and
 * optionally updates. To construct an `IndexedRDD`, use one of the constructors in the
 * [[org.zouzias.spark.rdd.lucenerdd.LuceneRDD object]].
 *
 * @tparam K the key associated with each entry in the set.
 * @tparam V the value associated with each entry in the set.
 */
class LuceneRDD[K: ClassTag, V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    private val partitionsRDD: RDD[LuceneRDDPartition[K, V]])
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner

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
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    firstParent[LuceneRDDPartition[K, V]].iterator(part, context).next.iterator
  }

  /** Applies a function to each partition of this IndexedRDD. */
  private def mapIndexedRDDPartitions[K2: ClassTag, V2: ClassTag](
      f: LuceneRDDPartition[K, V] => LuceneRDDPartition[K2, V2]): LuceneRDD[K2, V2] = {
    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    new LuceneRDD(newPartitionsRDD)
  }

  /** Applies a function to corresponding partitions of `this` and another IndexedRDD. */
  private def zipIndexedRDDPartitions[V2: ClassTag, V3: ClassTag](other: LuceneRDD[K, V2])
      (f: ZipPartitionsFunction[V2, V3]): LuceneRDD[K, V3] = {
    assert(partitioner == other.partitioner)
    val newPartitionsRDD = partitionsRDD.zipPartitions(other.partitionsRDD, true)(f)
    new LuceneRDD(newPartitionsRDD)
  }

  /** Applies a function to corresponding partitions of `this` and a pair RDD. */
  private def zipPartitionsWithOther[V2: ClassTag, V3: ClassTag](other: RDD[(K, V2)])
      (f: OtherZipPartitionsFunction[V2, V3]): LuceneRDD[K, V3] = {
    val partitioned = other.partitionBy(partitioner.get)
    val newPartitionsRDD = partitionsRDD.zipPartitions(partitioned, true)(f)
    new LuceneRDD(newPartitionsRDD)
  }

  /**
   * Restricts the entries to those satisfying the given predicate. This operation preserves the
   * index for efficient joins with the original IndexedRDD and is implemented using soft deletions.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to the `RDD[(K, V)]`
   * interface
   */
  override def filter(pred: Tuple2[K, V] => Boolean): LuceneRDD[K, V] =
    this.mapIndexedRDDPartitions(_.filter(Function.untupled(pred)))

  /** Maps each value, preserving the index. */
  def mapValues[V2: ClassTag](f: V => V2): LuceneRDD[K, V2] =
    this.mapIndexedRDDPartitions(_.mapValues((vid, attr) => f(attr)))

  /** Maps each value, supplying the corresponding key and preserving the index. */
  def mapValues[V2: ClassTag](f: (K, V) => V2): LuceneRDD[K, V2] =
    this.mapIndexedRDDPartitions(_.mapValues(f))

  /**
   * Intersects `this` and `other` and keeps only elements with differing values. For these
   * elements, keeps the values from `this`.
   */
  def diff(other: RDD[(K, V)]): LuceneRDD[K, V] = other match {
    case other: LuceneRDD[K, V] if partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new DiffZipper)
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherDiffZipper)
  }

  /**
   * Joins `this` with `other`, running `f` on the values of all keys in both sets. Note that for
   * efficiency `other` must be an IndexedRDD, not just a pair RDD. Use [[???]] to
   * construct an IndexedRDD co-partitioned with `this`.
   */
  def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: RDD[(K, V2)])
      (f: (K, Option[V], Option[V2]) => W): LuceneRDD[K, W] = other match {
    case other: LuceneRDD[K, V2] if partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new FullOuterJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherFullOuterJoinZipper(f))
  }

  /**
   * Left outer joins `this` with `other`, running `f` on the values of corresponding keys. Because
   * values in `this` with no corresponding entries in `other` are preserved, `f` cannot change the
   * value type.
   */
  def join[U: ClassTag]
      (other: RDD[(K, U)])(f: (K, V, U) => V): LuceneRDD[K, V] = other match {
    case other: LuceneRDD[K, U] if partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new JoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherJoinZipper(f))
  }

  /** Left outer joins `this` with `other`, running `f` on all values of `this`. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: RDD[(K, V2)])(f: (K, V, Option[V2]) => V3): LuceneRDD[K, V3] = other match {
    case other: LuceneRDD[K, V2] if partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new LeftJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherLeftJoinZipper(f))
  }

  /** Inner joins `this` with `other`, running `f` on the values of corresponding keys. */
  def innerJoin[V2: ClassTag, V3: ClassTag](other: RDD[(K, V2)])
      (f: (K, V, V2) => V3): LuceneRDD[K, V3] = other match {
    case other: LuceneRDD[K, V2] if partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new InnerJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherInnerJoinZipper(f))
  }

  // The following functions could have been anonymous, but we name them to work around a Scala
  // compiler bug related to specialization.

  private type ZipPartitionsFunction[V2, V3] = Function2[Iterator[LuceneRDDPartition[K, V]], Iterator[LuceneRDDPartition[K, V2]],
      Iterator[LuceneRDDPartition[K, V3]]]

  private type OtherZipPartitionsFunction[V2, V3] =
    Function2[Iterator[LuceneRDDPartition[K, V]], Iterator[(K, V2)],
      Iterator[LuceneRDDPartition[K, V3]]]


  private class DiffZipper extends ZipPartitionsFunction[V, V] with Serializable {
    def apply(thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[LuceneRDDPartition[K, V]]): Iterator[LuceneRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.diff(otherPart))
    }
  }

  private class OtherDiffZipper extends OtherZipPartitionsFunction[V, V] with Serializable {
    def apply(thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[(K, V)]): Iterator[LuceneRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.diff(otherIter))
    }
  }

  private class FullOuterJoinZipper[V2: ClassTag, W: ClassTag](f: (K, Option[V], Option[V2]) => W)
      extends ZipPartitionsFunction[V2, W] with Serializable {
    def apply(
               thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[LuceneRDDPartition[K, V2]])
        : Iterator[LuceneRDDPartition[K, W]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.fullOuterJoin(otherPart)(f))
    }
  }

  private class OtherFullOuterJoinZipper[V2: ClassTag, W: ClassTag](f: (K, Option[V], Option[V2]) => W)
      extends OtherZipPartitionsFunction[V2, W] with Serializable {
    def apply(
               thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[(K, V2)])
        : Iterator[LuceneRDDPartition[K, W]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.fullOuterJoin(otherIter)(f))
    }
  }

  private class JoinZipper[U: ClassTag](f: (K, V, U) => V)
      extends ZipPartitionsFunction[U, V] with Serializable {
    def apply(thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[LuceneRDDPartition[K, U]]): Iterator[LuceneRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.join(otherPart)(f))
    }
  }

  private class OtherJoinZipper[U: ClassTag](f: (K, V, U) => V)
      extends OtherZipPartitionsFunction[U, V] with Serializable {
    def apply(thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[(K, U)]): Iterator[LuceneRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.join(otherIter)(f))
    }
  }

  private class LeftJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, Option[V2]) => V3)
      extends ZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[LuceneRDDPartition[K, V2]]): Iterator[LuceneRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.leftJoin(otherPart)(f))
    }
  }

  private class OtherLeftJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, Option[V2]) => V3)
      extends OtherZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[(K, V2)]): Iterator[LuceneRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.leftJoin(otherIter)(f))
    }
  }

  private class InnerJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, V2) => V3)
      extends ZipPartitionsFunction[V2, V3] with Serializable {
    def apply(
               thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[LuceneRDDPartition[K, V2]])
      : Iterator[LuceneRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.innerJoin(otherPart)(f))
    }
  }

  private class OtherInnerJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, V2) => V3)
      extends OtherZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[LuceneRDDPartition[K, V]], otherIter: Iterator[(K, V2)])
      : Iterator[LuceneRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.innerJoin(otherIter)(f))
    }
  }
}

object LuceneRDD {
  /**
   * Constructs a LuceneRDD from an RDD of pairs, merging duplicate keys arbitrarily.
   */
  def apply[K: ClassTag, V: ClassTag]
      (elems: RDD[(K, V)]): LuceneRDD[K, V] = updatable(elems)

  /**
   * Constructs a LuceneRDD from an RDD of pairs, merging duplicate keys arbitrarily.
   */
  def updatable[K: ClassTag, V: ClassTag]
      (elems: RDD[(K, V)])
    : LuceneRDD[K, V] = updatable[K, V, V](elems, (id, a) => a, (id, a, b) => b)

  /** Constructs an Lucene from an RDD of pairs. */
  def updatable[K: ClassTag, U: ClassTag, V: ClassTag]
      (elems: RDD[(K, U)], z: (K, U) => V, f: (K, V, U) => V)
    : LuceneRDD[K, V] = {
    val elemsPartitioned =
      if (elems.partitioner.isDefined) elems
      else elems.partitionBy(new HashPartitioner(elems.partitions.size))
    val partitions = elemsPartitioned.mapPartitions[LuceneRDDPartition[K, V]](
      iter => Iterator(ElasticLuceneRDDPartition(iter, z, f)),
      preservesPartitioning = true)
    new LuceneRDD(partitions)
  }

}
