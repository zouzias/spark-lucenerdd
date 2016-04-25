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
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.zouzias.spark.rdd.lucenerdd.impl.RamLuceneRDDPartition
import org.zouzias.spark.rdd.lucenerdd.utils.SerializedDocument

import scala.reflect.ClassTag

/**
 *
 * @tparam T
 */
class LuceneRDD[T: ClassTag](private val partitionsRDD: RDD[LuceneRDDPartition[T]])
  extends RDD[T](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  /** Top K Documents */
  private val TopK = 10

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

  /**
   * Document results aggregator
   *
   * @param f
   * @return
   */
  private def docResultsAggregator(f: LuceneRDDPartition[T] => Iterable[SerializedDocument])
  : Iterable[SerializedDocument] = {
    partitionsRDD.flatMap(f(_)).toLocalIterator.toIterable
  }

  /**
   * Lucene generic query
   *
   * @param doc
   * @return
   */
  def exists(doc: Map[String, String]): Boolean = {
    partitionsRDD.map { case part =>
      part.query(doc)
    }.toLocalIterator.forall(x => x)
  }

  /**
   * Lucene generic query
   *
   * @param q
   * @param topK
   * @return
   */
  def query(q: Query, topK: Int = TopK): Iterable[SerializedDocument] = {
    docResultsAggregator(_.query(q, topK))
  }

  /**
   * Lucene term query
   *
   * @param fieldName
   * @param query
   * @param topK
   * @return
   */
  def termQuery(fieldName: String, query: String,
                topK: Int = TopK): Iterable[SerializedDocument] = {
    docResultsAggregator(_.termQuery(fieldName, query, topK))
  }

  /**
   * Lucene prefix query
   *
   * @param fieldName
   * @param query
   * @param topK
   * @return
   */
  def prefixQuery(fieldName: String, query: String,
                  topK: Int = TopK): Iterable[SerializedDocument] = {
    docResultsAggregator(_.prefixQuery(fieldName, query, topK))
  }

  /**
   * Lucene fuzzy query
   *
   * @param fieldName
   * @param query
   * @param maxEdits
   * @param topK
   * @return
   */
  def fuzzyQuery(fieldName: String, query: String,
                 maxEdits: Int, topK: Int = TopK): Iterable[SerializedDocument] = {
    docResultsAggregator(_.fuzzyQuery(fieldName, query, maxEdits, topK))
  }

  /**
   * Lucene phrase Query
   *
   * @param fieldName
   * @param query
   * @param topK
   * @return
   */
  def phraseQuery(fieldName: String, query: String,
                  topK: Int = TopK): Iterable[SerializedDocument] = {
    docResultsAggregator(_.phraseQuery(fieldName, query, topK))
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[T] = {
    firstParent[LuceneRDDPartition[T]].iterator(part, context).next.iterator
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
}

object LuceneRDD {

  /**
   * Constructs a LuceneRDD from an RDD of pairs, merging duplicate keys arbitrarily.
   */
  def apply[T: ClassTag](elems: RDD[T])(implicit docConversion: T => Document): LuceneRDD[T] = {
    val partitions = elems.mapPartitions[LuceneRDDPartition[T]](
      iter => Iterator(RamLuceneRDDPartition(iter)),
      preservesPartitioning = true)
    new LuceneRDD(partitions)
  }

  def apply[T: ClassTag]
  (elems: Iterable[T])(implicit docConversion: T => Document, sc: SparkContext): LuceneRDD[T] = {
    apply(sc.parallelize[T](elems.toSeq))
  }
}