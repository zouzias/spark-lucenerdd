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
package org.zouzias.spark.lucenerdd.spatial.point

import com.spatial4j.core.shape.Shape
import org.apache.lucene.document.Document
import org.apache.lucene.spatial.query.SpatialOperation
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.zouzias.spark.lucenerdd.aggregate.SparkScoreDocAggregatable
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.spatial.point.partition.{AbstractPointLuceneRDDPartition, PointLuceneRDDPartition}

import scala.reflect.ClassTag


class PointLuceneRDD[K: ClassTag, V: ClassTag]
  (private val partitionsRDD: RDD[AbstractPointLuceneRDDPartition[K, V]])
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
    with SparkScoreDocAggregatable {

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
   * Aggregates Lucene documents using monoidal structure, i.e., [[SparkDocTopKMonoid]]
   *
   * TODO: Move to aggregations
   * @param f
   * @return
   */
  private def docResultsAggregator
  (f: AbstractPointLuceneRDDPartition[K, V] => Iterable[SparkScoreDoc])
  : List[SparkScoreDoc] = {
    val parts = partitionsRDD.map(f(_)).map(SparkDocTopKMonoid.build(_))
    parts.reduce(SparkDocTopKMonoid.plus(_, _)).items
  }

  /**
   * K-nearest neighbors search
   *
   * @param queryPoint query point
   * @param k number of nearest points to return
   * @return
   */
  def knnSearch(queryPoint: (Double, Double), k: Int): Iterable[SparkScoreDoc] = {
    docResultsAggregator(_.knnSearch(queryPoint, k).reverse).reverse.take(k)
  }

  /**
   * Search for points within a circle
   *
   * @param center center of circle
   * @param radius radius of circle in kilometers (KM)
   * @param k number of points to return
   * @return
   */
  def circleSearch(center: (Double, Double), radius: Double, k: Int)
  : Iterable[SparkScoreDoc] = {
    // Points can only intersect
    docResultsAggregator(_.circleSearch(center, radius, k,
      SpatialOperation.Intersects.getName)).take(k)
  }

  /**
   * Spatial search with arbitrary shape
   *
   * @param shapeWKT
   * @param k
   * @param operationName
   * @return
   */
  def spatialSearch(shapeWKT: String, k: Int,
                    operationName: String = SpatialOperation.Intersects.getName)
  : Iterable[SparkScoreDoc] = {
    docResultsAggregator(_.spatialSearch(shapeWKT, k, operationName)).take(k)
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** RDD compute method. */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    firstParent[AbstractPointLuceneRDDPartition[K, V]].iterator(part, context).next.iterator
  }

  def filter(pred: (K, V) => Boolean): PointLuceneRDD[K, V] = {
    val newPartitionRDD = partitionsRDD.mapPartitions(partition =>
      partition.map(_.filter(pred)), preservesPartitioning = true
    )
    new PointLuceneRDD(newPartitionRDD)
  }

  def exists(elem: K): Boolean = {
    partitionsRDD.map(_.isDefined(elem)).collect().exists(x => x)
  }

  def close(): Unit = {
    partitionsRDD.foreach(_.close())
  }
}

object PointLuceneRDD {

  /**
   * Instantiate a PointLuceneRDD given an RDD[T]
   *
   * @param elems RDD of type T
   * @return
   */
  def apply[K: ClassTag, V: ClassTag](elems: RDD[(K, V)])
                                     (implicit shapeConv: K => Shape,
                                      docConverter: V => Document)
  : PointLuceneRDD[K, V] = {
    val partitions = elems.mapPartitions[AbstractPointLuceneRDDPartition[K, V]](
      iter => Iterator(PointLuceneRDDPartition[K, V](iter)),
      preservesPartitioning = true)
    new PointLuceneRDD(partitions)
  }

  /**
   * Instantiate a PointLuceneRDD with an iterable
   *
   * @param elems
   * @param sc
   * @return
   */
  def apply[K: ClassTag, V: ClassTag]
  (elems: Iterable[(K, V)])(implicit sc: SparkContext, shapeConv: K => Shape,
                            docConverter: V => Document): PointLuceneRDD[K, V] = {
    apply(sc.parallelize[(K, V)](elems.toSeq))
  }
}
