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
package org.zouzias.spark.lucenerdd.spatial.shape.rdds

import com.spatial4j.core.shape.Shape
import com.twitter.algebird.{TopK, TopKMonoid}
import org.apache.lucene.document.Document
import org.apache.lucene.spatial.query.SpatialOperation
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.xerial.snappy.Snappy
import org.apache.spark.storage.StorageLevel
import org.zouzias.spark.lucenerdd.config.LuceneRDDConfigurable
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.query.LuceneQueryHelpers
import org.zouzias.spark.lucenerdd.response.{LuceneRDDResponse, LuceneRDDResponsePartition}
import org.zouzias.spark.lucenerdd.versioning.Versionable
import org.zouzias.spark.lucenerdd.spatial.shape.partition.AbstractShapeLuceneRDDPartition
import org.zouzias.spark.lucenerdd.spatial.shape.partition.impl.ShapeLuceneRDDPartition
import org.zouzias.spark.lucenerdd.spatial.shape.rdds.ShapeLuceneRDD.PointType

import scala.reflect.ClassTag
import scala.util.Try

/**
 * ShapeLuceneRDD for geospatial and full-text search queries
 *
 * @param partitionsRDD
 * @tparam K Type containing the geospatial information (must be implicitly converted to [[Shape]])
 * @tparam V Type containing remaining information (must be implicitly converted to [[Document]])
 */
class ShapeLuceneRDD[K: ClassTag, V: ClassTag]
  (private val partitionsRDD: RDD[AbstractShapeLuceneRDDPartition[K, V]])
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
    with LuceneRDDConfigurable {

  logInfo("Instance is created...")
  logInfo(s"Number of partitions: ${partitionsRDD.count()}")

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def cache(): this.type = {
    this.persist(StorageLevel.MEMORY_ONLY)
  }

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    super.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    super.unpersist(blocking)
    this
  }

  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("ShapeLuceneRDD")

  private def partitionMapper(f: AbstractShapeLuceneRDDPartition[K, V] =>
    LuceneRDDResponsePartition): LuceneRDDResponse = {
    new LuceneRDDResponse(partitionsRDD.map(f), SparkScoreDoc.ascending)
  }

  private def linker[T: ClassTag](that: RDD[T], pointFunctor: T => PointType,
    mapper: ( PointType, AbstractShapeLuceneRDDPartition[K, V]) =>
                            Iterable[SparkScoreDoc]): RDD[(T, Array[SparkScoreDoc])] = {
    logDebug("Linker requested")

    val topKMonoid = new TopKMonoid[SparkScoreDoc](MaxDefaultTopKValue)(SparkScoreDoc.ascending)
    val thatWithIndex = that.zipWithIndex().map(_.swap)
    val queries = thatWithIndex.mapValues(pointFunctor).collect()
    val queriesB = partitionsRDD.context.broadcast(queries)

    val resultsByPart: RDD[(Long, TopK[SparkScoreDoc])] = partitionsRDD.mapPartitions {
      case partitions =>
        partitions.flatMap { case partition =>
          queriesB.value.par
            .map{ case (index, (x, y)) =>
            (index, topKMonoid.build(mapper((x, y), partition)))
          }
        }
    }

    logDebug("Merge topK linkage results")
    val results = resultsByPart.reduceByKey(topKMonoid.plus)
    thatWithIndex.join(results).values.map(joined => (joined._1, joined._2.items.toArray))
  }

  /**
   * Link entities based on k-nearest neighbors (Knn)
   *
   * Links this and that based on nearest neighbors, returns Knn
   *
   *
   * @param that An RDD of entities to be linked
   * @param pointFunctor Function that generates a point from each element of other
   * @tparam T A type
   * @return an RDD of Tuple2 that contains the linked results
   *
   * Note: Currently the query coordinates of the other RDD are collected to the driver and
   * broadcast to the workers.
   */
  def linkByKnn[T: ClassTag](that: RDD[T], pointFunctor: T => PointType,
                           topK: Int = DefaultTopK)
  : RDD[(T, Array[SparkScoreDoc])] = {
    logInfo("linkByKnn requested")
    linker[T](that, pointFunctor, (queryPoint, part) =>
      part.knnSearch(queryPoint, topK, LuceneQueryHelpers.MatchAllDocsString))
  }

  /**
   * Link entities if their shapes are within a distance in kilometers (km)
   *
   * Links this and that based on distance threshold
   *
   * @param that An RDD of entities to be linked
   * @param pointFunctor Function that generates a point from each element of other
   * @tparam T A type
   * @return an RDD of Tuple2 that contains the linked results
   *
   * Note: Currently the query coordinates of the other RDD are collected to the driver and
   * broadcast to the workers.
   */
  def linkByRadius[T: ClassTag](that: RDD[T], pointFunctor: T => PointType, radius: Double,
    topK: Int = DefaultTopK, spatialOp: String = SpatialOperation.Intersects.getName)
  : RDD[(T, Array[SparkScoreDoc])] = {
    logInfo("linkByRadius requested")
    linker[T](that, pointFunctor, (queryPoint, part) =>
      part.circleSearch(queryPoint, radius, topK, spatialOp))
  }


  /**
   * Link with DataFrame based on k-nearest neighbors (Knn)
   *
   * Links this and that based on nearest neighbors, returns Knn
   *
   *
   * @param other
   * @param searchQueryGen
   * @param topK
   * @return
   */
  def linkDataFrameByKnn(other: DataFrame, searchQueryGen: Row => PointType,
                         topK: Int = DefaultTopK)
  : RDD[(Row, Array[SparkScoreDoc])] = {
    logInfo("linkDataFrameByKnn requested")
    linkByKnn[Row](other.rdd, searchQueryGen, topK)
  }

  /**
   * Link entities if their shapes are within a distance in kilometers (km)
   *
   * Links this and that based on distance threshold
   *
   * @param other DataFrame of entities to be linked
   * @param pointFunctor Function that generates a point from each element of other
   * @return an RDD of Tuple2 that contains the linked results
   *
   * Note: Currently the query coordinates of the other RDD are collected to the driver and
   * broadcast to the workers.
   */
  def linkDataFrameByRadius(other: DataFrame, pointFunctor: Row => PointType,
                            radius: Double, topK: Int = DefaultTopK,
                            spatialOp: String = SpatialOperation.Intersects.getName)
  : RDD[(Row, Array[SparkScoreDoc])] = {
    logInfo("linkDataFrameByRadius requested")
    linkByRadius[Row](other.rdd, pointFunctor, radius, topK, spatialOp)
  }

  /**
   * K-nearest neighbors search
   *
   * @param queryPoint query point (X, Y)
   * @param k number of nearest neighbor points to return
   * @param searchString Lucene query string
   * @return
   */
  def knnSearch(queryPoint: PointType, k: Int,
                searchString: String = LuceneQueryHelpers.MatchAllDocsString)
  : LuceneRDDResponse = {
    logInfo(s"Knn search with query ${queryPoint} and search string ${searchString}")
    partitionMapper(_.knnSearch(queryPoint, k, searchString))
  }

  /**
   * Search for points within a circle
   *
   * @param center center of circle
   * @param radius radius of circle in kilometers (KM)
   * @param k number of points to return
   * @return
   */
  def circleSearch(center: PointType, radius: Double, k: Int)
  : LuceneRDDResponse = {
    logInfo(s"Circle search with center ${center} and radius ${radius}")
    // Points can only intersect
    partitionMapper(_.circleSearch(center, radius, k,
      SpatialOperation.Intersects.getName))
  }

  /**
   * Spatial search with arbitrary shape
   *
   * @param shapeWKT Shape in WKT format
   * @param k Number of element to return
   * @param operationName
   * @return
   */
  def spatialSearch(shapeWKT: String, k: Int,
                    operationName: String = SpatialOperation.Intersects.getName)
  : LuceneRDDResponse = {
    logInfo(s"Spatial search with shape ${shapeWKT} and operation ${operationName}")
    partitionMapper(_.spatialSearch(shapeWKT, k, operationName))
  }

  /**
   * Spatial search with a single Point
   *
   * @param point
   * @param k
   * @param operationName
   * @return
   */
  def spatialSearch(point: PointType, k: Int,
                    operationName: String)
  : LuceneRDDResponse = {
    logInfo(s"Spatial search with point ${point} and operation ${operationName}")
    partitionMapper(_.spatialSearch(point, k, operationName))
  }

  /**
   * Bounding box search with center and radius
   *
   * @param center given as (x, y)
   * @param radius in kilometers (KM)
   * @param k
   * @param operationName
   * @return
   */
  def bboxSearch(center: PointType, radius: Double, k: Int,
                    operationName: String = SpatialOperation.Intersects.getName)
  : LuceneRDDResponse = {
    logInfo(s"Bounding box with center ${center}, radius ${radius}, k = ${k}")
    partitionMapper(_.bboxSearch(center, radius, k, operationName))
  }

  /**
   * Bounding box search with rectangle
   * @param lowerLeft Lower left corner
   * @param upperRight Upper right corner
   * @param k Number of results
   * @param operationName Intersect, contained, etc.
   * @return
   */
  def bboxSearch(lowerLeft: PointType, upperRight: PointType, k: Int,
                 operationName: String)
  : LuceneRDDResponse = {
    logInfo(s"Bounding box with lower left ${lowerLeft}, upper right ${upperRight} and k = ${k}")
    partitionMapper(_.bboxSearch(lowerLeft, upperRight, k, operationName))
  }

  override def count(): Long = {
    logInfo("Count requested")
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** RDD compute method. */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    firstParent[AbstractShapeLuceneRDDPartition[K, V]].iterator(part, context).next.iterator
  }

  def filter(pred: (K, V) => Boolean): ShapeLuceneRDD[K, V] = {
    val newPartitionRDD = partitionsRDD.mapPartitions(partition =>
      partition.map(_.filter(pred)), preservesPartitioning = true
    )
    new ShapeLuceneRDD(newPartitionRDD)
  }

  def exists(elem: K): Boolean = {
    partitionsRDD.map(_.isDefined(elem)).collect().exists(x => x)
  }

  def close(): Unit = {
    logInfo(s"Closing...")
    partitionsRDD.foreach(_.close())
  }
}

object ShapeLuceneRDD extends Versionable {

  /** Type for a point */
  type PointType = (Double, Double)

  /**
   * Instantiate a ShapeLuceneRDD given an RDD[T]
   *
   * @param elems RDD of type T
   * @return
   */
  def apply[K: ClassTag, V: ClassTag](elems: RDD[(K, V)])
                                     (implicit shapeConv: K => Shape,
                                      docConverter: V => Document)
  : ShapeLuceneRDD[K, V] = {
    val partitions = elems.mapPartitions[AbstractShapeLuceneRDDPartition[K, V]](
      iter => Iterator(ShapeLuceneRDDPartition[K, V](iter)),
      preservesPartitioning = true)
    new ShapeLuceneRDD(partitions)
  }

  def apply[K: ClassTag, V: ClassTag](elems: Dataset[(K, V)])
                                     (implicit shapeConv: K => Shape,
                                      docConverter: V => Document)
  : ShapeLuceneRDD[K, V] = {
    val partitions = elems.rdd.mapPartitions[AbstractShapeLuceneRDDPartition[K, V]](
      iter => Iterator(ShapeLuceneRDDPartition[K, V](iter)),
      preservesPartitioning = true)
    new ShapeLuceneRDD(partitions)
  }

  /**
   * Instantiate [[ShapeLuceneRDD]] from DataFrame with spatial column (shape format)
   *
   * Shape format can be one of ShapeIO.GeoJSON, ShapeIO.LEGACY, ShapeIO.POLY, ShapeIO.WKT
   *
   * {{
   *  val countries = spark.read.parquet("data/countries-bbox.parquet")
   *  val lucene = ShapeLuceneRDD(counties, "shape")
   *
   * }}
   * @param df Input dataframe containing Shape as String field named "shapeField"
   * @param shapeField Name of DataFrame column that contains Shape as String, i.e., WKT
   * @param shapeConv Implicit convertion for spatial / shape
   * @param docConverter Implicit conversion for Lucene Document
   * @return
   */
  def apply(df : DataFrame, shapeField: String)
                                     (implicit shapeConv: String => Shape,
                                      docConverter: Row => Document)
  : ShapeLuceneRDD[String, Row] = {
    val partitions = df.rdd.map(row => (row.getString(row.fieldIndex(shapeField)), row))
      .mapPartitions[AbstractShapeLuceneRDDPartition[String, Row]](
      iter => Iterator(ShapeLuceneRDDPartition[String, Row](iter)),
      preservesPartitioning = true)
    new ShapeLuceneRDD(partitions)
  }
}
