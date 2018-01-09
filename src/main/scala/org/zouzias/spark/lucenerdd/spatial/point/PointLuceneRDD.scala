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

import com.twitter.algebird._
import org.apache.lucene.document.Document
import org.apache.lucene.spatial.query.SpatialOperation
import org.apache.spark.{OneToOneDependency, Partition, Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.zouzias.spark.lucenerdd.aggregate.{MaxPointMonoid, MinPointMonoid}
import org.zouzias.spark.lucenerdd.analyzers.AnalyzerConfigurable
import org.zouzias.spark.lucenerdd.config.ShapeLuceneRDDConfigurable
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.query.{LuceneQueryHelpers, SimilarityConfigurable}
import org.zouzias.spark.lucenerdd.response.{LuceneRDDResponse, LuceneRDDResponsePartition}
import org.zouzias.spark.lucenerdd.spatial.commons.SpatialByXPartitioner
import org.zouzias.spark.lucenerdd.spatial.point.PointLuceneRDD.PointType
import org.zouzias.spark.lucenerdd.spatial.point.partition.{AbstractPointLuceneRDDPartition, PointLuceneRDDPartition}
import org.zouzias.spark.lucenerdd.versioning.Versionable

import scala.reflect.ClassTag

class PointLuceneRDD[V: ClassTag]
  (private val partitionsRDD: RDD[AbstractPointLuceneRDDPartition[V]],
   val indexAnalyzerName: String,
   val queryAnalyzerName: String,
   val similarity: String)
  extends RDD[(PointType, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
    with ShapeLuceneRDDConfigurable {

  logInfo("Instance is created...")
  logInfo(s"Number of partitions: ${partitionsRDD.count()}")
  setName("PointLuceneRDD")

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

  private def partitionMapper(f: AbstractPointLuceneRDDPartition[V] =>
    LuceneRDDResponsePartition): LuceneRDDResponse = {
    new LuceneRDDResponse(partitionsRDD.map(f), SparkScoreDoc.ascending)
  }

  def linkByRadius[T: ClassTag](that: RDD[T],
                                       pointFunctor: T => PointType,
                                       topK: Int = DefaultTopK,
                                       radius: Double,
                                       linkerMethod: String = getShapeLinkerMethod)
  : RDD[(T, Array[SparkScoreDoc])] = {
    logInfo("linkByRadius requested")

    val partitioner = SpatialByXPartitioner(boundsPerPartition()
      .map(x => (x._1._1, x._2._1)).collect()
    )

    val queries = that.zipWithIndex().map(_.swap)
    val queriesPart = queries.mapValues(pointFunctor).partitionBy(partitioner)

    val coGrouped = partitionsRDD.zipPartitions(queriesPart, preservesPartitioning = true)
      {case tp =>
        val queries = tp._2.toArray

        tp._1.flatMap{lucene =>
          queries.map{q =>
            (q._1,
              lucene.circleSearch(q._2, radius, topK, linkerMethod).toArray)
          }
        }
      }

    queries.join(coGrouped).values
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
   * @param operationName Spatial operation name, i.e., intersection, contained, etc.
   * @return
   */
  def spatialSearch(shapeWKT: String, k: Int,
                    operationName: String = SpatialOperation.Intersects.getName)
  : LuceneRDDResponse = {
    partitionMapper(_.spatialSearch(shapeWKT, k, operationName))
  }

  /**
   * Bounding box search with center and radius
   *
   * @param center given as (x, y)
   * @param radius in kilometers (KM)
   * @param k
   * @param operationName Spatial operation name, i.e., intersection, contained, etc.
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
   * @param operationName Spatial operation name, i.e., intersection, contained, etc.
   * @return
   */
  def bboxSearch(lowerLeft: PointType,
                 upperRight: PointType,
                 k: Int,
                 operationName: String)
  : LuceneRDDResponse = {
    logInfo(s"Bounding box with lower left ${lowerLeft}, upper right ${upperRight} and k = ${k}")
    partitionMapper(_.bboxSearch(lowerLeft, upperRight, k, operationName))
  }

  /**
    * Returns the smallest enclosing axis aligned bounding box per partition
    * @return
    */
  def boundsPerPartition(): RDD[(PointType, PointType)] = {
    logInfo("boundsPerPartition requested")
    partitionsRDD.map(_.bounds())
  }

  def bounds(): (PointType, PointType) = {
    logInfo("bounds requested")
    boundsPerPartition()
      .reduce { case (x, y) =>
        (MinPointMonoid.plus(x._1, y._1), MaxPointMonoid.plus(x._2, y._2))
      }
  }

  /**
    * Returns an x-axis spatial [[Partitioner]] that is used in link related methods
    * @return Spark [[Partitioner]]
    */
  def spatialPartitioner(): Partitioner = {
    val bpp = boundsPerPartition().map(x => (x._1._1, x._2._1)).collect()
    SpatialByXPartitioner(bpp)
  }

  override def count(): Long = {
    logInfo("Count requested")
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** RDD compute method. */
  override def compute(part: Partition, context: TaskContext): Iterator[(PointType, V)] = {
    firstParent[AbstractPointLuceneRDDPartition[V]].iterator(part, context).next.iterator
  }

  def filter(pred: (PointType, V) => Boolean): PointLuceneRDD[V] = {
    val newPartitionRDD = partitionsRDD.mapPartitions(partition =>
      partition.map(_.filter(pred)), preservesPartitioning = true
    )
    new PointLuceneRDD[V](newPartitionRDD, indexAnalyzerName, queryAnalyzerName, similarity)
  }

  def exists(point: PointType): Boolean = {
    partitionsRDD.map(_.isDefined(point)).collect().exists(x => x)
  }

  def close(): Unit = {
    logInfo(s"Closing...")
    partitionsRDD.foreach(_.close())
  }
}


object PointLuceneRDD extends Versionable
  with AnalyzerConfigurable
  with SimilarityConfigurable {

  /** Type for a point */
  type PointType = (Double, Double)

  /**
    * Instantiate a ShapeLuceneRDD given an RDD[T]
    *
    * @param elems RDD of type T
    * @param indexAnalyzer Index Analyzer name
    * @param queryAnalyzer Query Analyzer name
    * @param similarity Lucene scoring similarity, i.e., BM25 or TF-IDF
    * @return
    */
  def apply[V: ClassTag](elems: RDD[(PointType, V)],
                                      indexAnalyzer: String,
                                      queryAnalyzer: String,
                                      similarity: String)
                                     (implicit docConverter: V => Document)
  : PointLuceneRDD[V] = {
    val partitions = elems
      .sortBy(item => item._1._1) // Sort by x-axis
      .mapPartitions[AbstractPointLuceneRDDPartition[V]](
      iter => Iterator(PointLuceneRDDPartition[V](iter, indexAnalyzer, queryAnalyzer)),
      preservesPartitioning = true)
    new PointLuceneRDD(partitions, indexAnalyzer, queryAnalyzer, similarity)
  }

  def apply[V: ClassTag](elems: RDD[(PointType, V)])
                                     (implicit docConverter: V => Document)
  : PointLuceneRDD[V] = {
    apply[V](elems, getOrElseEn(IndexAnalyzerConfigName), getOrElseEn(QueryAnalyzerConfigName),
      getOrElseClassic())
  }

  /**
    * Constructor for [[Dataset]]
    */
  def apply[V: ClassTag](elems: Dataset[(PointType, V)],
                                      indexAnalyzer: String,
                                      queryAnalyzer: String,
                                      similarity: String)
                                     (implicit docConverter: V => Document)
  : PointLuceneRDD[V] = {
    val partitions = elems.rdd
        .sortBy(item => item._1._1) // Sort by x-axis
        .mapPartitions[AbstractPointLuceneRDDPartition[V]](
          iter => Iterator(PointLuceneRDDPartition[V](iter, indexAnalyzer, queryAnalyzer)),
          preservesPartitioning = true)
    new PointLuceneRDD(partitions, indexAnalyzer, queryAnalyzer, similarity)
  }

  /**
    * Constructor for [[Dataset]]
    */
  def apply[V: ClassTag](elems: Dataset[(PointType, V)])
                                     (implicit docConverter: V => Document)
  : PointLuceneRDD[V] = {
    apply[V](elems, getOrElseEn(IndexAnalyzerConfigName), getOrElseEn(QueryAnalyzerConfigName),
      getOrElseClassic())
  }

  /**
    * Instantiate [[PointLuceneRDD]] from DataFrame with spatial column (shape format)
    *
    * Shape format can be one of ShapeIO.GeoJSON, ShapeIO.LEGACY, ShapeIO.POLY, ShapeIO.WKT
    *
    * {{
    *  val countries = spark.read.parquet("data/countries-bbox.parquet")
    *  val lucene = ShapeLuceneRDD(counties, "shape")
    *
    * }}
    * @param df Input DataFrame containing Shape as String field named "shapeField"
    * @param shapeField Name of DataFrame column that contains Shape as String, i.e., WKT
    * @param shapeConv Implicit conversion for spatial / shape
    * @param docConverter Implicit conversion for Lucene Document
    * @return
    */
  def apply(df: DataFrame,
            shapeField: String)
           (implicit shapeConv: String => PointType, docConverter: Row => Document)
  : PointLuceneRDD[Row] = {
    apply(df, shapeField,
      getOrElseEn(IndexAnalyzerConfigName), getOrElseEn(QueryAnalyzerConfigName),
      getOrElseClassic())
  }


  def apply(df: DataFrame,
            shapeField: String,
            indexAnalyzer: String,
            queryAnalyzer: String,
            similarity: String)
           (implicit shapeConv: String => PointType, docConverter: Row => Document)
  : PointLuceneRDD[Row] = {
    val partitions = df.rdd.map(row => (shapeConv(row.getString(row.fieldIndex(shapeField))), row))
        .sortBy(item => item._1._1) // Sort by x-axis
      .mapPartitions[AbstractPointLuceneRDDPartition[Row]](
      iter => Iterator(PointLuceneRDDPartition[Row](iter, indexAnalyzer, queryAnalyzer)),
      preservesPartitioning = true)
    new PointLuceneRDD(partitions, indexAnalyzer, queryAnalyzer, similarity)
  }

  /** Algebird bounding box aggregator */

  val boundingBoxMonoid = new Tuple2Monoid()(MinPointMonoid, MaxPointMonoid)
}
