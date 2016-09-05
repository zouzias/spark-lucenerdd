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
package org.zouzias.spark.lucenerdd.facets

import org.apache.lucene.document.Document
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.aggregate.SparkFacetResultMonoid
import org.zouzias.spark.lucenerdd.models.SparkFacetResult
import org.zouzias.spark.lucenerdd.partition.{AbstractLuceneRDDPartition, LuceneRDDPartition}
import org.zouzias.spark.lucenerdd.response.LuceneRDDResponse

import scala.reflect.ClassTag

/**
 * LuceneRDD with faceted functionality
 */
class FacetedLuceneRDD[T: ClassTag]
  (override protected val partitionsRDD: RDD[AbstractLuceneRDDPartition[T]])
  extends LuceneRDD[T](partitionsRDD) {

  setName("FacetedLuceneRDD")

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

  /**
   * Aggregates faceted search results using monoidal structure [[SparkFacetResultMonoid]]
   *
   * @param f a function that computes faceted search results per partition
   * @return faceted search results
   */
  private def facetResultsAggregator(f: AbstractLuceneRDDPartition[T] => SparkFacetResult)
  : SparkFacetResult = {
    partitionsRDD.map(f(_)).reduce(SparkFacetResultMonoid.plus)
  }

  /**
   * Faceted query
   *
   * @param searchString
   * @param facetField
   * @param topK
   * @return
   */
  def facetQuery(searchString: String,
                 facetField: String,
                 topK: Int = DefaultTopK,
                 facetNum: Int = DefaultFacetNum
                ): (LuceneRDDResponse, SparkFacetResult) = {
    val aggrTopDocs = partitionMapper(_.query(searchString, topK), topK)
    val aggrFacets = facetResultsAggregator(_.facetQuery(searchString, facetField, facetNum))
    (aggrTopDocs, aggrFacets)
  }

  /**
   * Faceted query with multiple facets
   *
   * @param searchString
   * @param facetFields
   * @param topK
   * @return
   */
  def facetQueries(searchString: String,
                   facetFields: Seq[String],
                   topK: Int = DefaultTopK,
                   facetNum: Int = DefaultFacetNum)
  : (LuceneRDDResponse, Map[String, SparkFacetResult]) = {
    logInfo(s"Faceted query on facet fields ${facetFields.mkString(",")}...")
    val aggrTopDocs = partitionMapper(_.query(searchString, topK), topK)
    val aggrFacets = facetFields.map { case facetField =>
      (facetField, facetResultsAggregator(_.facetQuery(searchString, facetField, facetNum)))
    }.toMap[String, SparkFacetResult]
    (aggrTopDocs, aggrFacets)
  }


}

object FacetedLuceneRDD {

  /** All faceted fields are suffixed with _facet */
  val FacetTextFieldSuffix = "_facet"
  val FacetNumericFieldSuffix = "_numFacet"

  /**
   * Instantiate a FacetedLuceneRDD given an RDD[T]
   *
   * @param elems RDD of type T
   * @tparam T Generic type
   * @return
   */
  def apply[T : ClassTag](elems: RDD[T])
                         (implicit conv: T => Document): FacetedLuceneRDD[T] = {
    val partitions = elems.mapPartitions[AbstractLuceneRDDPartition[T]](
      iter => Iterator(LuceneRDDPartition(iter)),
      preservesPartitioning = true)
    new FacetedLuceneRDD[T](partitions)
  }

  /**
   * Instantiate a FacetedLuceneRDD with an iterable
   *
   * @param elems
   * @param sc
   * @tparam T
   * @return
   */
  def apply[T : ClassTag]
  (elems: Iterable[T])(implicit sc: SparkContext, conv: T => Document)
  : FacetedLuceneRDD[T] = {
    apply(sc.parallelize[T](elems.toSeq))
  }

  /**
   * Instantiate a FacetedLuceneRDD with DataFrame
   *
   * @param dataFrame Spark DataFrame
   * @return
   */
  def apply(dataFrame: DataFrame)
  : FacetedLuceneRDD[Row] = {
    apply(dataFrame.rdd)
  }
}
