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

package org.zouzias.spark.lucenerdd

import org.apache.lucene.document.Document
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.zouzias.spark.lucenerdd.aggregate.{SparkFacetResultMonoid, SparkScoreDocAggregatable}
import org.zouzias.spark.lucenerdd.partition.{AbstractLuceneRDDPartition, LuceneRDDPartition}
import org.zouzias.spark.lucenerdd.models.{SparkFacetResult, SparkScoreDoc}

import scala.reflect.ClassTag

/**
 * Spark RDD with Lucene's query capabilities (term, prefix, fuzzy, phrase query)
 *
 * @tparam T
 */
class LuceneRDD[T: ClassTag](protected val partitionsRDD: RDD[AbstractLuceneRDDPartition[T]])
  extends RDD[T](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
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
   * @param f
   * @return
   */
  private def docResultsAggregator(f: AbstractLuceneRDDPartition[T] => Iterable[SparkScoreDoc])
  : Iterable[SparkScoreDoc] = {
    val parts = partitionsRDD.map(f(_)).map(SparkDocTopKMonoid.build(_))
    parts.reduce(SparkDocTopKMonoid.plus(_, _)).items
  }

  /**
   * Aggregates faceted search results using monoidal structure [[SparkFacetResultMonoid]]
   *
   * @param f a function that computes faceted search results per partition
   * @return faceted search results
   */
  private def facetResultsAggregator(f: AbstractLuceneRDDPartition[T] => SparkFacetResult)
  : SparkFacetResult = {
    partitionsRDD.map(f(_)).reduce( (x, y) => SparkFacetResultMonoid.plus(x, y))
  }


  /**
   * Return all document fields
   *
   * @return
   */
  def fields(): Set[String] = {
    partitionsRDD.map(_.fields()).reduce(_ ++ _)
  }

  /**
   * Lucene generic query
   *
   * @param doc
   * @return
   */
  def exists(doc: Map[String, String]): Boolean = {
    docResultsAggregator(_.multiTermQuery(doc, DefaultTopK)).nonEmpty
  }

  /**
   * Generic query using Lucene's query parser
   *
   * @param searchString  Query String
   * @param topK
   * @return
   */
  def query(searchString: String,
            topK: Int = DefaultTopK): Iterable[SparkScoreDoc] = {
    docResultsAggregator(_.query(searchString, topK))
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
  ): (Iterable[SparkScoreDoc], SparkFacetResult) = {
    val aggrTopDocs = docResultsAggregator(_.query(searchString, topK))
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
  : (Iterable[SparkScoreDoc], Map[String, SparkFacetResult]) = {
    val aggrTopDocs = docResultsAggregator(_.query(searchString, topK))
    val aggrFacets = facetFields.map { case facetField =>
      facetField -> facetResultsAggregator(_.facetQuery(searchString, facetField, facetNum))
    }.toMap[String, SparkFacetResult]
    (aggrTopDocs, aggrFacets)
  }

  /**
   * Entity linkage via Lucene query over all elements of an RDD.
   *
   * @param other RDD to be linked
   * @param searchQueryGen Function that generates a search query for each element of other
   * @tparam T1 A type
   * @return an RDD of Tuple2 that contains the linked search Lucene Document in the second position
   */
  def link[T1: ClassTag](other: RDD[T1], searchQueryGen: T1 => String, topK: Int = DefaultTopK)
    (implicit sc: SparkContext)
    : RDD[(T1, List[SparkScoreDoc])] = {
    /*
    val resultsByPart = partitionsRDD.flatMap(_.queries(queries.value, topK))
        .mapValues(x => SparkDocTopKMonoid.build(x))
    val aggTopDocs = resultsByPart.reduceByKey(SparkDocTopKMonoid.plus(_, _))
      .map{ case (_, topKMonoid) => topKMonoid.items}

    other.zip(aggTopDocs)
    */

    // Collect searchString Lucene Queries to Spark driver
    val queries = sc.broadcast(other.map(searchQueryGen).collect())
    val results = queries.value.map{ case query =>
      docResultsAggregator(_.query(query, topK)).toList
    }
    val rdd = sc.parallelize(results)
    other.zip(rdd)
  }

  /**
   * Lucene term query
   *
   * @param fieldName Name of field
   * @param query Term to search on
   * @param topK Number of documents to return
   * @return
   */
  def termQuery(fieldName: String, query: String,
                topK: Int = DefaultTopK): Iterable[SparkScoreDoc] = {
    docResultsAggregator(_.termQuery(fieldName, query, topK))
  }

  /**
   * Lucene prefix query
   *
   * @param fieldName Name of field
   * @param query Prefix query text
   * @param topK Number of documents to return
   * @return
   */
  def prefixQuery(fieldName: String, query: String,
                  topK: Int = DefaultTopK): Iterable[SparkScoreDoc] = {
    docResultsAggregator(_.prefixQuery(fieldName, query, topK))
  }

  /**
   * Lucene fuzzy query
   *
   * @param fieldName Name of field
   * @param query Query text
   * @param maxEdits Fuzziness, edit distance
   * @param topK Number of documents to return
   * @return
   */
  def fuzzyQuery(fieldName: String, query: String,
                 maxEdits: Int, topK: Int = DefaultTopK): Iterable[SparkScoreDoc] = {
    docResultsAggregator(_.fuzzyQuery(fieldName, query, maxEdits, topK))
  }

  /**
   * Lucene phrase Query
   *
   * @param fieldName Name of field
   * @param query Query text
   * @param topK Number of documents to return
   * @return
   */
  def phraseQuery(fieldName: String, query: String,
                  topK: Int = DefaultTopK): Iterable[SparkScoreDoc] = {
    docResultsAggregator(_.phraseQuery(fieldName, query, topK))
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** RDD compute method. */
  override def compute(part: Partition, context: TaskContext): Iterator[T] = {
    firstParent[AbstractLuceneRDDPartition[T]].iterator(part, context).next.iterator
  }

  override def filter(pred: T => Boolean): LuceneRDD[T] = {
    val newPartitionRDD = partitionsRDD.mapPartitions(partition =>
      partition.map(_.filter(pred)), preservesPartitioning = true
    )
    new LuceneRDD(newPartitionRDD)
  }

  def exists(elem: T): Boolean = {
    partitionsRDD.map(_.isDefined(elem)).collect().exists(x => x)
  }

  def close(): Unit = {
    partitionsRDD.foreach(_.close())
  }
}

object LuceneRDD {

  /** All faceted fields are suffixed with _facet */
  val FacetTextFieldSuffix = "_facet"
  val FacetNumericFieldSuffix = "_numFacet"

  /**
   * Instantiate a LuceneRDD given an RDD[T]
   *
   * @param elems RDD of type T
   * @tparam T Generic type
   * @return
   */
  def apply[T <% Document : ClassTag](elems: RDD[T]): LuceneRDD[T] = {
    val partitions = elems.mapPartitions[AbstractLuceneRDDPartition[T]](
      iter => Iterator(LuceneRDDPartition(iter)),
      preservesPartitioning = true)
    new LuceneRDD(partitions)
  }

  /**
   * Instantiate a LuceneRDD with an iterable
   *
   * @param elems
   * @param sc
   * @tparam T
   * @return
   */
  def apply[T <% Document : ClassTag]
  (elems: Iterable[T])(implicit sc: SparkContext): LuceneRDD[T] = {
    apply(sc.parallelize[T](elems.toSeq))
  }
}
