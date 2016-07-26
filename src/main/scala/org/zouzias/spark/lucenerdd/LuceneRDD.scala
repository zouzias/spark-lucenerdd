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

import com.twitter.algebird.TopK
import org.apache.lucene.document.Document
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.lucene.search.Query
import org.apache.spark.sql.{DataFrame, Row}
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
  with SparkScoreDocAggregatable
  with Logging{

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

  /** Set the name for the RDD; By default set to "LuceneRDD" */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("LuceneRDD")


  /**
   * Aggregates Lucene documents using monoidal structure, i.e., [[SparkDocTopKMonoid]]
   *
   * @param f
   * @return
   */
  private def docResultsAggregator(f: AbstractLuceneRDDPartition[T] => Iterable[SparkScoreDoc])
  : Iterable[SparkScoreDoc] = {
    val parts = partitionsRDD.map(f(_)).map(x => SparkDocTopKMonoid.build(x))
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
    logDebug("Fields requested")
    partitionsRDD.first().fields()
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
      (facetField, facetResultsAggregator(_.facetQuery(searchString, facetField, facetNum)))
    }.toMap[String, SparkFacetResult]
    (aggrTopDocs, aggrFacets)
  }

  /**
   * Entity linkage via Lucene query over all elements of an RDD.
   *
   * @param other DataFrame to be linked
   * @param searchQueryGen Function that generates a search query for each element of other
   * @param topK
   * @return an RDD of Tuple2 that contains the linked search Lucene documents in the second
   */
  def linkDataFrame(other: DataFrame, searchQueryGen: Row => String, topK: Int = DefaultTopK)
  : RDD[(Row, List[SparkScoreDoc])] = {
    logDebug("LinkDataFrame requested")
    link[Row](other.rdd, searchQueryGen, topK)
  }

  /**
   * Entity linkage via Lucene query over all elements of an RDD.
   *
   * @param other RDD to be linked
   * @param searchQueryGen Function that generates a search query for each element of other
   * @tparam T1 A type
   * @return an RDD of Tuple2 that contains the linked search Lucene documents in the second
   *
   * Note: Currently the query strings of the other RDD are collected to the driver and
   * broadcast to the workers.
   */
  def link[T1: ClassTag](other: RDD[T1], searchQueryGen: T1 => String, topK: Int = DefaultTopK)
    : RDD[(T1, List[SparkScoreDoc])] = {
    logDebug("Linkage requested")
    val queries = other.map(searchQueryGen).collect()
    val queriesB = partitionsRDD.context.broadcast(queries)

    val resultsByPart: RDD[(Long, TopK[SparkScoreDoc])] = partitionsRDD.flatMap {
      case partition => queriesB.value.zipWithIndex.map { case (qr, index) =>
        val results = partition.query(qr, topK).map(x => SparkDocTopKMonoid.build(x))
        if (results.nonEmpty) {
          (index.toLong, results.reduce( (x, y) => SparkDocTopKMonoid.plus(x, y)))
        }
        else {
          (index.toLong, SparkDocTopKMonoid.zero)
        }
      }
    }

    val results = resultsByPart.reduceByKey( (x, y) => SparkDocTopKMonoid.plus(x, y))
    other.zipWithIndex.map(_.swap).join(results)
      .map{ case (_, joined) => (joined._1, joined._2.items)}
  }

  /**
   * Entity linkage via Lucene query over all elements of an RDD.
   *
   * @param other RDD to be linked
   * @param searchQueryGen Function that generates a Lucene Query object for each element of other
   * @tparam T1 A type
   * @return an RDD of Tuple2 that contains the linked search Lucene Document in the second position
   */
  def linkByQuery[T1: ClassTag](other: RDD[T1],
                                searchQueryGen: T1 => Query, topK: Int = DefaultTopK)
  : RDD[(T1, List[SparkScoreDoc])] = {
    logDebug("LinkByQuery requested")
    def typeToQueryString = (input: T1) => {
      searchQueryGen(input).toString
    }

    link[T1](other, typeToQueryString, topK)
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
    logDebug("Term search requested")
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
    logDebug("Prefix search requested")
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
    logDebug("Fuzzy search requested")
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
    logDebug("Phrase query requested")
    docResultsAggregator(_.phraseQuery(fieldName, query, topK))
  }

  override def count(): Long = {
    logDebug("Count action requested")
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
  def apply[T : ClassTag](elems: RDD[T])
    (implicit conv: T => Document): LuceneRDD[T] = {
    log.info("LuceneRDD is created")
    val partitions = elems.mapPartitions[AbstractLuceneRDDPartition[T]](
      iter => Iterator(LuceneRDDPartition(iter)),
      preservesPartitioning = true)
    new LuceneRDD[T](partitions)
  }

  /**
   * Instantiate a LuceneRDD with an iterable
   *
   * @param elems
   * @param sc
   * @tparam T
   * @return
   */
  def apply[T : ClassTag]
  (elems: Iterable[T])(implicit sc: SparkContext, conv: T => Document)
  : LuceneRDD[T] = {
    apply(sc.parallelize[T](elems.toSeq))
  }

  /**
   * Instantiate a LuceneRDD with DataFrame
   *
   * @param dataFrame Spark DataFrame
   * @return
   */
  def apply(dataFrame: DataFrame)
  : LuceneRDD[Row] = {
    apply(dataFrame.rdd)
  }
}
