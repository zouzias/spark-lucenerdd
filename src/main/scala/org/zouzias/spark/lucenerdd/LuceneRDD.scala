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

import com.twitter.algebird.{TopK, TopKMonoid}
import org.apache.lucene.document.Document
import org.zouzias.spark.lucenerdd.config.LuceneRDDConfigurable
import org.zouzias.spark.lucenerdd.response.{LuceneRDDResponse, LuceneRDDResponsePartition}
import org.apache.spark.rdd.RDD
import org.apache.lucene.search.Query
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.xerial.snappy.Snappy
import org.zouzias.spark.lucenerdd.partition.{AbstractLuceneRDDPartition, LuceneRDDPartition}
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.utils.GzipUtils

import scala.reflect.ClassTag

/**
 * Spark RDD with Lucene's query capabilities (term, prefix, fuzzy, phrase query)
 *
 * @tparam T
 */
class LuceneRDD[T: ClassTag](protected val partitionsRDD: RDD[AbstractLuceneRDDPartition[T]])
  extends RDD[T](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
  with LuceneRDDConfigurable {

  logInfo("Instance is created...")

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def cache(): LuceneRDD.this.type = {
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
   * Maps partition results
   *
   * @param f Function to apply on each partition / distributed index
   * @param k number of documents to return
   * @return
   */
  protected def partitionMapper(f: AbstractLuceneRDDPartition[T] => LuceneRDDResponsePartition,
                                k: Int): LuceneRDDResponse = {
    new LuceneRDDResponse(partitionsRDD.map(f), SparkScoreDoc.descending)
  }


  /**
   * Return all document fields
   *
   * @return
   */
  def fields(): Set[String] = {
    logInfo("Fields requested")
    partitionsRDD.map(_.fields()).reduce(_ ++ _)
  }

  /**
   * Lucene generic query
   *
   * @param doc
   * @return
   */
  def exists(doc: Map[String, String]): Boolean = {
    !partitionMapper(_.multiTermQuery(doc, DefaultTopK), DefaultTopK).isEmpty()
  }

  /**
   * Generic query using Lucene's query parser
   *
   * @param searchString  Query String
   * @param topK
   * @return
   */
  def query(searchString: String,
            topK: Int = DefaultTopK): LuceneRDDResponse = {
    partitionMapper(_.query(searchString, topK), topK)
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
  : RDD[(Row, Array[SparkScoreDoc])] = {
    logInfo("LinkDataFrame requested")
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
    : RDD[(T1, Array[SparkScoreDoc])] = {
    logInfo("Linkage requested")
    val topKMonoid = new TopKMonoid[SparkScoreDoc](topK)(SparkScoreDoc.descending)
    logInfo("Collecting query points to driver")
    val otherWithIndex = other.zipWithIndex().map(_.swap)
    val queriesString = otherWithIndex.mapValues(searchQueryGen).map(x => s"${x._1}/${x._2}")
      .reduce{ case (x, y) =>
        s"${x}${LuceneRDD.Separator}${y}"}
    val queries = Snappy.compress(queriesString)
    logInfo(s"Uncompressed queries: ${queriesString.length / 1024.0} MB")
    logInfo(s"Compressed queries: ${queries.length / 1024.0} MB")
    logInfo("Query points collected to driver successfully")
    logInfo("Broadcasting query points")
    val queriesB = partitionsRDD.context.broadcast(queries)
    logInfo("Query points broadcasting was successfully")

    val resultsByPart: RDD[(Long, TopK[SparkScoreDoc])] = partitionsRDD.mapPartitions(partitions =>
        partitions.flatMap { case partition =>
          Snappy.uncompressString(queriesB.value)
            .split(LuceneRDD.Separator).par
            .map(_.split(LuceneRDD.IndexQuerySeparator)) // Split by '/'
            .filter(_.length == 2)  // Keep only index'/'query strings
            .map { case indexWithQuery =>
            val index = indexWithQuery(0)
            val qr = indexWithQuery(1)
            (index.toLong, topKMonoid.build(partition.query(qr, topK)))
          }
        }
    )

    logInfo("Compute topK linkage per partition")
    val results = resultsByPart.reduceByKey(topKMonoid.plus)

    otherWithIndex.join(results).values
      .map(joined => (joined._1, joined._2.items.toArray))
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
  : RDD[(T1, Array[SparkScoreDoc])] = {
    logInfo("LinkByQuery requested")
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
                topK: Int = DefaultTopK): LuceneRDDResponse = {
    logInfo(s"Term search on field ${fieldName} with query ${query}")
    partitionMapper(_.termQuery(fieldName, query, topK), topK)
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
                  topK: Int = DefaultTopK): LuceneRDDResponse = {
    logInfo(s"Prefix search on field ${fieldName} with query ${query}")
    partitionMapper(_.prefixQuery(fieldName, query, topK), topK)
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
                 maxEdits: Int, topK: Int = DefaultTopK): LuceneRDDResponse = {
    logInfo(s"Fuzzy search on field ${fieldName} with query ${query}")
    partitionMapper(_.fuzzyQuery(fieldName, query, maxEdits, topK), topK)
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
                  topK: Int = DefaultTopK): LuceneRDDResponse = {
    logInfo(s"Phrase search on field ${fieldName} with query ${query}")
    partitionMapper(_.phraseQuery(fieldName, query, topK), topK)
  }

  override def count(): Long = {
    logInfo("Count action requested")
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
    logInfo("Closing LuceneRDD...")
    partitionsRDD.foreach(_.close())
  }
}

object LuceneRDD {

  val Separator = '|'
  val IndexQuerySeparator = '/'

  /**
   * Instantiate a LuceneRDD given an RDD[T]
   *
   * @param elems RDD of type T
   * @tparam T Generic type
   * @return
   */
  def apply[T : ClassTag](elems: RDD[T])
    (implicit conv: T => Document): LuceneRDD[T] = {
    val partitions = elems.mapPartitions[AbstractLuceneRDDPartition[T]](
      iter => Iterator(LuceneRDDPartition(iter)),
      preservesPartitioning = true)
    new LuceneRDD[T](partitions)
  }

  /**
   * Instantiate a LuceneRDD with an iterable
   *
   * @param elems Elements to index
   * @param sc Spark Context
   * @tparam T Input type
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

  /**
   * Return project information, i.e., version number, build time etc
   * @return
   */
  def version(): Map[String, Any] = {
    // BuildInfo is automatically generated using sbt plugin `sbt-buildinfo`
    org.zouzias.spark.lucenerdd.BuildInfo.toMap
  }
}
