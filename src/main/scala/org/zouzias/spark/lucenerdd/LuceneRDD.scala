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
import org.zouzias.spark.lucenerdd.analyzers.AnalyzerConfigurable
import org.zouzias.spark.lucenerdd.models.indexstats.IndexStatistics
import org.zouzias.spark.lucenerdd.partition.{AbstractLuceneRDDPartition, LuceneRDDPartition}
import org.zouzias.spark.lucenerdd.models.{SparkScoreDoc, TermVectorEntry}
import org.zouzias.spark.lucenerdd.query.SimilarityConfigurable
import org.zouzias.spark.lucenerdd.versioning.Versionable

import scala.reflect.ClassTag

/**
  * Spark RDD with Lucene's query capabilities (term, prefix, fuzzy, phrase query)
  *
  * @param partitionsRDD Partitions of RDD
  * @param indexAnalyzer Analyzer during indexing time
  * @param queryAnalyzer Analyzer during query time
  * @param similarity Query similarity (TF-IDF / BM25)
  * @tparam T
  */
class LuceneRDD[T: ClassTag](protected val partitionsRDD: RDD[AbstractLuceneRDDPartition[T]],
                             protected val indexAnalyzer: String,
                             protected val queryAnalyzer: String,
                             protected val similarity: String)
  extends RDD[T](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
  with LuceneRDDConfigurable {

  logInfo("Instance is created...")
  setName("LuceneRDD")

  /** Lucene fields */
  private lazy val _fields: Set[String] = partitionsRDD.map(_.fields()).reduce(_ ++ _)

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
    _fields
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
    * Deduplication of self
    *
    * @param searchQueryGen Search query mapper function
    * @param topK Number of results to deduplication
    * @param linkerMethod Method to perform linkage
    *
    * @return
    */
  def dedup[T1: ClassTag](searchQueryGen: T1 => String,
                          topK: Int = DefaultTopK,
                          linkerMethod: String = getLinkerMethod)
  : RDD[(T1, Array[SparkScoreDoc])] = {
    // FIXME: is this asInstanceOf necessary?
    link[T1](this.asInstanceOf[RDD[T1]], searchQueryGen, topK, linkerMethod)
  }

  /**
   * Entity linkage via Lucene query over all elements of an RDD.
   *
   * @param other DataFrame to be linked
   * @param searchQueryGen Function that generates a search query for each element of other
   * @param topK
   * @param linkerMethod Method to perform linkage
   * @return an RDD of Tuple2 that contains the linked search Lucene documents in the second
   */
  def linkDataFrame(other: DataFrame,
                    searchQueryGen: Row => String,
                    topK: Int = DefaultTopK,
                    linkerMethod: String = getLinkerMethod)
  : RDD[(Row, Array[SparkScoreDoc])] = {
    logInfo("LinkDataFrame requested")
    link[Row](other.rdd, searchQueryGen, topK, linkerMethod)
  }

  /**
    * Entity linkage via Lucene query over all elements of an RDD.
    *
    * @param other RDD to be linked
    * @param searchQueryGen Function that generates a Lucene Query object for each element of other
    * @param linkerMethod Method to perform linkage
    * @tparam T1 A type
    * @return an RDD of Tuple2 that contains the linked search Lucene Document
    *         in the second position
    */
  def linkByQuery[T1: ClassTag](other: RDD[T1],
                                searchQueryGen: T1 => Query,
                                topK: Int = DefaultTopK,
                                linkerMethod: String = getLinkerMethod)
  : RDD[(T1, Array[SparkScoreDoc])] = {
    logInfo("LinkByQuery requested")
    def typeToQueryString = (input: T1) => {
      searchQueryGen(input).toString
    }

    link[T1](other, typeToQueryString, topK, linkerMethod)
  }

  /**
   * Entity linkage via Lucene query over all elements of an RDD.
   *
   * @param other RDD to be linked
   * @param searchQueryGen Function that generates a search query for each element of other
   * @param linkerMethod Method to perform linkage, default value from configuration
   * @tparam T1 A type
   * @return an RDD of Tuple2 that contains the linked search Lucene documents in the second
   *
   * Note: Currently the query strings of the other RDD are collected to the driver and
   * broadcast to the workers.
   */
  def link[T1: ClassTag](other: RDD[T1],
                         searchQueryGen: T1 => String,
                         topK: Int = DefaultTopK,
                         linkerMethod: String = getLinkerMethod)
    : RDD[(T1, Array[SparkScoreDoc])] = {
    logInfo("Linkage requested")

    val topKMonoid = new TopKMonoid[SparkScoreDoc](topK)(SparkScoreDoc.descending)
    val queriesWithIndex = other.zipWithIndex().map(_.swap)

    logInfo(s"Linker method is ${linkerMethod}")
    val resultsByPart = linkerMethod match {
      case "cartesian" =>
        val concatenated = queriesWithIndex.mapValues(searchQueryGen).glom()

        concatenated.cartesian(partitionsRDD)
          .flatMap { case (qs, part) =>
            qs.map(q => (q._1, topKMonoid.build(part.query(q._2, topK))))
          }
      case _ =>
        logInfo("Collecting query points to driver")
        val collectedQueries = queriesWithIndex.mapValues(searchQueryGen).collect()
        val queriesB = partitionsRDD.context.broadcast(collectedQueries)

        val resultsByPart: RDD[(Long, TopK[SparkScoreDoc])] =
          partitionsRDD.mapPartitions(partitions =>
          partitions.flatMap { partition =>
            queriesB.value.map { case (index, qr) =>
              (index, topKMonoid.build(partition.query(qr, topK)))
            }
          })

        resultsByPart
    }

    logInfo("Computing top-k linkage per partition")
    val results = resultsByPart.reduceByKey(topKMonoid.plus)

    queriesWithIndex.join(results).values
      .map(joined => (joined._1, joined._2.items.toArray))
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

  /**
    * Lucene's More Like This (MLT) functionality
    *
    * @param fieldName Field name
    * @param query Query text
    * @param minTermFreq Minimum term frequency
    * @param minDocFreq Minimum document frequency
    * @param topK Number of returned documents
    * @return
    */
  def moreLikeThis(fieldName: String, query: String,
                   minTermFreq: Int, minDocFreq: Int, topK: Int = DefaultTopK)
  : LuceneRDDResponse = {
    logInfo(s"MoreLikeThis field: ${fieldName}, query: ${query}")
    partitionMapper(_.moreLikeThis(fieldName, query, minTermFreq, minDocFreq, topK), topK)
  }

  /**
    * Return Term vector for a Lucene field
    *
    * @param fieldName Field name for term vectors
    * @param idFieldName Lucene field that contains unique id:
    *     default set to None, in which case id equals (docId, partitionId)
    * @return RDD of term vector entries,
    *         i.e., (document id, term as String, term frequency in document)
    */
  def termVectors(fieldName: String, idFieldName: Option[String] = None): RDD[TermVectorEntry] = {
    require(StringFieldsStoreTermVector,
      "Store term vectors is not configured. Set lucenerdd.index.stringfields.terms.vectors=true")
    partitionsRDD.flatMap { case part =>
      part.termVectors(fieldName, idFieldName)
    }
  }

  def indexStats(): RDD[IndexStatistics] = {
    val flds = fields()
    partitionsRDD.map(_.indexStats(flds))
  }

  /** RDD compute method. */
  override def compute(part: Partition, context: TaskContext): Iterator[T] = {
    firstParent[AbstractLuceneRDDPartition[T]].iterator(part, context).next.iterator
  }

  override def filter(pred: T => Boolean): LuceneRDD[T] = {
    val newPartitionRDD = partitionsRDD.mapPartitions(partition =>
      partition.map(_.filter(pred)), preservesPartitioning = true
    )
    new LuceneRDD(newPartitionRDD, indexAnalyzer, queryAnalyzer, similarity)
  }

  def exists(elem: T): Boolean = {
    partitionsRDD.map(_.isDefined(elem)).collect().exists(x => x)
  }

  def close(): Unit = {
    logInfo("Closing LuceneRDD...")
    partitionsRDD.foreach(_.close())
  }
}

object LuceneRDD extends Versionable
  with AnalyzerConfigurable
  with SimilarityConfigurable {

  /**
   * Instantiate a LuceneRDD given an RDD[T]
   *
   * @param elems RDD of type T
   * @param indexAnalyzer Index Analyzer name
   * @param queryAnalyzer Query Analyzer name
   * @param similarity Lucene scoring similarity, i.e., BM25 or TF-IDF
   * @tparam T Generic type
   * @return
   */
  def apply[T : ClassTag](elems: RDD[T],
                          indexAnalyzer: String,
                          queryAnalyzer: String,
                          similarity: String)
    (implicit conv: T => Document): LuceneRDD[T] = {
    val partitions = elems.mapPartitionsWithIndex[AbstractLuceneRDDPartition[T]](
      (partId, iter) => Iterator(LuceneRDDPartition(iter, partId, indexAnalyzer, queryAnalyzer,
        similarity)),
      preservesPartitioning = true)
    new LuceneRDD[T](partitions, indexAnalyzer, queryAnalyzer, similarity)
  }

  def apply[T : ClassTag](elems: RDD[T])
                         (implicit conv: T => Document): LuceneRDD[T] = {
   apply(elems, getOrElseEn(IndexAnalyzerConfigName), getOrElseEn(QueryAnalyzerConfigName),
     getOrElseClassic())
  }

  /**
   * Instantiate a LuceneRDD with an iterable
   *
   * @param elems Elements to index
   * @param indexAnalyzer Index Analyzer name
   * @param queryAnalyzer Query Analyzer name
   * @param similarity Lucene scoring similarity, i.e., BM25 or TF-IDF
   * @param sc Spark Context
   * @tparam T Input type
   * @return
   */
  def apply[T : ClassTag]
  (elems: Iterable[T],
   indexAnalyzer: String,
   queryAnalyzer: String,
   similarity: String)
  (implicit sc: SparkContext, conv: T => Document)
  : LuceneRDD[T] = {
    apply[T](sc.parallelize[T](elems.toSeq), indexAnalyzer, queryAnalyzer, similarity)
  }

  def apply[T : ClassTag]
  (elems: Iterable[T])
  (implicit sc: SparkContext, conv: T => Document)
  : LuceneRDD[T] = {
    apply[T](elems,
      getOrElseEn(IndexAnalyzerConfigName),
      getOrElseEn(QueryAnalyzerConfigName),
      getOrElseClassic())
  }

  /**
   * Instantiate a LuceneRDD with DataFrame
   *
   * @param dataFrame Spark DataFrame
   * @param indexAnalyzer Index Analyzer name
   * @param queryAnalyzer Query Analyzer name
   * @param similarity Lucene scoring similarity, i.e., BM25 or TF-IDF
    * @return
   */
  def apply(dataFrame: DataFrame,
            indexAnalyzer: String,
            queryAnalyzer: String,
            similarity: String)
  : LuceneRDD[Row] = {
    apply[Row](dataFrame.rdd, indexAnalyzer, queryAnalyzer, similarity)
  }

  /**
    * Constructor with default index, query analyzers and Lucene similarity
    *
    * @param dataFrame Input DataFrame
    * @return
    */
  def apply(dataFrame: DataFrame)
  : LuceneRDD[Row] = {
    apply[Row](dataFrame.rdd,
      getOrElseEn(IndexAnalyzerConfigName),
      getOrElseEn(QueryAnalyzerConfigName),
      getOrElseClassic())
  }

  /**
    * Entity linkage between two [[DataFrame]] by filtering on one or more columns.
    *
    * @param queries
    * @param corpus
    * @param rowToQueryString
    * @param queriesPartColumns
    * @param corpusPartColumns
    * @param topK
    * @param indexAnalyzer
    * @param queryAnalyzer
    * @param similarity
    * @tparam T
    * @return
    */
  def blockEntityLinkage(queries: DataFrame,
                         corpus: DataFrame,
                         rowToQueryString: Row => String,
                         queriesPartColumns: Array[String],
                         corpusPartColumns: Array[String],
                         topK : Int = 3,
                         indexAnalyzer: String = getOrElseEn(IndexAnalyzerConfigName),
                         queryAnalyzer: String = getOrElseEn(QueryAnalyzerConfigName),
                         similarity: String = getOrElseClassic())
  : RDD[(Row, Array[SparkScoreDoc])] = {

    import org.apache.spark.sql.functions._

    val partColumn = "PARTITION_COLUMN"
    val blocked = corpus.withColumn(partColumn,
      concat(corpusPartColumns.map(corpus.col): _*))
      .rdd.keyBy(x => x.getString(x.fieldIndex(partColumn)))
    val blockedQueries = queries.withColumn(partColumn,
      concat(corpusPartColumns.map(corpus.col): _*))
      .rdd.keyBy(x => x.getString(x.fieldIndex(partColumn)))

    val output = blockedQueries.cogroup(blocked)
      .mapPartitionsWithIndex{ case (idx, iterKeyedByHash) =>
        iterKeyedByHash.flatMap { case (_, (qs, lucene)) =>
          val lucenePart = LuceneRDDPartition(lucene.toIterator, idx, indexAnalyzer,
            queryAnalyzer, similarity)

          qs.map(q => (q, lucenePart.query(rowToQueryString(q), topK).results.toArray))
        }
    }

    output
  }
}
