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
package org.zouzias.spark.lucenerdd.partition

import org.apache.lucene.search.{BooleanClause, Query}
import org.zouzias.spark.lucenerdd.models.indexstats.IndexStatistics
import org.zouzias.spark.lucenerdd.models.TermVectorEntry
import org.zouzias.spark.lucenerdd.response.LuceneRDDResponsePartition

import scala.reflect.ClassTag

/**
 * LuceneRDD partition.
 *
 * @tparam T the type associated with each entry in the set.
 */
private[lucenerdd] abstract class AbstractLuceneRDDPartition[T] extends Serializable
  with AutoCloseable {

  protected implicit def kTag: ClassTag[T]

  def size: Long

  def iterator: Iterator[T]

  def isDefined(key: T): Boolean

  def fields(): Set[String]

  /**
   * Multi term query
   *
   * @param docMap Map of field names to terms
   * @param topK Number of documents to return
   * @return
   */
  def multiTermQuery(docMap: Map[String, String],
                     topK: Int,
                     boolClause: BooleanClause.Occur = BooleanClause.Occur.MUST)
  : LuceneRDDResponsePartition


  /**
   * Generic Lucene Query using QueryParser
   * @param searchString Lucene query string, i.e., textField:hello*
   * @param topK Number of documents to return
   * @return
   */
  def query(searchString: String, topK: Int): LuceneRDDResponsePartition


  /**
    * Lucene search using Lucene [[Query]]
    * @param query Lucene query, i.e., [[org.apache.lucene.search.BooleanQuery]] or
    *              [[org.apache.lucene.search.PhraseQuery]]
    * @param topK Number of documents to return
    * @return
    */
  def query(query: Query, topK: Int): LuceneRDDResponsePartition

  /**
   * Multiple generic Lucene Queries using QueryParser
   * @param searchString  Lucene query string
   * @param topK Number of results to return
   * @return
   */
  def queries(searchString: Iterable[String], topK: Int)
  : Iterable[(String, LuceneRDDResponsePartition)]

  /**
   * Term Query
   * @param fieldName Name of field
   * @param query Query text
   * @param topK Number of documents to return
   * @return
   */
  def termQuery(fieldName: String, query: String, topK: Int): LuceneRDDResponsePartition

  /**
   * Prefix Query
   * @param fieldName Name of field
   * @param query Prefix query
   * @param topK Number of documents to return
   * @return
   */
  def prefixQuery(fieldName: String, query: String, topK: Int): LuceneRDDResponsePartition

  /**
   * Fuzzy Query
   * @param fieldName Name of field
   * @param query Query text
   * @param maxEdits Fuzziness, edit distance
   * @param topK Number of documents to return
   * @return
   */
  def fuzzyQuery(fieldName: String, query: String,
                 maxEdits: Int, topK: Int): LuceneRDDResponsePartition

  /**
   * PhraseQuery
   * @param fieldName Name of field
   * @param query Phrase query, i.e., "hello world"
   * @param topK Number of documents to return
   * @return
   */
  def phraseQuery(fieldName: String, query: String, topK: Int): LuceneRDDResponsePartition


  /**
    * Lucene's More Like This (MLT) functionality
    * @param fieldName Field name
    * @param query Query text
    * @param minTermFreq Minimum term frequency
    * @param minDocFreq Minimum document frequency
    * @param topK Number of returned documents
    * @return
    */
  def moreLikeThis(fieldName: String, query: String,
                   minTermFreq: Int, minDocFreq: Int, topK: Int)
  : LuceneRDDResponsePartition

  /**
    * Returns term vectors for a partition
    *
    * Since each Lucene index is created per partition, docId are not unique.
    * The partitionIndex is used to compute "global" document id from all documents
    * over all partitions
    *
    * @param fieldName Field on which to compute term vectors
    * @param idFieldName Field name which contains unique id
    * @return Array of term vector entries
    */
  def termVectors(fieldName: String, idFieldName: Option[String]): Array[TermVectorEntry]

  /**
    * Returns statistics of the indices over all executors.
    *
    * @param fields Set of defined fields
    * @return
    */
  def indexStats(fields: Set[String]): IndexStatistics

  /**
   * Restricts the entries to those satisfying a predicate
   * @param pred Predicate to filter on
   * @return
   */
  def filter(pred: T => Boolean): AbstractLuceneRDDPartition[T]
}
