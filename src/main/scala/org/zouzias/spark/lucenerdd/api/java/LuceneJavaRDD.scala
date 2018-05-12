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
package org.zouzias.spark.lucenerdd.api.java

import org.apache.lucene.search.Query
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.models.{SparkScoreDoc, TermVectorEntry}
import org.zouzias.spark.lucenerdd.models.indexstats.IndexStatistics
import org.zouzias.spark.lucenerdd.response.LuceneRDDResponse

import scala.reflect.ClassTag

class LuceneJavaRDD[T](override val rdd: LuceneRDD[T])
                      (implicit override val classTag: ClassTag[T])
  extends JavaRDD(rdd) {

  /**
    * Return all document fields
    *
    * @return
    */
  def fields(): Set[String] = {
    rdd.fields()
  }

  /**
    * Lucene generic query
    *
    * @param doc
    * @return
    */
  def exists(doc: Map[String, String]): Boolean = {
    rdd.exists(doc)
  }

  /**
    * Generic query using Lucene's query parser
    *
    * @param searchString  Query String
    * @param topK
    * @return
    */
  def query(searchString: String, topK: Int): LuceneRDDResponse = {
    rdd.query(searchString, topK)
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
                          topK: Int,
                          linkerMethod: String )
  : JavaRDD[(T1, Array[SparkScoreDoc])] = {
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
                    topK: Int,
                    linkerMethod: String)
  : JavaRDD[(Row, Array[SparkScoreDoc])] = {
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
                                topK: Int,
                                linkerMethod: String)
  : JavaRDD[(T1, Array[SparkScoreDoc])] = {
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
                         topK: Int,
                         linkerMethod: String)
  : JavaRDD[(T1, Array[SparkScoreDoc])] = {
    JavaRDD.fromRDD(rdd.link(other, searchQueryGen, topK, linkerMethod))
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
                topK: Int): LuceneRDDResponse = {
    rdd.termQuery(fieldName, query, topK)
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
                  topK: Int): LuceneRDDResponse = {
    rdd.prefixQuery(fieldName, query, topK)
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
                 maxEdits: Int, topK: Int): LuceneRDDResponse = {
    rdd.fuzzyQuery(fieldName, query, maxEdits, topK)
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
                  topK: Int): LuceneRDDResponse = {
    rdd.phraseQuery(fieldName, query, topK)
  }

  override def count(): Long = {
    rdd.count()
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
                   minTermFreq: Int, minDocFreq: Int, topK: Int)
  : LuceneRDDResponse = {
    rdd.moreLikeThis(fieldName, query, minTermFreq, minDocFreq, topK)
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
  def termVectors(fieldName: String, idFieldName: Option[String] = None)
  : JavaRDD[TermVectorEntry] = {
    JavaRDD.fromRDD(rdd.termVectors(fieldName, idFieldName))
  }

  def indexStats(): JavaRDD[IndexStatistics] = {
    JavaRDD.fromRDD(rdd.indexStats())
  }


  def filter(pred: T => Boolean): LuceneJavaRDD[T] = {
    val filteredRDD = rdd.filter(pred)
    new LuceneJavaRDD(filteredRDD)
  }

  def exists(elem: T): Boolean = {
    rdd.exists(elem)
  }

  def close(): Unit = {
    rdd.close()
  }
}
