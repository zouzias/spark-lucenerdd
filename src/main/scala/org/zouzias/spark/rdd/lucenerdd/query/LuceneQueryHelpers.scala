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
package org.zouzias.spark.rdd.lucenerdd.query

import org.apache.lucene.document.Document
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.zouzias.spark.rdd.lucenerdd.model.SparkScoreDoc

/**
 * Helpers for lucene queries
 */
object LuceneQueryHelpers {

  private val MatchAllDocs = new MatchAllDocsQuery()

  /**
   * Count number of lucene documents
   * @param indexSearcher
   * @return
   */
  def totalDocs(indexSearcher: IndexSearcher): Long = {
    indexSearcher.search(MatchAllDocs, 1).totalHits
  }

  /**
   * Search topK documents
   * @param indexSearcher
   * @param query
   * @param k
   * @return
   */
  def searchTopKDocs(indexSearcher: IndexSearcher, query: Query, k: Int): Seq[Document] = {
    val topDocs = indexSearcher.search(query, k)
    topDocs.scoreDocs.map(_.doc).map(x => indexSearcher.doc(x))
  }

  def searchTopK(indexSearcher: IndexSearcher, query: Query, k: Int): Seq[SparkScoreDoc] = {
   indexSearcher.search(query, k).scoreDocs.map(SparkScoreDoc(indexSearcher, _))
  }

  /**
   * Term query on given field
   * @param indexSearcher
   * @param fieldName
   * @param fieldText
   * @param topK
   * @return
   */
  def termQuery(indexSearcher: IndexSearcher,
                fieldName: String,
                fieldText: String,
                topK: Int): Seq[SparkScoreDoc] = {
    val term = new Term(fieldName, fieldText)
    val qr = new TermQuery(term)
    LuceneQueryHelpers.searchTopK(indexSearcher, qr, topK)
  }

  /**
   * Prefix query
   * @param indexSearcher
   * @param fieldName
   * @param fieldText
   * @param topK
   * @return
   */
  def prefixQuery(indexSearcher: IndexSearcher,
                  fieldName: String,
                  fieldText: String,
                  topK: Int): Seq[SparkScoreDoc] = {
    val term = new Term(fieldName, fieldText)
    val qr = new PrefixQuery(term)
    LuceneQueryHelpers.searchTopK(indexSearcher, qr, topK)
  }

  /**
   * Fuzzy query
   * @param indexSearcher
   * @param fieldName
   * @param fieldText
   * @param maxEdits
   * @param topK
   * @return
   */
  def fuzzyQuery(indexSearcher: IndexSearcher,
                 fieldName: String,
                 fieldText: String,
                 maxEdits: Int,
                 topK: Int): Seq[SparkScoreDoc] = {
    val term = new Term(fieldName, fieldText)
    val qr = new FuzzyQuery(term, maxEdits)
    LuceneQueryHelpers.searchTopK(indexSearcher, qr, topK)
  }

  /**
   * Phrase query
   * @param indexSearcher
   * @param fieldName
   * @param fieldText
   * @param topK
   * @return
   */
  def phraseQuery(indexSearcher: IndexSearcher,
                  fieldName: String,
                  fieldText: String,
                  topK: Int): Seq[SparkScoreDoc] = {
    val builder = new PhraseQuery.Builder()
    fieldText.split(" ").foreach( token => builder.add(new Term(fieldName, token)))
    LuceneQueryHelpers.searchTopK(indexSearcher, builder.build(), topK)
  }

  /**
   * Multi term search
   * @param indexSearcher
   * @param docMap
   * @param topK
   * @return
   */
  def multiTermQuery(indexSearcher: IndexSearcher,
                     docMap: Map[String, String],
                     topK : Int): Seq[SparkScoreDoc] = {

    val builder = new BooleanQuery.Builder()

    val terms = docMap.map{ case (field, fieldValue) =>
      new TermQuery(new Term(field, fieldValue))
    }

    terms.foreach{ case termQuery =>
      builder.add(termQuery, BooleanClause.Occur.MUST)
    }

    searchTopK(indexSearcher, builder.build(), topK)
  }
}
