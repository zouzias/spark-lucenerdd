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

  def totalDocs(indexSearcher: IndexSearcher): Long = {
    indexSearcher.search(MatchAllDocs, 1).totalHits
  }

  def searchTopKDocs(indexSearcher: IndexSearcher, query: Query, k: Int): Seq[Document] = {
    val topDocs = indexSearcher.search(query, k)
    topDocs.scoreDocs.map(_.doc).map(indexSearcher.doc(_))
  }

  def searchTopK(indexSearcher: IndexSearcher, query: Query, k: Int): Seq[SparkScoreDoc] = {
   indexSearcher.search(query, k).scoreDocs.map(SparkScoreDoc(indexSearcher, _))
  }

  def termQuery(indexSearcher: IndexSearcher,
                fieldName: String,
                fieldText: String,
                topK: Int): Seq[SparkScoreDoc] = {
    val term = new Term(fieldName, fieldText)
    val qr = new TermQuery(term)
    LuceneQueryHelpers.searchTopK(indexSearcher, qr, topK)
  }

  def prefixQuery(indexSearcher: IndexSearcher,
                  fieldName: String,
                  fieldText: String,
                  topK: Int): Seq[SparkScoreDoc] = {
    val term = new Term(fieldName, fieldText)
    val qr = new PrefixQuery(term)
    LuceneQueryHelpers.searchTopK(indexSearcher, qr, topK)
  }

  def fuzzyQuery(indexSearcher: IndexSearcher,
                 fieldName: String,
                 fieldText: String,
                 maxEdits: Int,
                 topK: Int): Seq[SparkScoreDoc] = {
    val term = new Term(fieldName, fieldText)
    val qr = new FuzzyQuery(term, maxEdits)
    LuceneQueryHelpers.searchTopK(indexSearcher, qr, topK)
  }

  def phraseQuery(indexSearcher: IndexSearcher,
                  fieldName: String,
                  fieldText: String,
                  topK: Int): Seq[SparkScoreDoc] = {
    val builder = new PhraseQuery.Builder()
    fieldText.split(" ").foreach( token => builder.add(new Term(fieldName, token)))
    LuceneQueryHelpers.searchTopK(indexSearcher, builder.build(), topK)
  }
}
