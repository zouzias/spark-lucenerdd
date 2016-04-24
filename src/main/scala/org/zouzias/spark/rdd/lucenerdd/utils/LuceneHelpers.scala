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
package org.zouzias.spark.rdd.lucenerdd.utils

import org.apache.lucene.document.Document
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, Query, ScoreDoc}


object LuceneHelpers {

  private val MatchAllDocs = new MatchAllDocsQuery()

  def totalDocs(indexSearcher: IndexSearcher): Long = {
    indexSearcher.search(MatchAllDocs, 1).totalHits
  }

  def searchTopKDocs(indexSearcher: IndexSearcher, query: Query, k: Int): Seq[Document] = {
    val topDocs = indexSearcher.search(query, k)
    topDocs.scoreDocs.map(_.doc).map(indexSearcher.doc(_))
  }

  def searchTopK(indexSearcher: IndexSearcher, query: Query, k: Int): Seq[ScoreDoc] = {
   indexSearcher.search(query, k).scoreDocs
  }

}
