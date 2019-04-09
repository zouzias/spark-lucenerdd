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
package org.zouzias.spark.lucenerdd.config

import org.apache.lucene.analysis.Analyzer
import org.zouzias.spark.lucenerdd.analyzers.AnalyzerConfigurable
import org.zouzias.spark.lucenerdd.query.SimilarityConfigurable

 /** Lucene analysis parameters during indexing and querying.
   *
   * @param indexAnalyzer         Index analyzer name. Lucene [[Analyzer]] used during indexing
   * @param queryAnalyzer         Query analyzer name. Lucene [[Analyzer]] used during querying
   * @param similarity            Lucene scoring similarity, i.e., BM25 or TF-IDF
   * @param indexAnalyzerPerField Lucene Analyzer per field (indexing time), default empty
   * @param queryAnalyzerPerField Lucene Analyzer per field (query time), default empty
   */
case class LuceneRDDParams(indexAnalyzer: String,
                           queryAnalyzer: String,
                           similarity: String,
                           indexAnalyzerPerField: Map[String, String],
                           queryAnalyzerPerField: Map[String, String]) extends Serializable


object LuceneRDDParams extends AnalyzerConfigurable with SimilarityConfigurable {

  def apply(indexAnalyzer: String,
            queryAnalyzer: String,
            similarity: String,
            indexAnalyzerPerField: Map[String, String],
            queryAnalyzerPerField: Map[String, String])
  : LuceneRDDParams = {
    new LuceneRDDParams(indexAnalyzer, queryAnalyzer, similarity,
      indexAnalyzerPerField, queryAnalyzerPerField)
  }

  def apply(): LuceneRDDParams = {
    new LuceneRDDParams(getOrElseEn(IndexAnalyzerConfigName),
      getOrElseEn(QueryAnalyzerConfigName),
      getOrElseClassic(),
      Map.empty[String, String],
      Map.empty[String, String])
  }
}
