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
package org.zouzias.spark.lucenerdd.query

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.document.Document
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts
import org.apache.lucene.facet.taxonomy.{FastTaxonomyFacetCounts, TaxonomyReader}
import org.apache.lucene.facet.{FacetsCollector, FacetsConfig}
import org.apache.lucene.queries.mlt.MoreLikeThis
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._
import org.apache.spark.sql.Row
import org.zouzias.spark.lucenerdd.aggregate.SparkFacetResultMonoid
import org.zouzias.spark.lucenerdd.builders.{FuzzyQueryBuilder, MultitermQueryBuilder, PhraseQueryBuilder, PrefixQueryBuilder, TermQueryBuilder}
import org.zouzias.spark.lucenerdd.models.{SparkFacetResult, SparkScoreDoc}

import java.io.StringReader
import scala.collection.JavaConverters._

/**
 * Helper methods for Lucene queries, i.e., term, fuzzy, prefix query
 */
object LuceneQueryHelpers extends Serializable {

  lazy val MatchAllDocs = new MatchAllDocsQuery()
  lazy val MatchAllDocsString = "*:*"
  private val QueryParserDefaultField = "text"

  /**
   * Return all field names
   *
   * @param indexSearcher Index searcher
   * @return
   */
  def fields(indexSearcher: IndexSearcher): Set[String] = {
    indexSearcher.search(MatchAllDocs, 10).scoreDocs.flatMap(x =>
      indexSearcher.getIndexReader.document(x.doc)
      .iterator().asScala
    ).map(doc =>
      doc.name()).toSet[String]
  }

  /**
   * Parse a Query string
   *
   * @param searchString query
   * @param queryAnalyzerPerField Lucene query Analyzers per field
   * @return
   */
  def parseQueryString(searchString: String, queryAnalyzerPerField: PerFieldAnalyzerWrapper)
  : Query = {
    val queryParser = new QueryParser(QueryParserDefaultField, queryAnalyzerPerField)
    queryParser.parse(searchString)
  }

  /**
   * Lucene query parser
   *
   * @param indexSearcher Index searcher
   * @param searchString Lucene search query string
   * @param topK Number of documents to return
   * @param queryAnalyzerPerField Lucene Analyzer per field
   * @return
   */
  def searchParser(indexSearcher: IndexSearcher,
                   searchString: String,
                   topK: Int,
                   queryAnalyzerPerField: PerFieldAnalyzerWrapper)
  : Seq[Row] = {
    val q = parseQueryString(searchString, queryAnalyzerPerField)
    indexSearcher.search(q, topK).scoreDocs.map(SparkScoreDoc(indexSearcher, _).toRow())
  }

  /**
    * Lucene search using a Lucene [[Query]]
    *
    * Important: Query analysis is done during the definition of query
    * @param indexSearcher Lucene index searcher
    * @param query Lucene query
    * @param topK Number of documents to return
    * @return
    */
  def searchQuery(indexSearcher: IndexSearcher,
                  query: Query,
                  topK: Int)
  : Seq[SparkScoreDoc] = {
    indexSearcher.search(query, topK).scoreDocs.map(SparkScoreDoc(indexSearcher, _))
  }

  /**
   * Faceted search using [[SortedSetDocValuesFacetCounts]]
   *
   * @param indexSearcher Index searcher
   * @param taxoReader taxonomy reader used for faceted search
   * @param searchString Lucene search query string
   * @param facetField Facet field name
   * @param topK Number of returned documents
   * @return
   */
  def facetedTextSearch(indexSearcher: IndexSearcher,
                        taxoReader: TaxonomyReader,
                        facetsConfig: FacetsConfig,
                        searchString: String,
                        facetField: String,
                        topK: Int, analyzer: Analyzer): SparkFacetResult = {
    // Prepare the query
    val queryParser = new QueryParser(QueryParserDefaultField, analyzer)
    val q: Query = queryParser.parse(searchString)

    // Collect the facets
    val fc = new FacetsCollector()
    FacetsCollector.search(indexSearcher, q, topK, fc)
    val facets = Option(new FastTaxonomyFacetCounts(taxoReader, facetsConfig, fc))

    // Present the facets
    facets match {
      case Some(fcts) => SparkFacetResult(facetField, fcts.getTopChildren(topK, facetField))
      case None => SparkFacetResultMonoid.zero(facetField)
    }
  }

  /**
   * Returns total number of lucene documents
   *
   * @param indexSearcher Index searcher
   * @return
   */
  def totalDocs(indexSearcher: IndexSearcher): Long = {
    indexSearcher.getIndexReader.numDocs().toLong
  }

  /**
   * Search top-k documents
   *
   * @param indexSearcher Index searcher
   * @param query Lucene Query object
   * @param topK Number of returned documents
   * @return
   */
  def searchTopKDocs(indexSearcher: IndexSearcher, query: Query, topK: Int): Seq[Document] = {
    val topDocs = indexSearcher.search(query, topK)
    topDocs.scoreDocs.map(_.doc).map(x => indexSearcher.doc(x))
  }

  /**
   * Search top-k documents given a query
   *
   * @param indexSearcher Index searcher
   * @param query Lucene Query object
   * @param topK Number of returned documents
   * @return
   */
  def searchTopK(indexSearcher: IndexSearcher, query: Query, topK: Int): Seq[SparkScoreDoc] = {
   indexSearcher.search(query, topK).scoreDocs.map(SparkScoreDoc(indexSearcher, _))
  }

  /**
   * Term query
   *
   * @param indexSearcher Index searcher
   * @param fieldName Field name
   * @param fieldText Query
   * @param topK Number of returned documents
   * @return
   */
  def termQuery(indexSearcher: IndexSearcher,
                fieldName: String,
                fieldText: String,
                topK: Int): Seq[SparkScoreDoc] = {
    LuceneQueryHelpers.searchTopK(indexSearcher,
      TermQueryBuilder(fieldName, fieldText).buildQuery(), topK)
  }

  /**
   * Prefix query
   *
   * @param indexSearcher Index searcher
   * @param fieldName Field name
   * @param fieldText Query
   * @param topK Number of returned documents
   * @return
   */
  def prefixQuery(indexSearcher: IndexSearcher,
                  fieldName: String,
                  fieldText: String,
                  topK: Int): Seq[SparkScoreDoc] = {
    LuceneQueryHelpers.searchTopK(indexSearcher,
      PrefixQueryBuilder(fieldName, fieldText).buildQuery(), topK)
  }

  /**
   * Fuzzy query
   *
   * @param indexSearcher Index searcher
   * @param fieldName Field name
   * @param fieldText Query
   * @param maxEdits Edit distance
   * @param topK Number of returned documents
   * @return
   */
  def fuzzyQuery(indexSearcher: IndexSearcher,
                 fieldName: String,
                 fieldText: String,
                 maxEdits: Int,
                 topK: Int): Seq[SparkScoreDoc] = {
    LuceneQueryHelpers.searchTopK(indexSearcher,
      FuzzyQueryBuilder(fieldName, fieldText, maxEdits).buildQuery(), topK)
  }

  /**
   * Phrase query
   *
   * @param indexSearcher Index searcher
   * @param fieldName Field name
   * @param fieldText Query
   * @param topK Number of returned documents
   * @param analyzer Analyzer class name
   * @return
   */
  def phraseQuery(indexSearcher: IndexSearcher,
                  fieldName: String,
                  fieldText: String,
                  topK: Int,
                  analyzer: String): Seq[SparkScoreDoc] = {
    LuceneQueryHelpers.searchTopK(indexSearcher,
      PhraseQueryBuilder(fieldName, fieldText, analyzer).buildQuery(), topK)
  }

  /**
   * Multi term search
   *
   * @param indexSearcher Index searcher
   * @param docMap Query as map
   * @param topK Number of returned documents
   * @return
   */
  def multiTermQuery(indexSearcher: IndexSearcher,
                     docMap: Map[String, String],
                     topK : Int,
                     booleanClause: BooleanClause.Occur = BooleanClause.Occur.MUST)
  : Seq[SparkScoreDoc] = {
    searchTopK(indexSearcher,
      MultitermQueryBuilder(docMap, booleanClause).buildQuery(), topK)
  }

  /**
    * Lucene's More Like This (MLT) functionality
    *
    * @param indexSearcher Index searcher
    * @param fieldName Field on which MLT is applied
    * @param query Lucene query string
    * @param minTermFreq Minimum term frequency
    * @param minDocFreq Minimum document frequency
    * @param topK Number of returned results
    * @param analyzer Lucene analyzer
    * @return
    */
  def moreLikeThis(indexSearcher: IndexSearcher, fieldName: String,
                   query: String,
                   minTermFreq: Int, minDocFreq: Int, topK: Int,
                   analyzer: Analyzer)
  : Iterator[SparkScoreDoc] = {
    val mlt = new MoreLikeThis(indexSearcher.getIndexReader)
    mlt.setMinTermFreq(minTermFreq)
    mlt.setMinDocFreq(minDocFreq)
    mlt.setFieldNames(Array(fieldName)) // FIXME: Is this necessary?
    mlt.setAnalyzer(analyzer)
    val q = mlt.like(fieldName, new StringReader(query))
    searchTopK(indexSearcher, q, topK).toIterator
  }
}
