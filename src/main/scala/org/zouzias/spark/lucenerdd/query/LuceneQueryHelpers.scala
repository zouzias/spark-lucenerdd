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
import org.apache.lucene.document.Document
import org.apache.lucene.facet.{FacetsCollector, FacetsConfig}
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts
import org.apache.lucene.facet.taxonomy.{FastTaxonomyFacetCounts, TaxonomyReader}
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._
import org.zouzias.spark.lucenerdd.aggregate.SparkFacetResultMonoid
import org.zouzias.spark.lucenerdd.models.{SparkFacetResult, SparkScoreDoc}

import scala.collection.JavaConverters._
/**
 * Helpers for lucene queries
 */
object LuceneQueryHelpers extends Serializable {

  lazy val MatchAllDocs = new MatchAllDocsQuery()
  private val QueryParserDefaultField = "text"

  /**
   * Return all field names
   * @param indexSearcher
   * @return
   */
  def fields(indexSearcher: IndexSearcher): Set[String] = {
    indexSearcher.search(MatchAllDocs, 1).scoreDocs.flatMap(x =>
      indexSearcher.getIndexReader.document(x.doc)
      .getFields().asScala
    ).map{ case doc =>
      doc.name()
    }.toSet[String]
  }

  /**
   * Lucene query parser
   * @param indexSearcher
   * @param searchString
   * @param topK
   * @param analyzer
   * @return
   */
  def searchParser(indexSearcher: IndexSearcher,
                   searchString: String,
                   topK: Int)(implicit analyzer: Analyzer)
  : Seq[SparkScoreDoc] = {
    val queryParser = new QueryParser(QueryParserDefaultField, analyzer)
    val q: Query = queryParser.parse(searchString)
    indexSearcher.search(q, topK).scoreDocs.map(SparkScoreDoc(indexSearcher, _))
  }

  /**
   * Faceted search using [[SortedSetDocValuesFacetCounts]]
   * @param indexSearcher
   * @param taxoReader taxonomy reader used for faceted search
   * @param searchString
   * @param facetField
   * @param topK
   * @return
   */
  def facetedTextSearch(indexSearcher: IndexSearcher,
                        taxoReader: TaxonomyReader,
                        facetsConfig: FacetsConfig,
                        searchString: String,
                        facetField: String,
                        topK: Int)(implicit analyzer: Analyzer): SparkFacetResult = {
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
   * @param indexSearcher
   * @return
   */
  def totalDocs(indexSearcher: IndexSearcher): Long = {
    indexSearcher.getIndexReader.numDocs().toLong
  }

  /**
   * Search top-k documents
   * @param indexSearcher
   * @param query
   * @param k
   * @return
   */
  def searchTopKDocs(indexSearcher: IndexSearcher, query: Query, k: Int): Seq[Document] = {
    val topDocs = indexSearcher.search(query, k)
    topDocs.scoreDocs.map(_.doc).map(x => indexSearcher.doc(x))
  }

  /**
   * Search top-k documents given a query
   * @param indexSearcher
   * @param query
   * @param k
   * @return
   */
  def searchTopK(indexSearcher: IndexSearcher, query: Query, k: Int): Seq[SparkScoreDoc] = {
   indexSearcher.search(query, k).scoreDocs.map(SparkScoreDoc(indexSearcher, _))
  }

  /**
   * Term query
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
                  topK: Int,
                  phraseSeparator: String = " "): Seq[SparkScoreDoc] = {
    val builder = new PhraseQuery.Builder()
    fieldText.split(phraseSeparator).foreach( token => builder.add(new Term(fieldName, token)))
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
                     topK : Int,
                     booleanClause: BooleanClause.Occur = BooleanClause.Occur.MUST)
  : Seq[SparkScoreDoc] = {

    val builder = new BooleanQuery.Builder()
    val terms = docMap.map{ case (field, fieldValue) =>
      new TermQuery(new Term(field, fieldValue))
    }

    terms.foreach{ case termQuery =>
      builder.add(termQuery, booleanClause)
    }

    searchTopK(indexSearcher, builder.build(), topK)
  }
}
