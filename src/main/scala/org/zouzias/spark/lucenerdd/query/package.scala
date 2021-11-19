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

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanClause, BooleanQuery, BoostQuery, FuzzyQuery, PhraseQuery, PrefixQuery, Query, TermQuery}
import org.zouzias.spark.lucenerdd.LuceneRDD.loadConstructor

import java.io.StringReader
import scala.collection.mutable.ListBuffer

package object builders {

  /**
   * Common Query Builder
   */
  trait QueryBuilder extends Serializable {

    def buildQuery(): Query

    /**
     * Extract list of terms for a given analyzer
     *
     * @param text Text to analyze
     * @param analyzer Analyzer to utilize
     * @return
     */
    def analyzeTerms(text: String, analyzer: Analyzer): List[String] = {
      val stream = analyzer.tokenStream(null, new StringReader(text))
      val cattr = stream.addAttribute(classOf[CharTermAttribute])
      stream.reset()
      val buffer = ListBuffer.empty[String]
      while (stream.incrementToken()) {
        buffer.append(cattr.toString)
      }
      stream.end()
      stream.close()
      buffer.toList
    }
  }

  /**
   * Common Terms Based Query Builder
   */
  trait TermBasedQueryBuilder extends QueryBuilder {
    def fieldName: String
    def fieldText: String
  }

  /**
   * TermQuery Builder
   * @param fieldName Field name
   * @param fieldText Query
   */
  case class TermQueryBuilder(
    fieldName: String,
    fieldText: String
  ) extends TermBasedQueryBuilder {
    override def buildQuery(): Query = {
      val term = new Term(fieldName, fieldText)
      new TermQuery(term)
    }
  }

  /**
   * FuzzyQuery Builder
   * @param fieldName Field name
   * @param fieldText Query
   * @param maxEdits Edit Distance
   */
  case class FuzzyQueryBuilder(
    fieldName: String,
    fieldText: String,
    maxEdits: Int
  ) extends TermBasedQueryBuilder {
    override def buildQuery(): Query = {
      val term = new Term(fieldName, fieldText)
      new FuzzyQuery(term, maxEdits)
    }
  }

  /**
   * PrefixQuery Builder
   * @param fieldName Field name
   * @param fieldText Query
   */
  case class PrefixQueryBuilder(
    fieldName: String,
    fieldText: String
  ) extends TermBasedQueryBuilder {
    override def buildQuery(): Query = {
      val term = new Term(fieldName, fieldText)
      new PrefixQuery(term)
    }
  }


  /**
   * PhraseQuery builder
   * @param fieldName Field name
   * @param fieldText Query
   * @param analyzer Analyzer class name
   */
  case class PhraseQueryBuilder(
    fieldName: String,
    fieldText: String,
    analyzer: String
  ) extends TermBasedQueryBuilder {

    override def buildQuery(): Query = {
      val builder = new PhraseQuery.Builder()
      val luceneAnalyzer : Analyzer = loadConstructor(analyzer)
      val terms = analyzeTerms(fieldText, luceneAnalyzer)
      terms.foreach( token => builder.add(new Term(fieldName, token)))
      builder.build()
    }
  }

  /**
   * Field Analyzer based query builder
   * @param fieldName Field name
   * @param fieldText Query
   * @param analyzer Analyzer class name
   */
  case class FieldQueryBuilder(
                                 fieldName: String,
                                 fieldText: String,
                                 analyzer: String
                               ) extends TermBasedQueryBuilder {

    override def buildQuery(): Query = {
      val luceneAnalyzer : Analyzer = loadConstructor(analyzer)
      val queryParser = new QueryParser(fieldName, luceneAnalyzer)
      queryParser.parse(fieldText)
    }
  }


  /**
   * MultitermQuery Builder
   *
   * @param docMap Fields - terms map
   * @param booleanClause Boolean query clause
   */
  case class MultitermQueryBuilder(
    docMap: Map[String, String],
    booleanClause: BooleanClause.Occur = BooleanClause.Occur.MUST
  ) extends QueryBuilder {
    override def buildQuery(): Query = {
      val builder = new BooleanQuery.Builder()
      val terms = docMap.map{ case (field, fieldValue) =>
        new TermQuery(new Term(field, fieldValue))
      }

      terms.foreach(termQuery =>
        builder.add(termQuery, booleanClause))

      builder.build()
    }
  }

  /**
   * BoostQuery Builder
   *
   * @param queryBuilder Lucene RDD Query Builder
   * @param boost boosting weight
   */
  case class BoostQueryBuilder(
    queryBuilder: QueryBuilder,
    boost: Float
  ) extends QueryBuilder {
    override def buildQuery(): BoostQuery = {
      new BoostQuery(queryBuilder.buildQuery(), boost)
    }
  }

}
