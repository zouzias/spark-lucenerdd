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

import org.apache.lucene.document._
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search._
import org.apache.spark.Logging
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.models.{SparkFacetResult, SparkScoreDoc}
import org.zouzias.spark.lucenerdd.query.LuceneQueryHelpers
import org.zouzias.spark.lucenerdd.store.IndexWithTaxonomyWriter

import scala.reflect.{ClassTag, _}

private[lucenerdd] class LuceneRDDPartition[T]
(private val iter: Iterator[T])
(implicit docConversion: T => Document,
 override implicit val kTag: ClassTag[T])
  extends AbstractLuceneRDDPartition[T]
  with IndexWithTaxonomyWriter
  with Logging {

  logInfo("Instance is created...")

  private val (iterOriginal, iterIndex) = iter.duplicate

  logInfo("Indexing process initiated...")
  iterIndex.foreach { case elem =>
    // (implicitly) convert type T to Lucene document
    val doc = docConversion(elem)
    indexWriter.addDocument(FacetsConfig.build(taxoWriter, doc))
  }
  logInfo("Indexing process completed...")

  // Close the indexWriter and taxonomyWriter (for faceted search)
  closeAllWriters()
  logDebug("Closing index writers...")


  logDebug("Instantiating index/facet readers")
  private val indexReader = DirectoryReader.open(IndexDir)
  private val indexSearcher = new IndexSearcher(indexReader)
  private val taxoReader = new DirectoryTaxonomyReader(TaxonomyDir)
  logDebug("Index readers instantiated successfully")
  logInfo(s"Indexed ${size} documents")

  override def fields(): Set[String] = {
    LuceneQueryHelpers.fields(indexSearcher)
  }

  override def size: Long = {
    LuceneQueryHelpers.totalDocs(indexSearcher)
  }

  override def isDefined(elem: T): Boolean = {
    iterOriginal.contains(elem)
  }

  override def multiTermQuery(docMap: Map[String, String],
                              topK: Int,
                              booleanClause: BooleanClause.Occur = BooleanClause.Occur.MUST)
  : Seq[SparkScoreDoc] = {
   LuceneQueryHelpers.multiTermQuery(indexSearcher, docMap, topK,
     booleanClause: BooleanClause.Occur)
  }

  override def iterator: Iterator[T] = {
    iterOriginal
  }

  override def filter(pred: T => Boolean): AbstractLuceneRDDPartition[T] =
    new LuceneRDDPartition(iterOriginal.filter(pred))(docConversion, kTag)

  override def termQuery(fieldName: String, fieldText: String,
                         topK: Int = 1): Iterable[SparkScoreDoc] = {
    LuceneQueryHelpers.termQuery(indexSearcher, fieldName, fieldText, topK)
  }

  override def query(searchString: String,
                     topK: Int): Iterable[SparkScoreDoc] = {
    LuceneQueryHelpers.searchParser(indexSearcher, searchString, topK)(Analyzer)
  }

  override def queries(searchStrings: Iterable[String],
                     topK: Int): Iterable[(String, Iterable[SparkScoreDoc])] = {
    searchStrings.map( searchString =>
      (searchString, query(searchString, topK))
    )
  }

  override def prefixQuery(fieldName: String, fieldText: String,
                           topK: Int): Iterable[SparkScoreDoc] = {
    LuceneQueryHelpers.prefixQuery(indexSearcher, fieldName, fieldText, topK)
   }

  override def fuzzyQuery(fieldName: String, fieldText: String,
                          maxEdits: Int, topK: Int): Iterable[SparkScoreDoc] = {
    LuceneQueryHelpers.fuzzyQuery(indexSearcher, fieldName, fieldText, maxEdits, topK)
  }

  override def phraseQuery(fieldName: String, fieldText: String,
                           topK: Int): Iterable[SparkScoreDoc] = {
    LuceneQueryHelpers.phraseQuery(indexSearcher, fieldName, fieldText, topK)(Analyzer)
  }

  override def facetQuery(searchString: String,
                          facetField: String,
                          topK: Int): SparkFacetResult = {
    LuceneQueryHelpers.facetedTextSearch(indexSearcher, taxoReader, FacetsConfig,
      searchString,
      facetField + LuceneRDD.FacetTextFieldSuffix,
      topK)(Analyzer)
  }
}

object LuceneRDDPartition {
  def apply[T: ClassTag]
      (iter: Iterator[T])(implicit docConversion: T => Document): LuceneRDDPartition[T] = {
    new LuceneRDDPartition[T](iter)(docConversion, classTag[T])
  }
}
