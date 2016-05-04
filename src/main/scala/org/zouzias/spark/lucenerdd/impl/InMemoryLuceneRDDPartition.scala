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

package org.zouzias.spark.lucenerdd.impl

import org.apache.lucene.document._
import org.apache.lucene.facet.FacetsConfig
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search._
import org.apache.spark.Logging
import org.zouzias.spark.lucenerdd.AbstractLuceneRDDPartition
import org.zouzias.spark.lucenerdd.analyze.{StdAnalyzer, WSAnalyzer}
import org.zouzias.spark.lucenerdd.models.{SparkFacetResult, SparkScoreDoc}
import org.zouzias.spark.lucenerdd.query.LuceneQueryHelpers
import org.zouzias.spark.lucenerdd.store.InMemoryIndexStorable

import scala.reflect.{ClassTag, _}
import scala.collection.JavaConverters._

private[lucenerdd] class InMemoryLuceneRDDPartition[T]
(private val iter: Iterator[T])
(implicit docConversion: T => Document,
 override implicit val kTag: ClassTag[T])
  extends AbstractLuceneRDDPartition[T]
  with InMemoryIndexStorable
  with WSAnalyzer
  with Logging {

  private lazy val indexWriter = new IndexWriter(IndexDir,
    new IndexWriterConfig(Analyzer)
    .setOpenMode(OpenMode.CREATE))

  private lazy val FacetsConfig = new FacetsConfig()

  private val (iterOriginal, iterIndex) = iter.duplicate

  iterIndex.foreach { case elem =>
    // (implicitly) convert type T to lucene document
    val doc = docConversion(elem)
    indexWriter.addDocument(FacetsConfig.build(doc))
  }

  indexWriter.commit()
  indexWriter.close()

  private val indexReader = DirectoryReader.open(IndexDir)
  private val indexSearcher = new IndexSearcher(indexReader)

  override def fields(): Set[String] = {
    LuceneQueryHelpers.fields(indexSearcher)
  }

  override def close(): Unit = {
    indexReader.close()
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
    new InMemoryLuceneRDDPartition(iterOriginal.filter(pred))(docConversion, kTag)

  override def termQuery(fieldName: String, fieldText: String,
                         topK: Int = 1): Iterable[SparkScoreDoc] = {
    LuceneQueryHelpers.termQuery(indexSearcher, fieldName, fieldText, topK)
  }

  override def query(searchString: String,
                     topK: Int): Iterable[SparkScoreDoc] = {
    LuceneQueryHelpers.searchParser(indexSearcher, searchString, topK)(Analyzer)
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
    LuceneQueryHelpers.phraseQuery(indexSearcher, fieldName, fieldText, topK)
  }

  /**
   * Faceted search query
   *
   * @param topK
   * @return
   */
  override def facetQuery(searchString: String,
                          facetField: String,
                          topK: Int): SparkFacetResult = {
    LuceneQueryHelpers.facetedSearch(indexSearcher, searchString, facetField, topK)(Analyzer)
  }
}

object InMemoryLuceneRDDPartition {
  def apply[T: ClassTag]
      (iter: Iterator[T])(implicit docConversion: T => Document): InMemoryLuceneRDDPartition[T] = {
    new InMemoryLuceneRDDPartition[T](iter)(docConversion, classTag[T])
  }
}
