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
import org.joda.time.DateTime
import org.zouzias.spark.lucenerdd.facets.FacetedLuceneRDD
import org.zouzias.spark.lucenerdd.models.{SparkFacetResult, TermVectorEntry}
import org.zouzias.spark.lucenerdd.query.LuceneQueryHelpers
import org.zouzias.spark.lucenerdd.response.LuceneRDDResponsePartition
import org.zouzias.spark.lucenerdd.store.IndexWithTaxonomyWriter

import scala.reflect.{ClassTag, _}
import scala.collection.mutable.ArrayBuffer

private[lucenerdd] class LuceneRDDPartition[T]
(private val iter: Iterator[T], private val partitionId: Int)
(implicit docConversion: T => Document,
 override implicit val kTag: ClassTag[T])
  extends AbstractLuceneRDDPartition[T]
  with IndexWithTaxonomyWriter {

  logInfo(s"[partId=${partitionId}] Partition is created...")

  private val (iterOriginal, iterIndex) = iter.duplicate

  private val startTime = new DateTime(System.currentTimeMillis())
  logInfo(s"[partId=${partitionId}]Indexing process initiated at ${startTime}...")
  iterIndex.foreach { case elem =>
    // (implicitly) convert type T to Lucene document
    val doc = docConversion(elem)
    indexWriter.addDocument(FacetsConfig.build(taxoWriter, doc))
  }
  private val endTime = new DateTime(System.currentTimeMillis())
  logInfo(s"[partId=${partitionId}]Indexing process completed at ${endTime}...")
  logInfo(s"[partId=${partitionId}]Indexing process took ${(endTime.getMillis
    - startTime.getMillis) / 1000} seconds...")

  // Close the indexWriter and taxonomyWriter (for faceted search)
  closeAllWriters()
  logDebug("[partId=${partitionId}]Closing index writers...")


  logDebug(s"[partId=${partitionId}]Instantiating index/facet readers")
  private val indexReader = DirectoryReader.open(IndexDir)
  private val indexSearcher = new IndexSearcher(indexReader)
  private val taxoReader = new DirectoryTaxonomyReader(TaxonomyDir)
  logDebug(s"[partId=${partitionId}]Index readers instantiated successfully")
  logInfo(s"[partId=${partitionId}]Indexed ${size} documents")

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
  : LuceneRDDResponsePartition = {
   val results = LuceneQueryHelpers.multiTermQuery(indexSearcher, docMap, topK,
     booleanClause: BooleanClause.Occur)

    LuceneRDDResponsePartition(results.toIterator)
  }

  override def iterator: Iterator[T] = {
    iterOriginal
  }

  override def filter(pred: T => Boolean): AbstractLuceneRDDPartition[T] =
    new LuceneRDDPartition(iterOriginal.filter(pred), partitionId)(docConversion, kTag)

  override def termQuery(fieldName: String, fieldText: String,
                         topK: Int = 1): LuceneRDDResponsePartition = {
    val results = LuceneQueryHelpers.termQuery(indexSearcher, fieldName, fieldText, topK)

    LuceneRDDResponsePartition(results.toIterator)
  }

  override def query(searchString: String,
                     topK: Int): LuceneRDDResponsePartition = {
    val results = LuceneQueryHelpers.searchParser(indexSearcher, searchString, topK)(Analyzer)

    LuceneRDDResponsePartition(results.toIterator)
  }

  override def queries(searchStrings: Iterable[String],
                     topK: Int): Iterable[(String, LuceneRDDResponsePartition)] = {
    searchStrings.map( searchString =>
      (searchString, query(searchString, topK))
    )
  }

  override def prefixQuery(fieldName: String, fieldText: String,
                           topK: Int): LuceneRDDResponsePartition = {
    val results = LuceneQueryHelpers.prefixQuery(indexSearcher, fieldName, fieldText, topK)

    LuceneRDDResponsePartition(results.toIterator)
  }

  override def fuzzyQuery(fieldName: String, fieldText: String,
                          maxEdits: Int, topK: Int): LuceneRDDResponsePartition = {
    val results = LuceneQueryHelpers
      .fuzzyQuery(indexSearcher, fieldName, fieldText, maxEdits, topK)

    LuceneRDDResponsePartition(results.toIterator)
  }

  override def phraseQuery(fieldName: String, fieldText: String,
                           topK: Int): LuceneRDDResponsePartition = {
    val results = LuceneQueryHelpers
      .phraseQuery(indexSearcher, fieldName, fieldText, topK)(Analyzer)

    LuceneRDDResponsePartition(results.toIterator)
  }

  override def facetQuery(searchString: String,
                          facetField: String,
                          topK: Int): SparkFacetResult = {
    LuceneQueryHelpers.facetedTextSearch(indexSearcher, taxoReader, FacetsConfig,
      searchString,
      facetField + FacetedLuceneRDD.FacetTextFieldSuffix,
      topK)(Analyzer)
  }

  override def moreLikeThis(fieldName: String, query: String,
                            minTermFreq: Int, minDocFreq: Int, topK: Int)
  : LuceneRDDResponsePartition = {
    val docs = LuceneQueryHelpers.moreLikeThis(indexSearcher, fieldName,
      query, minTermFreq, minDocFreq, topK)(Analyzer)
    LuceneRDDResponsePartition(docs)
  }

  override def termVectors(fieldName: String): Array[TermVectorEntry] = {
      val termDocMatrix = ArrayBuffer.empty[TermVectorEntry]

      // Iterate over document ids
      val docsIter = DocIdSetIterator.all(indexReader.maxDoc())
      var docId = docsIter.nextDoc()

      while(docId != DocIdSetIterator.NO_MORE_DOCS) {
        val termsOpt = Option(indexReader.getTermVector(docId, fieldName))
        termsOpt.foreach { case terms =>

          // Iterate over terms of each document
          val termsIter = terms.iterator()
          while (termsIter.next() != null) {
            val termValue = termsIter.term().utf8ToString()
            val termFreq = termsIter.docFreq()
            termDocMatrix.append(TermVectorEntry((docId, partitionId), termValue, termFreq))
          }
        }
        // Next Lucene document
        docId = docsIter.nextDoc()
      }

      termDocMatrix.result().toArray[TermVectorEntry]
    }
}

object LuceneRDDPartition {
  def apply[T: ClassTag](iter: Iterator[T], partitionId: Int)(implicit docConversion: T => Document)
  : LuceneRDDPartition[T] = {
    new LuceneRDDPartition[T](iter, partitionId)(docConversion, classTag[T])
  }
}
