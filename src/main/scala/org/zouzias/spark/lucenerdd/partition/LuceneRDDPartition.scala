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

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.document._
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader
import org.apache.lucene.index.{DirectoryReader, IndexReader}
import org.apache.lucene.search._
import org.joda.time.DateTime
import org.zouzias.spark.lucenerdd.facets.FacetedLuceneRDD
import org.zouzias.spark.lucenerdd.models.indexstats.{FieldStatistics, IndexStatistics}
import org.zouzias.spark.lucenerdd.models.{SparkFacetResult, TermVectorEntry}
import org.zouzias.spark.lucenerdd.query.{LuceneQueryHelpers, SimilarityConfigurable}
import org.zouzias.spark.lucenerdd.response.LuceneRDDResponsePartition
import org.zouzias.spark.lucenerdd.store.IndexWithTaxonomyWriter
import org.zouzias.spark.lucenerdd.LuceneRDD
import scala.collection.JavaConverters._

import scala.reflect.{ClassTag, _}
import scala.collection.mutable.ArrayBuffer

/**
  * A partition of [[LuceneRDD]]
  *
  * @param iter
  * @param partitionId
  * @param indexAnalyzerName
  * @param queryAnalyzerName
  * @param docConversion
  * @param kTag
  * @tparam T the type associated with each entry in the set.
  */
private[lucenerdd] class LuceneRDDPartition[T]
(private val iter: Iterator[T],
 private val partitionId: Int,
 private val indexAnalyzerName: String,
 private val queryAnalyzerName: String,
 private val similarityName: String,
 private val indexAnalyzerPerField: Map[String, String] = Map.empty,
 private val queryAnalyzerPerField: Map[String, String] = Map.empty)
(implicit docConversion: T => Document,
 override implicit val kTag: ClassTag[T])
  extends AbstractLuceneRDDPartition[T]
  with IndexWithTaxonomyWriter
  with SimilarityConfigurable {

  logInfo(s"[partId=${partitionId}] Partition is created...")

  override def indexAnalyzer(): Analyzer = getAnalyzer(Some(indexAnalyzerName))

  override def indexPerFieldAnalyzer(): PerFieldAnalyzerWrapper = {
    val analyzerPerField: Map[String, Analyzer] = indexAnalyzerPerField.mapValues(x =>
      getAnalyzer(Some(x)))
    new PerFieldAnalyzerWrapper(indexAnalyzer(), analyzerPerField.asJava)
  }

  private val QueryAnalyzer: Analyzer = getAnalyzer(Some(queryAnalyzerName))

  private def PerFieldQueryAnalyzer(): PerFieldAnalyzerWrapper = {
    val analyzerPerField: Map[String, Analyzer] = queryAnalyzerPerField.mapValues(x =>
      getAnalyzer(Some(x)))
    new PerFieldAnalyzerWrapper(QueryAnalyzer, analyzerPerField.asJava)
  }

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
  logDebug(s"[partId=${partitionId}]Closing index writers...")

  logDebug(s"[partId=${partitionId}]Instantiating index/facet readers")
  private val indexReader = DirectoryReader.open(IndexDir)
  private lazy val indexSearcher = initializeIndexSearcher(indexReader)
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

  private def initializeIndexSearcher(indexReader: IndexReader): IndexSearcher = {
    val searcher = new IndexSearcher(indexReader)
    searcher.setSimilarity(getSimilarity(Some(similarityName)))
    searcher
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
    new LuceneRDDPartition(iterOriginal.filter(pred),
      partitionId, indexAnalyzerName, queryAnalyzerName, similarityName)(docConversion, kTag)

  override def termQuery(fieldName: String, fieldText: String,
                         topK: Int = 1): LuceneRDDResponsePartition = {
    val results = LuceneQueryHelpers.termQuery(indexSearcher, fieldName, fieldText, topK)

    LuceneRDDResponsePartition(results.toIterator)
  }

  override def query(searchString: String,
                     topK: Int): LuceneRDDResponsePartition = {
    val results = LuceneQueryHelpers.searchParser(indexSearcher, searchString, topK,
      PerFieldQueryAnalyzer)

    LuceneRDDResponsePartition(results.toIterator)
  }

  override def query(query: Query,
                     topK: Int): LuceneRDDResponsePartition = {
    val results = LuceneQueryHelpers.searchQuery(indexSearcher, query, topK)

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
      .phraseQuery(indexSearcher, fieldName, fieldText, topK, QueryAnalyzer)

    LuceneRDDResponsePartition(results.toIterator)
  }

  override def facetQuery(searchString: String,
                          facetField: String,
                          topK: Int): SparkFacetResult = {
    LuceneQueryHelpers.facetedTextSearch(indexSearcher, taxoReader, FacetsConfig,
      searchString,
      facetField + FacetedLuceneRDD.FacetTextFieldSuffix,
      topK, QueryAnalyzer)
  }

  override def moreLikeThis(fieldName: String, query: String,
                            minTermFreq: Int, minDocFreq: Int, topK: Int)
  : LuceneRDDResponsePartition = {
    val docs = LuceneQueryHelpers.moreLikeThis(indexSearcher, fieldName,
      query, minTermFreq, minDocFreq, topK, QueryAnalyzer)
    LuceneRDDResponsePartition(docs)
  }

  override def termVectors(fieldName: String, idFieldName: Option[String])
  : Array[TermVectorEntry] = {
      val termDocMatrix = ArrayBuffer.empty[TermVectorEntry]

      // Iterate over document ids
      val docsIter = DocIdSetIterator.all(indexReader.maxDoc())
      var docId = docsIter.nextDoc()

      while(docId != DocIdSetIterator.NO_MORE_DOCS) {
        val termsOpt = Option(indexReader.getTermVector(docId, fieldName))

        // If there is no unique id field, i.e., idFieldName = None
        // unique id is (documentId, partitionId)
        val uniqueDocId = idFieldName match {
          case Some(idField) => (indexReader.document(docId).get(idField), partitionId)
          case None => (docId.toString, partitionId)
        }

        termsOpt.foreach { case terms =>

          // Iterate over terms of each document
          val termsIter = terms.iterator()
          while (termsIter.next() != null) {
            val termValue = termsIter.term().utf8ToString()
            val termFreq = termsIter.totalTermFreq()  // # of term occurrences in document
            termDocMatrix.append(TermVectorEntry(uniqueDocId, termValue, termFreq))
          }
        }
        // Next Lucene document
        docId = docsIter.nextDoc()
      }

      termDocMatrix.result().toArray[TermVectorEntry]
    }

  override def indexStats(fields: Set[String]): IndexStatistics = {
    val maxDocId = indexReader.maxDoc()
    val numDocs = indexReader.numDocs()
    val numDelDocs = indexReader.numDeletedDocs()
    val fieldsStats = fields.map(FieldStatistics(indexReader, _)).toArray

    IndexStatistics(partitionId, numDocs, maxDocId, numDelDocs, fields.size, fieldsStats)
  }
}

object LuceneRDDPartition {

  /**
    * Constructor for [[LuceneRDDPartition]]
    *
    * @param iter
    * @param partitionId
    * @param indexAnalyzer
    * @param queryAnalyzer
    * @param similarityName
    * @param docConversion
    * @tparam T
    * @return
    */
  def apply[T: ClassTag](iter: Iterator[T],
                         partitionId: Int,
                         indexAnalyzer: String,
                         queryAnalyzer: String,
                         similarityName: String)
                        (implicit docConversion: T => Document)
  : LuceneRDDPartition[T] = {
    new LuceneRDDPartition[T](iter, partitionId,
      indexAnalyzer, queryAnalyzer, similarityName)(docConversion, classTag[T])
  }
}
