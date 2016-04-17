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

package org.zouzias.spark.rdd.lucenerdd.impl

import org.apache.spark.Logging
import org.zouzias.spark.rdd.lucenerdd.LuceneRDDPartition
import org.apache.lucene.search._
import com.gilt.lucene.{LuceneStandardAnalyzer, RamLuceneDirectory, ReadableLuceneIndex, WritableLuceneIndex}
import org.apache.lucene.document.{Document, Field, SortedSetDocValuesField, StringField}
import com.gilt.lucene.LuceneDocumentAdder._
import org.apache.lucene.facet.{Facets, FacetsCollector}
import org.apache.lucene.facet.sortedset.{DefaultSortedSetDocValuesReaderState, SortedSetDocValuesFacetCounts, SortedSetDocValuesReaderState}
import org.apache.lucene.index.Term
import org.apache.lucene.util.BytesRef
import org.zouzias.spark.rdd.lucenerdd.utils.{MyLuceneDocumentLike, SerializedDocument}

import scala.reflect.ClassTag

private[lucenerdd] class RamLuceneRDDPartition[T]
(private val iter: Iterator[T], val conversion: MyLuceneDocumentLike[T])
(override implicit val kTag: ClassTag[T])
  extends LuceneRDDPartition[T] with Logging {

  private lazy val index = new ReadableLuceneIndex
    with WritableLuceneIndex
    with LuceneStandardAnalyzer
    with RamLuceneDirectory


  iter.foreach { case elem =>
    // Convert it to lucene document
    index.addDocuments(conversion.toDocuments(elem))
  }

  override def size: Long = index.allDocuments.size

  override def isDefined(elem: T): Boolean = iter.contains(elem)

  override def iterator: Iterator[T] = {
    index.allDocuments.map(_.toString.asInstanceOf[T]).toIterator
  }

  override def filter(pred: T => Boolean): LuceneRDDPartition[T] =
    new RamLuceneRDDPartition(iter.filter(pred), conversion)

  override def termQuery(fieldName: String, fieldText: String,
                         topK: Int = 1): Iterable[SerializedDocument] = {
    val term = new Term(fieldName, fieldText)
    val qr = new TermQuery(term)
    index.searchTopDocuments(qr, topK).map(SerializedDocument(_))
  }

  override def diff(other: LuceneRDDPartition[T]): LuceneRDDPartition[T] = ???

  override def diff(other: Iterator[T]): LuceneRDDPartition[T] = ???

  override def query(q: Query, topK: Int): Iterable[SerializedDocument] = {
    index.searchTopDocuments(q, topK).map(SerializedDocument(_))
  }

  override def prefixQuery(fieldName: String, fieldText: String,
                           topK: Int): Iterable[SerializedDocument] = {
    val term = new Term(fieldName, fieldText)
    val qr = new PrefixQuery(term)
    index.searchTopDocuments(qr, topK).map(SerializedDocument(_))
  }

  override def fuzzyQuery(fieldName: String, fieldText: String,
                          maxEdits: Int, topK: Int): Iterable[SerializedDocument] = {
    val term = new Term(fieldName, fieldText)
    val qr = new FuzzyQuery(term, maxEdits)
    index.searchTopDocuments(qr, topK).map(SerializedDocument(_))
  }

  override def phraseQuery(fieldName: String, fieldText: String,
                           topK: Int): Iterable[SerializedDocument] = {
    val term = new Term(fieldName, fieldText)
    val qr = new PhraseQuery()
    qr.add(term)
    index.searchTopDocuments(qr, topK).map(SerializedDocument(_))
  }

  override def facetedQuery(query: Query,
                            fieldName: String,
                            topK: Int): Option[Map[String, Long]] = ???
  /* {

    index.withIndexSearcher[Option[Map[String, Long]]]{indexSearcherOption =>
      indexSearcherOption.map { case indexSearcher =>
        val indexReader = indexSearcher.getIndexReader
        val state = new DefaultSortedSetDocValuesReaderState(indexReader, s"${fieldName}_facet")
        val fc = new FacetsCollector()

        FacetsCollector.search(indexSearcher, query, topK, fc)

        val facets: Facets = new SortedSetDocValuesFacetCounts(state, fc)
        facets.getTopChildren(topK, "0", s"${fieldName}_facet")
          .labelValues
          .map { case facetResult =>
          facetResult.label -> facetResult.value.longValue()
        }.toMap[String, Long]
      }
    }
  } */

  override def facetedQuery(fieldName: String, topK: Int): Option[Map[String, Long]] = {
    facetedQuery(new MatchAllDocsQuery, fieldName, topK)
  }
}

object RamLuceneRDDPartition {

  def apply[T: ClassTag]
      (iter: Iterator[T], conversion: MyLuceneDocumentLike[T]): RamLuceneRDDPartition[T] = {
    new RamLuceneRDDPartition[T](iter, conversion)
  }

  val stringConversion = new MyLuceneDocumentLike[String] {

    def defaultField: String = "value"

    override def toDocuments(value: String): Iterable[Document] = {
      val doc = new Document
      doc.add(new StringField(defaultField, value.toString, Field.Store.YES))
      // required for facet search
      // doc.add(new SortedSetDocValuesField(s"${defaultField}_facet",
      // new BytesRef(value.toString)))
      Seq(doc)
    }
  }
}
