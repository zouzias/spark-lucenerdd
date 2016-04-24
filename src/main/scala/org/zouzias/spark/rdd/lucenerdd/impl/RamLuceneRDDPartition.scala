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
import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.store.RAMDirectory
import org.zouzias.spark.rdd.lucenerdd.utils.{LuceneHelpers, MyLuceneDocumentLike, SerializedDocument}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private[lucenerdd] class RamLuceneRDDPartition[T]
(private val iter: Iterator[T], val conversion: MyLuceneDocumentLike[T])
(override implicit val kTag: ClassTag[T])
  extends LuceneRDDPartition[T] with Logging {

  private val indexDir = new RAMDirectory()
  private lazy val indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(
    new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE_OR_APPEND))

  iter.foreach { case elem =>
    // Convert it to lucene document
    indexWriter.addDocument(conversion.toDocument(elem))
  }

  indexWriter.commit()
  indexWriter.close()

  private val indexReader = DirectoryReader.open(indexDir)
  private val indexSearcher = new IndexSearcher(indexReader)

  override def size: Long = {
    LuceneHelpers.totalDocs(indexSearcher)
  }

  override def isDefined(elem: T): Boolean = {
    val doc: Document = conversion.toDocument(elem)
    this.query(doc.getFields.asScala.map(x => x.name() -> x.stringValue()).toMap)
  }

  override def query(docMap: Map[String, String]): Boolean = {
    val terms = docMap.map{ case (field, fieldValue) =>
      new TermQuery(new Term(field, fieldValue))
    }

    val builder = new BooleanQuery.Builder()
    terms.foreach{ case termQuery =>
      builder.add(termQuery, BooleanClause.Occur.MUST)
    }

    indexSearcher.search(builder.build(), 1).totalHits > 0
  }

  override def iterator: Iterator[T] = ???

  override def filter(pred: T => Boolean): LuceneRDDPartition[T] =
    new RamLuceneRDDPartition(iter.filter(pred), conversion)

  override def termQuery(fieldName: String, fieldText: String,
                         topK: Int = 1): Iterable[SerializedDocument] = {
    val term = new Term(fieldName, fieldText)
    val qr = new TermQuery(term)
    LuceneHelpers.searchTopKDocs(indexSearcher, qr, topK).map(SerializedDocument(_))
  }

  override def query(q: Query, topK: Int): Iterable[SerializedDocument] = {
    LuceneHelpers.searchTopKDocs(indexSearcher, q, topK).map(SerializedDocument(_))
  }

  override def prefixQuery(fieldName: String, fieldText: String,
                           topK: Int): Iterable[SerializedDocument] = {
    val term = new Term(fieldName, fieldText)
    val qr = new PrefixQuery(term)
    LuceneHelpers.searchTopKDocs(indexSearcher, qr, topK).map(SerializedDocument(_))
  }

  override def fuzzyQuery(fieldName: String, fieldText: String,
                          maxEdits: Int, topK: Int): Iterable[SerializedDocument] = {
    val term = new Term(fieldName, fieldText)
    val qr = new FuzzyQuery(term, maxEdits)
    LuceneHelpers.searchTopKDocs(indexSearcher, qr, topK).map(SerializedDocument(_))
  }

  override def phraseQuery(fieldName: String, fieldText: String,
                           topK: Int): Iterable[SerializedDocument] = {
    val term = new Term(fieldName, fieldText)
    val builder = new PhraseQuery.Builder()
    builder.add(term)
    LuceneHelpers.searchTopKDocs(indexSearcher, builder.build(), topK).map(SerializedDocument(_))
  }
}

object RamLuceneRDDPartition {

  def apply[T: ClassTag]
      (iter: Iterator[T], conversion: MyLuceneDocumentLike[T]): RamLuceneRDDPartition[T] = {
    new RamLuceneRDDPartition[T](iter, conversion)
  }

  val stringConversion = new MyLuceneDocumentLike[String] {

    def defaultField: String = "value"

    override def toDocument(value: String): Document = {
      val doc = new Document
      doc.add(new StringField(defaultField, value.toString, Field.Store.YES))
      doc
    }
  }
}
