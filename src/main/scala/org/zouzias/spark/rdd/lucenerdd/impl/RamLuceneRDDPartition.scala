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
import org.apache.lucene.document._
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.store.RAMDirectory
import org.zouzias.spark.rdd.lucenerdd.utils.{LuceneHelpers, SerializedDocument}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect._

private[lucenerdd] class RamLuceneRDDPartition[T]
(private val iter: Iterator[T])
(implicit docConversion: T => Document, override implicit val kTag: ClassTag[T])
  extends LuceneRDDPartition[T] with Logging {

  private val indexDir = new RAMDirectory()
  private lazy val indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(
    new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE_OR_APPEND))

  iter.foreach { case elem =>
    // Convert it to lucene document
    indexWriter.addDocument(docConversion(elem))
  }

  indexWriter.commit()
  indexWriter.close()

  private val indexReader = DirectoryReader.open(indexDir)
  private val indexSearcher = new IndexSearcher(indexReader)

  override def size: Long = {
    LuceneHelpers.totalDocs(indexSearcher)
  }

  override def isDefined(elem: T): Boolean = {
    val doc: Document = docConversion(elem)
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
    new RamLuceneRDDPartition(iter.filter(pred))(docConversion, kTag)

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
      (iter: Iterator[T])(implicit docConversion: T => Document): RamLuceneRDDPartition[T] = {
    new RamLuceneRDDPartition[T](iter)(docConversion, classTag[T])
  }

  implicit def intToDocument(v: Int): Document = {
    val doc = new Document
    doc.add(new IntField("_1", v, Field.Store.YES))
    doc
  }

  implicit def longToDocument(v: Long): Document = {
    val doc = new Document
    doc.add(new LongField("_1", v, Field.Store.YES))
    doc
  }

  implicit def doubleToDocument(v: Double): Document = {
    val doc = new Document
    doc.add(new DoubleField("_1", v, Field.Store.YES))
    doc
  }

  implicit def floatToDocument(v: Float): Document = {
    val doc = new Document
    doc.add(new FloatField("_1", v, Field.Store.YES))
    doc
  }

  implicit def stringToDocument(s: String): Document = {
    val doc = new Document
    doc.add(new StringField("_1", s, Field.Store.YES))
    doc
  }

  private def typeToDocument[T: ClassTag](doc: Document, index: Int, s: T): Document = {
    s match {
      case x: String =>
        doc.add(new StringField(s"_${index}", x, Field.Store.YES))
      case x: Int =>
        doc.add(new IntField(s"_${index}", x, Field.Store.YES))
      case x: Double =>
        doc.add(new DoubleField(s"_${index}", x, Field.Store.YES))
      case x: Float =>
        doc.add(new FloatField(s"_${index}", x, Field.Store.YES))
      case x: Long =>
        doc.add(new LongField(s"_${index}", x, Field.Store.YES))
    }

    doc
  }

  implicit def tuple2ToDocument[T1: ClassTag, T2: ClassTag](s: (T1, T2)): Document = {
    val doc = new Document
    typeToDocument[T1](doc, 1, s._1)
    typeToDocument[T2](doc, 2, s._2)
    doc
  }

  implicit def tuple3ToDocument[T1: ClassTag,
                                T2: ClassTag,
                                T3: ClassTag](s: (T1, T2, T3)): Document = {
    val doc = new Document
    typeToDocument[T1](doc, 1, s._1)
    typeToDocument[T2](doc, 2, s._2)
    typeToDocument[T3](doc, 3, s._3)
    doc
  }

}
