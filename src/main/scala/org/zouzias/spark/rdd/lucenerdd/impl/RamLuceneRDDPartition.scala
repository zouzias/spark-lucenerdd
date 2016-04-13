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
import org.apache.lucene.document.{Document, Field, StringField}
import com.gilt.lucene.LuceneDocumentAdder._
import org.apache.lucene.index.Term
import org.zouzias.spark.rdd.lucenerdd.utils.MyLuceneDocumentLike

import scala.reflect.ClassTag

private[lucenerdd] class RamLuceneRDDPartition[T]
(private val iter: Iterator[T], val conversion: MyLuceneDocumentLike[T])
(override implicit val kTag: ClassTag[T])
  extends LuceneRDDPartition[T] with Logging {

  private lazy val index = new ReadableLuceneIndex
    with WritableLuceneIndex
    with LuceneStandardAnalyzer
    with RamLuceneDirectory


  iter.foreach{ case elem =>
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
                         topK: Int = 1): Iterable[Document] = {
    val term = new Term(fieldName, fieldText)
    val qr = new TermQuery(term)
    index.searchTopDocuments(qr, topK)
  }

  override def diff(other: LuceneRDDPartition[T]): LuceneRDDPartition[T] = ???

  override def diff(other: Iterator[T]): LuceneRDDPartition[T] = ???

  override def query(q: Query, topK: Int): Iterable[Document] = {
    index.searchTopDocuments(q, topK)
  }

  override def prefixQuery(fieldName: String, fieldText: String, topK: Int): Iterable[Document] = {
    val term = new Term(fieldName, fieldText)
    val qr = new PrefixQuery(term)
    index.searchTopDocuments(qr, topK)
  }

  override def fuzzyQuery(fieldName: String, fieldText: String,
                          maxEdits: Int, topK: Int): Iterable[Document] = {
    val term = new Term(fieldName, fieldText)
    val qr = new FuzzyQuery(term, maxEdits)
    index.searchTopDocuments(qr, topK)
  }

  override def phraseQuery(fieldName: String, fieldText: String, topK: Int): Iterable[Document] = {
    val term = new Term(fieldName, fieldText)
    val qr = new PhraseQuery()
    qr.add(term)
    index.searchTopDocuments(qr, topK)
  }
}

object RamLuceneRDDPartition {

  def apply[T: ClassTag]
      (iter: Iterator[T], conversion: MyLuceneDocumentLike[T]): RamLuceneRDDPartition[T] = {
    new RamLuceneRDDPartition[T](iter, conversion)
  }

  val stringConversion = new MyLuceneDocumentLike[String] {
    def toDocuments(value: String): Iterable[Document] = {
      val doc = new Document
      doc.add(new StringField("value", value.toString, Field.Store.YES))
      Seq(doc)
    }
  }
}
