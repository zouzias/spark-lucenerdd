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
import org.zouzias.spark.rdd.lucenerdd.model.SparkScoreDoc
import org.zouzias.spark.rdd.lucenerdd.query.LuceneQueryHelpers

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect._

private[lucenerdd] class RamLuceneRDDPartition[T]
(private val iter: Iterator[T])
(implicit docConversion: T => Document,
 override implicit val kTag: ClassTag[T])
  extends LuceneRDDPartition[T] with Logging {

  private lazy val indexDir = new RAMDirectory()
  private lazy val indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(
    new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE_OR_APPEND))

  private val (iterOriginal, iterIndex) = iter.duplicate

  iterIndex.foreach { case elem =>
    // Convert it to lucene document
    indexWriter.addDocument(docConversion(elem))
  }

  indexWriter.commit()
  indexWriter.close()

  private val indexReader = DirectoryReader.open(indexDir)
  private val indexSearcher = new IndexSearcher(indexReader)

  override def size: Long = {
    LuceneQueryHelpers.totalDocs(indexSearcher)
  }

  override def isDefined(elem: T): Boolean = {
    val doc: Document = docConversion(elem)
    query(doc.getFields.asScala.map(x => x.name() -> x.stringValue()).toMap)
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

  override def iterator: Iterator[T] = {
    iterOriginal
  }

  override def filter(pred: T => Boolean): LuceneRDDPartition[T] =
    new RamLuceneRDDPartition(iter.filter(pred))(docConversion, kTag)

  override def termQuery(fieldName: String, fieldText: String,
                         topK: Int = 1): Iterable[SparkScoreDoc] = {
    LuceneQueryHelpers.termQuery(indexSearcher, fieldName, fieldText, topK)
  }

  override def query(q: Query, topK: Int): Iterable[SparkScoreDoc] = {
    LuceneQueryHelpers.searchTopK(indexSearcher, q, topK)
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
}

object RamLuceneRDDPartition {

  def apply[T: ClassTag]
      (iter: Iterator[T])(implicit docConversion: T => Document): RamLuceneRDDPartition[T] = {
    new RamLuceneRDDPartition[T](iter)(docConversion, classTag[T])
  }
}
