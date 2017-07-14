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
package org.zouzias.spark.lucenerdd.query

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.facet.FacetField
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.zouzias.spark.lucenerdd.facets.FacetedLuceneRDD
import org.zouzias.spark.lucenerdd.store.IndexWithTaxonomyWriter

import scala.io.Source

class LuceneQueryHelpersSpec extends FlatSpec
  with IndexWithTaxonomyWriter
  with Matchers
  with BeforeAndAfterEach {

  // Load cities
  val countries = Source.fromFile("src/test/resources/countries.txt").getLines()
    .map(_.toLowerCase()).toSeq

  private val MaxFacetValue: Int = 10

  override def indexAnalyzer: Analyzer = getAnalyzer(Some("en"))

  countries.zipWithIndex.foreach { case (elem, index) =>
    val doc = convertToDoc(index % MaxFacetValue, elem)
    indexWriter.addDocument(FacetsConfig.build(taxoWriter, doc))
  }

  indexWriter.commit()
  taxoWriter.close()
  indexWriter.close()

  private val indexReader = DirectoryReader.open(IndexDir)
  private val indexSearcher = new IndexSearcher(indexReader)
  private lazy val taxoReader = new DirectoryTaxonomyReader(TaxonomyDir)


  private lazy val TestFacetName = s"_2${FacetedLuceneRDD.FacetTextFieldSuffix}"

  def convertToDoc(pos: Int, text: String): Document = {
    val doc = new Document()
    doc.add(new StringField("_1", text, Store.YES))
    doc.add(new FacetField(s"_1${FacetedLuceneRDD.FacetTextFieldSuffix}", text))
    doc.add(new IntPoint("_2", pos))
    doc.add(new StoredField("_2", pos))
    doc.add(new FacetField(TestFacetName, pos.toString))
    doc
  }

  "LuceneQueryHelpers.fields" should "return the list of fields" in {
    LuceneQueryHelpers.fields(indexSearcher) should equal (Set("_1", "_2"))
  }

  "LuceneQueryHelpers.totalDocs" should "return correct total document counts" in {
    LuceneQueryHelpers.totalDocs(indexSearcher) should equal (countries.size)
  }

  "LuceneQueryHelpers.facetedTextSearch" should "return correct facet counts" in {
    val facets = LuceneQueryHelpers.facetedTextSearch(indexSearcher, taxoReader,
      FacetsConfig, "*:*", TestFacetName, 100, indexAnalyzer)

    facets.facetName should equal(TestFacetName)
    facets.facets.size should equal(MaxFacetValue)
  }

  "LuceneQueryHelpers.termQuery" should "return correct documents" in {
    val greece = "greece"
    val topDocs = LuceneQueryHelpers.termQuery(indexSearcher, "_1", greece, 100)

    topDocs.size should equal(1)

    topDocs.exists(doc => doc.doc.textField("_1").forall(x =>
      x.toString().toLowerCase().contains(greece))) should equal(true)
  }

  "LuceneQueryHelpers.prefixQuery" should "return correct documents" in {
    val prefix = "gree"
    val topDocs = LuceneQueryHelpers.prefixQuery(indexSearcher, "_1", prefix, 100)

    topDocs.forall(doc => doc.doc.textField("_1").exists(x =>
      x.toString().toLowerCase().contains(prefix))) should equal(true)
  }
}
