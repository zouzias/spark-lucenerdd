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
package org.zouzias.spark.lucenerdd.store

import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.zouzias.spark.lucenerdd.analyzers.AnalyzerConfigurable

/**
 * Index and Taxonomy Writer
 */
trait IndexWithTaxonomyWriter extends IndexStorable
  with AnalyzerConfigurable {

  protected lazy val indexWriter = new IndexWriter(IndexDir,
    new IndexWriterConfig(Analyzer)
      .setOpenMode(OpenMode.CREATE))

  protected lazy val taxoWriter = new DirectoryTaxonomyWriter(TaxonomyDir)

  protected def closeAllWriters(): Unit = {
    indexWriter.commit()
    taxoWriter.commit()
    taxoWriter.close()
    indexWriter.close()
  }

}
