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

import java.io.File
import java.nio.file.{Path, Paths}

import org.apache.lucene.facet.FacetsConfig
import org.apache.lucene.store._
import org.zouzias.spark.lucenerdd.config.Configurable

/**
 * In memory lucene index
 */
trait IndexStorable extends Configurable
  with AutoCloseable {

  protected lazy val FacetsConfig = new FacetsConfig()

  private val IndexStoreKey = "lucenerdd.index.store.mode"


  private val indexDirName = s"indexDirectory-${System.currentTimeMillis()}"
  private val indexDir = Paths.get(indexDirName)

  private val taxonomyDirName = s"taxonomyDirectory-${System.currentTimeMillis()}"
  private val taxonomyDir = Paths.get(taxonomyDirName)

  private val lockFactory = new SingleInstanceLockFactory


  protected def storageMode(directoryPath: Path): Directory = {
    if (config.hasPath(IndexStoreKey)) {
      val storageMode = config.getString(IndexStoreKey)

      storageMode match {
        case "disk" => new MMapDirectory(directoryPath, lockFactory)
        case _ => new RAMDirectory()
      }
    }
    else {
      new RAMDirectory()
    }
  }

  /**
   * There is an issue that multiple workers are using the same directory.
   */

  protected val IndexDir = storageMode(indexDir)

  protected val TaxonomyDir = storageMode(taxonomyDir)


  override def close(): Unit = {
    IndexDir.close()
    TaxonomyDir.close()
  }
}
