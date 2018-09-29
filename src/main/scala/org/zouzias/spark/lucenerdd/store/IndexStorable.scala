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

import java.nio.file.{Files, Path}

import org.apache.lucene.facet.FacetsConfig
import org.apache.lucene.store._
import org.zouzias.spark.lucenerdd.config.Configurable
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Storage of a Lucene index Directory
 *
 * Currently, the following storage methods are supported:
 *
 * 1) "lucenerdd.index.store.mode=disk" : MMapStorage on temp disk
 * 2) Otherwise, memory storage using [[RAMDirectory]]
 */
trait IndexStorable extends Configurable
  with AutoCloseable
  with Logging {

  protected lazy val FacetsConfig = new FacetsConfig()

  private val IndexStoreKey = "lucenerdd.index.store.mode"

  private val tmpJavaDir = System.getProperty("java.io.tmpdir")

  private val indexDirName =
    s"indexDirectory.${System.currentTimeMillis()}.${Thread.currentThread().getId}"

  private val indexDir = Files.createTempDirectory(indexDirName)

  private val taxonomyDirName =
    s"taxonomyDirectory-${System.currentTimeMillis()}.${Thread.currentThread().getId}"

  private val taxonomyDir = Files.createTempDirectory(taxonomyDirName)

  protected val IndexDir = storageMode(indexDir)

  protected val TaxonomyDir = storageMode(taxonomyDir)

  /**
   * Select Lucene index storage implementation based on config
   * @param directoryPath Directory in disk to store index
   * @return
   */
  protected def storageMode(directoryPath: Path): Directory = {
    if (Config.hasPath(IndexStoreKey)) {
      val storageMode = Config.getString(IndexStoreKey)

      storageMode match {
          // TODO: FIX: Currently there is a single lock instance for each directory.
          // TODO: Implement better lock handling here
        case "disk" => {
          logInfo(s"Config parameter ${IndexStoreKey} is set to 'disk'")
          logInfo("Lucene index will be storage in disk")
          logInfo(s"Index disk location ${tmpJavaDir}")
          // directoryPath.toFile.deleteOnExit() // Delete on exit
          new MMapDirectory(directoryPath, new SingleInstanceLockFactory)
        }
        case ow =>
          logInfo(s"Config parameter ${IndexStoreKey} is set to ${ow}")
          logInfo("Lucene index will be storage in memory (default)")
          logInfo(
            """
              Quoting from
              http://lucene.apache.org/core/7_5_0/core/org/apache/
              lucene/store/RAMDirectory.html

              A memory-resident Directory implementation. Locking
              implementation is by default the SingleInstanceLockFactory.
              Warning: This class is not intended to work with huge indexes.
              Everything beyond several hundred megabytes will waste resources
              (GC cycles), because it uses an internal buffer size of 1024 bytes,
              producing millions of byte[1024] arrays.
              This class is optimized for small memory-resident indexes.
              It also has bad concurrency on multithreaded environments.

              It is recommended to materialize large indexes on disk and
              use MMapDirectory, which is a high-performance directory
              implementation working directly on the file system cache of
              the operating system, so copying data to Java heap
              space is not useful.
            """.stripMargin)
          new RAMDirectory()
      }
    }
    else {
      logInfo(s"Config parameter ${IndexStoreKey} is not set")
      logInfo("Lucene index will be storage in disk")
      new MMapDirectory(directoryPath, new SingleInstanceLockFactory)
    }
  }

  override def close(): Unit = {
    IndexDir.close()
    TaxonomyDir.close()
  }
}
