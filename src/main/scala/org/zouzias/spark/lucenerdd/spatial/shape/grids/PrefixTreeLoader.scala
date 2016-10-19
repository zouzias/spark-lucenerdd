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
package org.zouzias.spark.lucenerdd.spatial.shape.grids

import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTreeFactory
import org.zouzias.spark.lucenerdd.config.ShapeLuceneRDDConfigurable
import org.zouzias.spark.lucenerdd.spatial.shape.context.ContextLoader

import scala.collection.JavaConverters._

trait PrefixTreeLoader extends ContextLoader
  with ShapeLuceneRDDConfigurable {

  // results in sub-meter precision for geohash
  protected val maxLevels: Int = getPrefixTreeMaxLevel

  // Excepting 'geohash' or 'quad'
  protected val prefixTreeName: String = getPrefixTreeName

  // Maximum distance error (in KM)
  protected val prefixTreeMaxDistErr: Double = getPrefixTreeMaxDistErr

  // This can also be constructed from SpatialPrefixTreeFactory
  protected val grid = SpatialPrefixTreeFactory.makeSPT(
    Map("prefixTree" -> prefixTreeName,
    "maxLevels" -> maxLevels.toString,
    "maxDistErr" -> prefixTreeMaxDistErr.toString).asJava,
    ClassLoader.getSystemClassLoader,
    ctx)
}
