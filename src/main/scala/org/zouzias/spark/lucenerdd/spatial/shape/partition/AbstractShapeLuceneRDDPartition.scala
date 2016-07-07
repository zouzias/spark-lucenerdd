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
package org.zouzias.spark.lucenerdd.spatial.shape.partition

import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

import scala.reflect.ClassTag

abstract class AbstractShapeLuceneRDDPartition[K, V] extends Serializable {

  protected implicit def kTag: ClassTag[K]
  protected implicit def vTag: ClassTag[V]

  def size: Long

  def iterator: Iterator[(K, V)]

  def isDefined(key: K): Boolean

  def close(): Unit

  /**
   * Nearest neighbour search
   *
   * @param point query point
   * @param k number of neighbors to return
   * @param searchString Lucene Query string
   * @return
   */
  def knnSearch(point: (Double, Double), k: Int,
                searchString: String): List[SparkScoreDoc]

  /**
   * Search for points within a circle
   *
   * @param center center of circle
   * @param radius radius of circle in kilometers (KM)
   * @param k number of points to return
   * @return
   */
  def circleSearch(center: (Double, Double),
                   radius: Double, k: Int, operationName: String)
  : Iterable[SparkScoreDoc]

  /**
   * Spatial search with arbitrary shape
   *
   * @param shapeAsString
   * @param k
   * @param operationName
   * @return
   */
  def spatialSearch(shapeAsString: String, k: Int, operationName: String)
  : Iterable[SparkScoreDoc]

  /**
   * Spatial search with point
   *
   * @param point
   * @param k
   * @param operationName
   * @return
   */
  def spatialSearch(point: (Double, Double), k: Int, operationName: String)
  : Iterable[SparkScoreDoc]

  /**
   * Bounding box search with point and radius
   *
   * @param center given as (x, y)
   * @param radius distance from center in kilometers (KM)
   * @param k
   * @param operationName
   * @return
   */
  def bboxSearch(center: (Double, Double),
                 radius: Double, k: Int, operationName: String)
  : Iterable[SparkScoreDoc]

  /**
   * Bounding box search with lower left and upper right corners
   *
   * @param lowerLeft
   * @param upperRight
   * @param k
   * @param operationName
   * @return
   */
  def bboxSearch(lowerLeft: (Double, Double),
                 upperRight: (Double, Double), k: Int,
                 operationName: String) : Iterable[SparkScoreDoc]

  /**
   * Restricts the entries to those satisfying a predicate
   *
   * @param pred Predicate
   * @return
   */
  def filter(pred: (K, V) => Boolean): AbstractShapeLuceneRDDPartition[K, V]
}
