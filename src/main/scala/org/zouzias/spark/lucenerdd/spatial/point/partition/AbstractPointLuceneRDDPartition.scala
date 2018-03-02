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
package org.zouzias.spark.lucenerdd.spatial.point.partition

import org.zouzias.spark.lucenerdd.models.BoundingBox
import org.zouzias.spark.lucenerdd.response.LuceneRDDResponsePartition
import org.zouzias.spark.lucenerdd.spatial.shape.ShapeLuceneRDD.PointType

import scala.reflect.ClassTag


private[point] abstract class AbstractPointLuceneRDDPartition[V] extends Serializable {

  protected implicit def vTag: ClassTag[V]

  def size: Long

  def iterator: Iterator[(PointType, V)]

  def isDefined(point: PointType): Boolean

  def close(): Unit

  /**
    * Return smallest enclosing axis aligned bounding box
    * @return Smallest enclosing bounding box
    */
  def bounds(): BoundingBox

  /**
   * Nearest neighbour search
   *
   * @param point query point
   * @param k number of neighbors to return
   * @param searchString Lucene Query string
   * @return
   */
  def knnSearch(point: PointType, k: Int, searchString: String): LuceneRDDResponsePartition

  /**
   * Search for points within a circle
   *
   * @param center center of circle
   * @param radius radius of circle in kilometers (KM)
   * @param k number of points to return
   * @return
   */
  def circleSearch(center: PointType, radius: Double, k: Int, operationName: String)
  : LuceneRDDResponsePartition

  /**
   * Spatial search with arbitrary shape
   *
   * @param shapeAsString Shape object represented as String
   * @param k Number of results to return
   * @param operationName Operation name, i.e., intersect, within, etc
   * @return
   */
  def spatialSearch(shapeAsString: String, k: Int, operationName: String)
  : LuceneRDDResponsePartition

  /**
   * Bounding box search with point and radius
   *
   * @param center given as (x, y)
   * @param radius distance from center in kilometers (KM)
   * @param k Number of results to return
   * @param operationName Operation name, i.e., intersect, within, etc
   * @return
   */
  def bboxSearch(center: PointType, radius: Double, k: Int, operationName: String)
  : LuceneRDDResponsePartition

  /**
   * Bounding box search with lower left and upper right corners
   *
   * @param lowerLeft Lower left point
   * @param upperRight Upper left point
   * @param k Number of results
   * @param operationName Operation name, i.e., intersect, within, etc
   * @return
   */
  def bboxSearch(lowerLeft: PointType, upperRight: PointType, k: Int, operationName: String)
  : LuceneRDDResponsePartition

  /**
   * Restricts the entries to those satisfying a predicate
   *
   * @param pred Predicate to filter on
   * @return
   */
  def filter(pred: (PointType, V) => Boolean): AbstractPointLuceneRDDPartition[V]
}
