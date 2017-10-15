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

import org.apache.lucene.document.Document
import org.zouzias.spark.lucenerdd.spatial.commons.strategies.SpatialStrategy
import org.zouzias.spark.lucenerdd.store.IndexWithTaxonomyWriter

import scala.reflect.{ClassTag, classTag}


private[shape] class PointLuceneRDDPartition[V]
(private val iter: Iterator[V],
 private val indexAnalyzerName: String,
 private val queryAnalyzerName: String)
(override implicit val vTag: ClassTag[V])
(implicit docConversion: V => Document)
  extends AbstractPointLuceneRDDPartition[V]
    with IndexWithTaxonomyWriter
    with SpatialStrategy {
  override def size = ???

  override def iterator = ???

  override def isDefined(point: (Double, Double)) = ???

  /**
    * Nearest neighbour search
    *
    * @param point        query point
    * @param k            number of neighbors to return
    * @param searchString Lucene Query string
    * @return
    */
  override def knnSearch(point: (Double, Double), k: Int, searchString: String) = ???

  /**
    * Search for points within a circle
    *
    * @param center center of circle
    * @param radius radius of circle in kilometers (KM)
    * @param k      number of points to return
    * @return
    */
  override def circleSearch(center: (Double, Double), radius: Double, k: Int, operationName: String) = ???

  /**
    * Spatial search with arbitrary shape
    *
    * @param shapeAsString Shape object represented as String
    * @param k             Number of results to return
    * @param operationName Operation name, i.e., intersect, within, etc
    * @return
    */
  override def spatialSearch(shapeAsString: String, k: Int, operationName: String) = ???

  /**
    * Spatial search with point
    *
    * @param point         Query point
    * @param k             Number of result to return
    * @param operationName Operation name, i.e., intersect, within, etc
    * @return
    */
  override def spatialSearch(point: (Double, Double), k: Int, operationName: String) = ???

  /**
    * Bounding box search with point and radius
    *
    * @param center        given as (x, y)
    * @param radius        distance from center in kilometers (KM)
    * @param k             Number of results to return
    * @param operationName Operation name, i.e., intersect, within, etc
    * @return
    */
  override def bboxSearch(center: (Double, Double), radius: Double, k: Int, operationName: String) = ???

  /**
    * Bounding box search with lower left and upper right corners
    *
    * @param lowerLeft     Lower left point
    * @param upperRight    Upper left point
    * @param k             Number of results
    * @param operationName Operation name, i.e., intersect, within, etc
    * @return
    */
  override def bboxSearch(lowerLeft: (Double, Double),
                          upperRight: (Double, Double),
                          k: Int, operationName: String) = ???

  /**
    * Restricts the entries to those satisfying a predicate
    *
    * @param pred Predicate to filter on
    * @return
    */
  override def filter(pred: V => Boolean) = ???

  override protected def indexAnalyzer() = ???
}

object PointLuceneRDDPartition {

  /**
    * Constructor for [[PointLuceneRDDPartition]]
    *
    * @param iter Iterator over data
    * @param indexAnalyzer Index analyzer
    * @param queryAnalyzer Query analyzer
    * @param docConv Implicit convertion to Lucene document
    * @tparam V
    * @return
    */
  def apply[V: ClassTag](iter: Iterator[V],
                                      indexAnalyzer: String,
                                      queryAnalyzer: String)
                                     (implicit docConv: V => Document)
  : PointLuceneRDDPartition[V] = {
    new PointLuceneRDDPartition[V](iter,
      indexAnalyzer, queryAnalyzer)(classTag[V]) (docConv)
  }
}
