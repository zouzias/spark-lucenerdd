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
package org.zouzias.spark.lucenerdd.models

import org.apache.lucene.facet.FacetResult

case class SparkFacetResult(facetName: String, facets: Map[String, Long]) {

  /**
   * Return facet counts sorted descending
   * @return Sequence of (facet value, facet counts)
   */
  def sortedFacets(): Seq[(String, Long)] = {
    facets.toSeq.sortBy[Long](x => -x._2)
  }
}


object SparkFacetResult extends Serializable {

  def apply(facetName: String, facetResult: FacetResult): SparkFacetResult = {
    val facetResultOpt = Option(facetResult)
    facetResultOpt match {
    case Some(fctResult) =>
      val map = fctResult.labelValues
        .map(labelValue => labelValue.label -> labelValue.value.longValue())
        .toMap[String, Long]
      SparkFacetResult(facetName, map)
      case _ => SparkFacetResult(facetName, Map.empty[String, Long])
    }
  }
}
