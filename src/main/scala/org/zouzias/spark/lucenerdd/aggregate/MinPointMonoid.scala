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
package org.zouzias.spark.lucenerdd.aggregate

import com.twitter.algebird.Monoid
import org.zouzias.spark.lucenerdd.spatial.point.PointLuceneRDD.PointType

/**
  * Minimum point Monoid used for spatial linkage
  *
  * Keeps the minimum value per coordinate
  */
object MinPointMonoid extends Monoid[PointType] {

  override def zero: PointType = (Double.MaxValue, Double.MaxValue)

  override def plus(x: PointType, y: PointType): PointType = {
    (Math.min(x._1, y._1), Math.min(x._2, y._2))
  }
}
