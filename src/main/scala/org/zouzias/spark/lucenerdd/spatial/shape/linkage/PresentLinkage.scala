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
package org.zouzias.spark.lucenerdd.spatial.shape.linkage

import org.apache.spark.rdd.RDD
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

import scala.reflect.ClassTag

trait PresentLinkage[V] {

  /**
    * Return a one-to-one linkage, given a scored top-K linkage
    * @param that
    * @param linkage
    * @tparam T
    * @return
    */
  def postLinker[T: ClassTag](that: RDD[(Long, T)], linkage: RDD[(T, Array[SparkScoreDoc])])
  : RDD[(T, V)]
}
