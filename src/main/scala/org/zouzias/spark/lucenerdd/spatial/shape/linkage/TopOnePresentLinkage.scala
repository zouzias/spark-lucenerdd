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
import org.zouzias.spark.lucenerdd.spatial.shape.rdds.ShapeRDD

import scala.reflect.ClassTag

/**
 * Top-one linkage, link with the
 */
trait TopOnePresentLinkage[V]
  extends PresentLinkage[V] {

  /**
   * Post linker function. Picks the top result for linkage
   *
   * @param linkage
   * @return
   */
  def postLinker[T: ClassTag](that: RDD[(Long, T)], linkage: RDD[(T, Array[SparkScoreDoc])]): RDD[(T, V)] = {
    val linkageById: RDD[(Long, T)] = linkage.flatMap{ case (k, v) =>
      v.headOption.flatMap(x => x.doc.numericField(ShapeRDD.RddPositionFieldName).map(_.longValue()))
        .map(x => (x, k))
    }

    linkageById.join(that).values.mapValues(_.)
  }
}
