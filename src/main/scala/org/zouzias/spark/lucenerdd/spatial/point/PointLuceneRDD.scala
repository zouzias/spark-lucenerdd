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
package org.zouzias.spark.lucenerdd.spatial.point

import org.apache.lucene.document.Document
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.zouzias.spark.lucenerdd.spatial.core.SpatialLuceneRDD
import org.zouzias.spark.lucenerdd.spatial.core.partition.AbstractSpatialLuceneRDDPartition
import org.zouzias.spark.lucenerdd.spatial.point.partition.PointLuceneRDDPartition

import scala.reflect.ClassTag


class PointLuceneRDD[K: ClassTag, V: ClassTag]
  (private val partitionsRDD: RDD[AbstractSpatialLuceneRDDPartition[K, V]])
  extends SpatialLuceneRDD[K, V](partitionsRDD)

object PointLuceneRDD {

  /**
   * Instantiate a PointLuceneRDD given an RDD[T]
   *
   * @param elems RDD of type T
   * @return
   */
  def apply[K: ClassTag, V: ClassTag](elems: RDD[(K, V)])
                                     (implicit pointConverter: K => (Double, Double),
                                      docConverter: V => Document)
  : SpatialLuceneRDD[K, V] = {
    val partitions = elems.mapPartitions[AbstractSpatialLuceneRDDPartition[K, V]](
      iter => Iterator(PointLuceneRDDPartition[K, V](iter)),
      preservesPartitioning = true)
    new SpatialLuceneRDD(partitions)
  }

  /**
   * Instantiate a LuceneRDD with an iterable
   *
   * @param elems
   * @param sc
   * @return
   */
  def apply[K: ClassTag, V: ClassTag]
  (elems: Iterable[(K, V)])(implicit sc: SparkContext, pointConverter: K => (Double, Double),
                            docConverter: V => Document): SpatialLuceneRDD[K, V] = {
    apply(sc.parallelize[(K, V)](elems.toSeq))
  }
}

