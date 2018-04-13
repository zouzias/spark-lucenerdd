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
package org.zouzias.spark.lucenerdd.spatial.commons

import org.apache.spark.Partitioner
import org.zouzias.spark.lucenerdd.spatial.point.PointLuceneRDD.PointType

import scala.util.Random

/**
  * Spark RDD [[Partitioner]] based on the x-axis and bounds per partition
  * @param boundsPerPart Bounds of x-axis (minX, maxX) per RDD's partition
  */
case class SpatialByXPartitioner(boundsPerPart: Array[PointType]) extends Partitioner {
  override def numPartitions: Int = boundsPerPart.length

  override def getPartition(key: Any): Int = {
    val keyPoint = key.asInstanceOf[PointType]
    val indexOpt = boundsPerPart.indexWhere{ case (minX, maxX) =>
      minX <= keyPoint._1 && keyPoint._1 <= maxX
    }

    // If key is not assigned to a partition, randomly assign the key
    if (indexOpt == -1) Random.nextInt(numPartitions) else indexOpt
  }
}
