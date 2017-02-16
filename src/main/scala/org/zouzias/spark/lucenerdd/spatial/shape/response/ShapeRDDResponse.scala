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
package org.zouzias.spark.lucenerdd.spatial.shape.response

import com.twitter.algebird.TopKMonoid
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

/**
 * Response of [[org.zouzias.spark.lucenerdd.spatial.shape.rdds.ShapeRDD]]
 */
private[lucenerdd] class ShapeRDDResponse
  (protected val partitionsRDD: RDD[ShapeRDDResponsePartition],
   protected val ordering: Ordering[SparkScoreDoc])
  extends RDD[SparkScoreDoc](partitionsRDD.context,
    List(new OneToOneDependency(partitionsRDD))) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext)
  : Iterator[SparkScoreDoc] = {
    firstParent[ShapeRDDResponsePartition].iterator(split, context).next().iterator()
  }

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def cache(): this.type = {
    this.persist(StorageLevel.MEMORY_ONLY)
  }

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    super.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    super.unpersist(blocking)
    this
  }

  /**
   * Use [[TopKMonoid]] to take
   * @param num
   * @return
   */
  override def take(num: Int): Array[SparkScoreDoc] = {
    val monoid = new TopKMonoid[SparkScoreDoc](num)(ordering)
    partitionsRDD.map(monoid.build(_))
      .reduce(monoid.plus).items.toArray
  }

  override def collect(): Array[SparkScoreDoc] = {
    val sz = partitionsRDD.map(_.size).sum().toInt
    val monoid = new TopKMonoid[SparkScoreDoc](sz)(ordering)
    partitionsRDD.map(monoid.build(_))
      .reduce(monoid.plus).items.toArray
  }
}
