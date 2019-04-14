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
package org.zouzias.spark.lucenerdd.response

import com.twitter.algebird.TopKMonoid
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

/**
 * LuceneRDD response
 */
private[lucenerdd] class LuceneRDDResponse
  (protected val partitionsRDD: RDD[LuceneRDDResponsePartition],
   protected val ordering: Ordering[Row])
  extends RDD[Row](partitionsRDD.context,
    List(new OneToOneDependency(partitionsRDD))) {

  setName("LuceneRDDResponse")

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext)
  : Iterator[Row] = {
    firstParent[LuceneRDDResponsePartition].iterator(split, context).next().iterator()
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
  override def take(num: Int): Array[Row] = {
    val monoid = new TopKMonoid[Row](num)(ordering)
    partitionsRDD.map(monoid.build(_))
      .reduce(monoid.plus).items.toArray
  }

  override def collect(): Array[Row] = {
    val sz = partitionsRDD.map(_.size).sum().toInt
    if (sz > 0) {
      val monoid = new TopKMonoid[Row](sz)(ordering)
      partitionsRDD.map(monoid.build(_))
        .reduce(monoid.plus).items.toArray
    } else {
      Array.empty[Row]
    }
  }

  def toDF(): DataFrame = {
    this.toDF()
  }
}
