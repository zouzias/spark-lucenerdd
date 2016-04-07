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

package org.zouzias.spark.rdd.lucenerdd.impl

import org.apache.spark.Logging
import org.zouzias.spark.rdd.lucenerdd.LuceneRDDPartition

import scala.reflect.ClassTag

private[lucenerdd] class ElasticLuceneRDDPartition[T]
(private val iter: Iterator[T])
    (override implicit val kTag: ClassTag[T])
  extends LuceneRDDPartition[T] with Logging {

  override def size: Long = iter.size

  override def isDefined(elem: T): Boolean = iter.contains(elem)

  override def iterator: Iterator[T] = iter

  override def filter(pred: T => Boolean): LuceneRDDPartition[T] = new ElasticLuceneRDDPartition(iter.filter(pred))

  override def diff(other: LuceneRDDPartition[T]): LuceneRDDPartition[T] = ???

  override def diff(other: Iterator[T]): LuceneRDDPartition[T] =
    diff(ElasticLuceneRDDPartition(other))
}

private[lucenerdd] object ElasticLuceneRDDPartition {

  def apply[T: ClassTag]
      (iter: Iterator[T]): ElasticLuceneRDDPartition[T] = {
    new ElasticLuceneRDDPartition[T](iter)
  }
}
