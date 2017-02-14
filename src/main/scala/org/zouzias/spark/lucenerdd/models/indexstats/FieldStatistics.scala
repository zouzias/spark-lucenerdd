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
package org.zouzias.spark.lucenerdd.models.indexstats

import org.apache.lucene.index.IndexReader

/**
  * Statistics for Lucene index field
  */
case class FieldStatistics(fieldName: String, docCount: Int, sumDocFreq: Long,
                           totalTermFreq: Long) {
  override def toString(): String = {
    val buf = new StringBuilder()
    buf.append(s"fieldName: ${fieldName} / ")
    buf.append(s"docCount: ${docCount} / ")
    buf.append(s"sumDocFreq: ${sumDocFreq} / ")
    buf.append(s"totalTermFreq: ${totalTermFreq}\n")
    buf.result()
  }
}

object FieldStatistics {
  def apply(indexReader: IndexReader, fieldName: String): FieldStatistics = {
    val docCount = indexReader.getDocCount(fieldName)
    val sumDocFreq = indexReader.getSumDocFreq(fieldName)
    val totalTermFreq = indexReader.getSumTotalTermFreq(fieldName)

    FieldStatistics(fieldName, docCount, sumDocFreq, totalTermFreq)
  }
}
