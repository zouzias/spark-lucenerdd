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
package org.zouzias.spark.lucenerdd

import com.twitter.algebird.TopK
import com.twitter.chill.Kryo
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import org.zouzias.spark.lucenerdd.models.{SparkDoc, SparkFacetResult, SparkScoreDoc}
import org.zouzias.spark.lucenerdd.partition.LuceneRDDPartition

class LuceneRDDKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[LuceneRDD[_]])
    kryo.register(classOf[LuceneRDDPartition[_]])
    kryo.register(classOf[SparkDoc])
    kryo.register(classOf[SparkFacetResult])
    kryo.register(classOf[SparkScoreDoc])
    kryo.register(classOf[TopK[_]])
  }
}

/**
 * Decorator for LuceneRDD Kryo serialization
 */
object LuceneRDDKryoRegistrator {
  def registerKryoClasses(conf: SparkConf): SparkConf = {
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
          .set("spark.kryo.registrator", classOf[LuceneRDDKryoRegistrator].getName)
  }
}
