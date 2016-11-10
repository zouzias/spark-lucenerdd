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
package org.zouzias.spark.lucenerdd.spatial.shape

import com.twitter.algebird.TopK
import com.twitter.chill._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import org.zouzias.spark.lucenerdd.models.{SparkDoc, SparkFacetResult, SparkScoreDoc}
import org.zouzias.spark.lucenerdd.response.{LuceneRDDResponse, LuceneRDDResponsePartition}
import org.zouzias.spark.lucenerdd.spatial.shape.partition.ShapeLuceneRDDPartition


class ShapeLuceneRDDKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[ShapeLuceneRDD[_, _]])
    kryo.register(classOf[ShapeLuceneRDDPartition[_, _]])
    kryo.register(classOf[SparkDoc])
    kryo.register(classOf[SparkFacetResult])
    kryo.register(classOf[LuceneRDDResponse])
    kryo.register(classOf[LuceneRDDResponsePartition])
    kryo.register(classOf[SparkScoreDoc])
    kryo.register(classOf[TopK[_]])
    ()
  }
}

/**
 * Decorator for [[ShapeLuceneRDD]] Kryo serialization
 */
object ShapeLuceneRDDKryoRegistrator {
  def registerKryoClasses(conf: SparkConf): SparkConf = {
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[ShapeLuceneRDDKryoRegistrator].getName)
      .set("spark.kryo.registrationRequired", "false")
    /* Set the above to true s.t. all classes are registered with Kryo */
  }
}

