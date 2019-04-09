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
import org.zouzias.spark.lucenerdd.models.{SparkDoc, SparkScoreDoc}
import org.zouzias.spark.lucenerdd.partition.LuceneRDDPartition
import org.zouzias.spark.lucenerdd.response.{LuceneRDDResponse, LuceneRDDResponsePartition}
import org.zouzias.spark.lucenerdd.testing.{FavoriteCaseClass, Person}

class LuceneRDDKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[LuceneRDD[_]])
    kryo.register(classOf[LuceneRDDPartition[_]])
    kryo.register(classOf[SparkDoc])
    kryo.register(classOf[Number])
    kryo.register(classOf[java.lang.Double])
    kryo.register(classOf[java.lang.Float])
    kryo.register(classOf[java.lang.Integer])
    kryo.register(classOf[java.lang.Long])
    kryo.register(classOf[java.lang.Short])
    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofRef])
    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofFloat])
    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofDouble])
    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofInt])
    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofLong])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Number]])
    kryo.register(classOf[Array[Float]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Long]])
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Boolean]])
    kryo.register(classOf[Range])
    kryo.register(classOf[scala.collection.immutable.Map[String, String]])
    kryo.register(classOf[scala.collection.immutable.Map[String, Number]])
    kryo.register(classOf[scala.collection.immutable.Map$EmptyMap$])
    kryo.register(classOf[scala.collection.immutable.Set$EmptySet$])
    kryo.register(classOf[scala.collection.immutable.Map[_, _]])
    kryo.register(classOf[Array[scala.collection.immutable.Map[_, _]]])
    kryo.register(classOf[SparkScoreDoc])
    kryo.register(classOf[LuceneRDDResponse])
    kryo.register(classOf[LuceneRDDResponsePartition])
    kryo.register(classOf[TopK[_]])
    kryo.register(classOf[FavoriteCaseClass]) /* For testing */
    kryo.register(classOf[Array[FavoriteCaseClass]]) /* For testing */
    kryo.register(classOf[Person]) /* For testing */
    kryo.register(classOf[Array[Person]]) /* For testing */
    ()
  }
}

/**
 * Decorator for LuceneRDD Kryo serialization
 */
object LuceneRDDKryoRegistrator {
  def registerKryoClasses(conf: SparkConf): SparkConf = {
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[LuceneRDDKryoRegistrator].getName)
      .set("spark.kryo.registrationRequired", "false")
    /* Set the above to true s.t. all classes are registered with Kryo */
  }
}
