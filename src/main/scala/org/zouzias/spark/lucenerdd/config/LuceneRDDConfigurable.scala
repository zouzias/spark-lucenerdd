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
package org.zouzias.spark.lucenerdd.config

import com.typesafe.config.ConfigFactory

/**
 * Configuration for [[org.zouzias.spark.lucenerdd.LuceneRDD]]
 */
trait LuceneRDDConfigurable extends Serializable {

  val config = ConfigFactory.load()

  protected val MaxDefaultTopKValue: Int = {
    if (config.hasPath("lucenerdd.query.topk.default")) {
      config.getInt("lucenerdd.query.topk.maxvalue")
    }
    else 1000
  }

  /** Default value for topK queries */
  protected val DefaultTopK: Int = {
    if (config.hasPath("lucenerdd.query.topk.default")) {
      config.getInt("lucenerdd.query.topk.default")
    }
    else 10
  }
}
