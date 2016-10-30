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

import org.zouzias.spark.lucenerdd.spatial.shape._
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.spatial.shape.rdds.ShapeLuceneRDD
val df = spark.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").option("delimiter", "\t").load("src/test/resources/spatial/CH.txt")
val swissCities = df.select("_c0", "_c1", "_c5", "_c4").map(row => ((row.getDouble(2), row.getDouble(3)), row.getString(1).toLowerCase()))
val shapes = ShapeLuceneRDD(swissCities.rdd)
shapes.count