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

import org.zouzias.spark.lucenerdd.partition.LuceneRDDPartition
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.LuceneRDD
val df = sqlContext.read.format("com.databricks.spark.csv").option("escape", "\"").option("header", "false").option("inferSchema", "false").option("delimiter", "|").load("/Users/taazoan3/recordLinkageData/geonames/USclean.txt")
val cities = df.select("C0", "C1", "C3", "C4").map( row =>  (row.getString(0), row.getString(1), row.getString(2), row.getString(3)))
val luceneRDD = LuceneRDD(cities)
luceneRDD.count