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

import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.LuceneRDD

val df = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("src/test/resources/h1bvisa-2014.csv")
val words = df.select("lca_case_employer_name", "lca_case_job_title", "lca_case_employer_city", "lca_case_employer_state", "lca_case_employer_postal_code")
val luceneRDD = LuceneRDD(words)
luceneRDD.count