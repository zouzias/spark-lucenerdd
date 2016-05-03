/**
 * Created by taazoan3 on 2016-05-03.
 */

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

import scala.io.Source
import org.zouzias.spark.lucenerdd.impl.InMemoryLuceneRDDPartition
import org.zouzias.spark.lucenerdd.implicits.LuceneRDDImplicits._
import org.zouzias.spark.lucenerdd.LuceneRDD._
import org.zouzias.spark.lucenerdd.LuceneRDD
val cities = Source.fromFile("src/test/resources/cities.txt").getLines().toSeq
val rdd = sc.parallelize(cities)
val luceneRDD = LuceneRDD(rdd)
luceneRDD.count
