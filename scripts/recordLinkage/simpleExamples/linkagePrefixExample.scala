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
import org.apache.spark.rdd.RDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

// Step 1: Query prefixes of countries
// Shooting for Greece, Russian, Argentina and Belgium
val leftCountries = Array("gre", "ru", "ar", "bel")
val leftCountriesRDD: RDD[String] = sc.parallelize(leftCountries)

// Step 2: Load all country names
val countries = sc.parallelize(Source.fromFile("src/test/resources/countries.txt").getLines()
  .map(_.toLowerCase()).toSeq)
val luceneRDD = LuceneRDD(countries)

// Step 3: Define you linkage function (prefix)
def prefixLinker(country: String): String = {
  s"_1:${country}*"
}

// Step 4: Perform the linkage
val linked: RDD[(String, List[SparkScoreDoc])] = luceneRDD.link(leftCountriesRDD, prefixLinker, 10)

// Step 5: View the results
linked.foreach(println)

// (gre,List(SparkScoreDoc(1.0,88,0,Text fields:_1:[grenada]), SparkScoreDoc(1.0,87,0,Text fields:_1:[greenland]), SparkScoreDoc(1.0,86,0,Text fields:_1:[greece])))
// (ar,List(SparkScoreDoc(1.0,12,0,Text fields:_1:[aruba]), SparkScoreDoc(1.0,11,0,Text fields:_1:[armenia]), SparkScoreDoc(1.0,10,0,Text fields:_1:[argentina])))
// (ru,List(SparkScoreDoc(1.0,55,0,Text fields:_1:[russia])))
// (be,List(SparkScoreDoc(1.0,25,0,Text fields:_1:[bermuda]), SparkScoreDoc(1.0,24,0,Text fields:_1:[benin]), SparkScoreDoc(1.0,23,0,Text fields:_1:[belize]), SparkScoreDoc(1.0,22,0,Text fields:_1:[belgium]), SparkScoreDoc(1.0,21,0,Text fields:_1:[belarus])))