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
import org.zouzias.spark.lucenerdd.implicits.LuceneRDDImplicits._
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

// Step 1: Query prefixes of countries
// Shooting for Greece, Germany, Spain and Italy
val leftCountries = Array("gree", "germa", "spa", "ita")
val leftCountriesRDD: RDD[String] = sc.parallelize(leftCountries)

// Step 2: Load all country names
val countries = sc.parallelize(Source.fromFile("src/test/resources/countries.txt").getLines()
  .map(_.toLowerCase()).toSeq)
val luceneRDD = LuceneRDD(countries)

// Step 3: Define you linkage function (prefix)
def fuzzyLinker(country: String): String = {
  val Fuzziness = 2
  s"_1:${country}~${Fuzziness}"
}

// Step 4: Perform the linkage
val linked: RDD[(String, List[SparkScoreDoc])] = luceneRDD.link(leftCountriesRDD, fuzzyLinker, 10)

// Step 5: View the results
linked.foreach(println)
// spa,List(SparkScoreDoc(5.1271343,84,0,Text fields:_1:[spain])))
// (gree,List(SparkScoreDoc(5.1271343,86,0,Text fields:_1:[greece])))
// (germa,List(SparkScoreDoc(5.127134,83,0,Text fields:_1:[germany])))
// (ita,List(SparkScoreDoc(2.9601524,106,0,Text fields:_1:[italy]), SparkScoreDoc(2.9601524,102,0,Text fields:_1:[iraq]), SparkScoreDoc(2.9601524,101,0,Text fields:_1:[iran]))

