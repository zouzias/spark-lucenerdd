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

//==================================================================//
// Example usage for record linkage between Google Scholar and DBLP //
//==================================================================//

import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._

val dblpDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/DBLP-Scholar/DBLP1.csv")
val scholarDF= sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/DBLP-Scholar/Scholar.csv")
val groundTruthDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/DBLP-Scholar/DBLP-Scholar_perfectMapping.csv")

val scholar = scholarDF.map( row => (row.get(0).toString, row.getString(1), row.getString(2)))
val dblp = LuceneRDD(dblpDF.map( row => (row.get(0).toString, row.getString(1), row.getString(2))))

// A custom linker
val linker: (String, String, String) => String = {
  case (_, title, authors) => {
    val titleTokens = title.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 3).mkString(" OR ")
    val authorsTerms = authors.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 2).mkString(" OR ")

    if (titleTokens.nonEmpty && authorsTerms.nonEmpty) {
      s"(_2:(${titleTokens}) OR _3:(${authorsTerms}))"
    }
    else if (titleTokens.nonEmpty){
      s"_2:(${titleTokens})"
    }
    else if (authorsTerms.nonEmpty){
      s"_3:(${authorsTerms})"
    }
    else {
      "*:*"
    }
  }
}


val linkedResults = dblp.link(scholar, linker.tupled, 3)

import sqlContext.implicits._

val linkageResults = linkedResults.filter(_._2.nonEmpty).map{ case (scholar, topDocs) => (topDocs.head.doc.textField("_1").head, scholar._1)}.toDF("idDBLP", "idScholar")

val correctHits: Double = linkageResults.join(groundTruthDF, groundTruthDF.col("idDBLP").equalTo(linkageResults("idDBLP")) &&  groundTruthDF.col("idScholar").equalTo(linkageResults("idScholar"))).count
val total: Double = groundTruthDF.count
val accuracy = correctHits / total

println(s"Accuracy of linkage is ${accuracy}")