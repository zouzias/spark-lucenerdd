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
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._

//================================================================//
// Example usage for record linkage between ACM and DBLP          //
//================================================================//

val acmDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/DBLP-ACM/ACM.csv")
val dblp2DF= sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/DBLP-ACM/DBLP2.csv")
val groundTruthDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/DBLP-ACM/DBLP-ACM_perfectMapping.csv")

val acm = acmDF.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3), row.get(4).toString))
val dblp2 = LuceneRDD(dblp2DF.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3), row.get(4).toString)))
dblp2.cache()

// Link is the author tokens or title tokens match. Combine all tokens by an OR clause
val linker: (String, String, String, String, String) => String = {
  case (_, title, authors, _, year) => {
    val titleTokens = title.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 3).mkString(" OR ")
    val authorsTerms = authors.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 2).mkString(" OR ")

    if (authorsTerms.nonEmpty) {
      s"(_2:(${titleTokens})) OR (_3:${authorsTerms})"
    }
    else{
      s"_2:(${titleTokens})"
    }
  }
}


val linkedResults = dblp2.link(acm, linker.tupled, 10)

import sqlContext.implicits._

val linkageResults = linkedResults.filter(_._2.nonEmpty).map{ case (acm, topDocs) => (topDocs.head.doc.textField("_1").head, acm._1.toInt)}.toDF("idDBLP", "idACM")

val correctHits: Double = linkageResults.join(groundTruthDF, groundTruthDF.col("idDBLP").equalTo(linkageResults("idDBLP")) &&  groundTruthDF.col("idACM").equalTo(linkageResults("idACM"))).count
val total: Double = groundTruthDF.count

val accuracy = correctHits / total
println(s"Accuracy of linkage is ${accuracy}")


