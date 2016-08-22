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

val abtDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/Abt-Buy/Abt.csv")
val buyDF= sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/Abt-Buy/Buy.csv")
val groundTruthDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/Abt-Buy/abt_buy_perfectMapping.csv")

val abt = abtDF.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3)))
val buy = LuceneRDD(buyDF.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3))))



val linker: (String, String, String, String) => String = {
  case (_, name, description, _) => {
    val nameTokens = name.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 0).mkString(" OR ")
    val descTerms = description.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 0).mkString(" OR ")

    if (descTerms.nonEmpty) {
      s"(_2:(${nameTokens})) OR (_3:${descTerms})"
    }
    else{
      s"_2:(${nameTokens})"
    }
  }
}


val linkedResults = buy.link(abt, linker.tupled, 3)

import sqlContext.implicits._

val linkageResultsIds = linkedResults.filter(_._2.nonEmpty).map{ case (abtId, topDocs) => (topDocs.head.doc.textField("_1").head, abtId._1.toInt)}.toDF("idBuy", "idAbt")

val correctHits: Double = linkageResultsIds.join(groundTruthDF, groundTruthDF.col("idAbt").equalTo(linkageResultsIds("idAbt")) &&  groundTruthDF.col("idBuy").equalTo(linkageResultsIds("idBuy"))).count
val total: Double = groundTruthDF.count

val accuracy = correctHits / total
println(s"Accuracy of linkage is ${accuracy}")


