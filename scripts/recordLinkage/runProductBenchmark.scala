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

val amazonDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/Amazon-GoogleProducts/Amazon.csv")
val googleDF= sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/Amazon-GoogleProducts/GoogleProducts.csv")
val groundTruthDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("scripts/recordLinkage/Amazon-GoogleProducts/Amazon_GoogleProducts_perfectMapping.csv")

val amazon = amazonDF.select("id", "title", "description", "manufacturer").map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3)))
val googleLuceneRDD = LuceneRDD(googleDF.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3))))


val linker: (String, String, String, String) => String = {
  case (_, name, description, manu) => {
    val nameTokens = name.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 1).distinct.mkString(" OR ")
    val descTerms = description.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 6).distinct.mkString(" OR ")
    val manuTerms = manu.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 1).mkString(" OR ")

    /*
    if (descTerms.nonEmpty && nameTokens.nonEmpty && manuTerms.nonEmpty) {
      s"(_2:(${nameTokens})) OR (_3:${descTerms}) OR (_4:${manuTerms})"
    }
    else if (nameTokens.nonEmpty && manuTerms.nonEmpty) {
      s"(_2:(${nameTokens})) OR (_4:${manuTerms})"
    }
    else if (nameTokens.nonEmpty) {
      s"_2:(${nameTokens})"
    }
    else {
      "*:*"
    }*/

    if (nameTokens.nonEmpty) {
      s"_2:(${nameTokens})"
    }
    else {
      "*:*"
    }
  }
}

val linkedResults = googleLuceneRDD.link(amazon, linker.tupled, 3)

import sqlContext.implicits._

val linkageResults = linkedResults.filter(_._2.nonEmpty).map{ case (left, topDocs) => (topDocs.head.doc.textField("_1").head, left._1)}.toDF("idGoogleBase", "idAmazon")

val correctHits: Double = linkageResults.join(groundTruthDF, groundTruthDF.col("idAmazon").equalTo(linkageResults("idAmazon")) &&  groundTruthDF.col("idGoogleBase").equalTo(linkageResults("idGoogleBase"))).count
val total: Double = groundTruthDF.count
val accuracy = correctHits / total
println(s"Accuracy of linkage is ${accuracy}")

