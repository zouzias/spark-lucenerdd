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
package org.zouzias.spark.lucenerdd.testing

import org.apache.spark.sql.Row
import scala.collection.JavaConverters._
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

trait LuceneRDDTestUtils {

  val Bern = ( (7.45, 46.95), "Bern")
  val Zurich = ( (8.55, 47.366667), "Zurich")
  val Laussanne = ( (6.6335, 46.519833), "Laussanne")
  val Athens = ((23.716667, 37.966667), "Athens")
  val Toronto = ((-79.4, 43.7), "Toronto")
  val Milan = ((45.4646, 9.198), "Milan")
  val cities = Array(Bern, Zurich, Laussanne, Athens, Milan, Toronto)

  def Radius: Double

  def convertToCircle(city: ((Double, Double), String)): (((Double, Double), Double), String) = {
    ((city._1, Radius), city._2)
  }

  def convertToRectangle(city: ((Double, Double), String))
  : ((Double, Double, Double, Double), String) = {
    val x = city._1._1
    val y = city._1._2

    ((x - Radius, x + Radius, y - Radius, y + Radius), city._2)
  }

  def convertToPolygon(city: ((Double, Double), String), width: Double)
  : (Array[(Double, Double)], String) = {
    val x = city._1._1
    val y = city._1._2

    val coords = Array((x - width, y - width), (x - width, y + width),
      (x + width, y + width), (x + width, y - width), (x - width, y - width))
    (coords, city._2)
  }

  protected def docTextFieldEq(doc: Row, fieldName: String, fieldValue: String): Boolean = {
    doc.getString(doc.fieldIndex(fieldName)).contains(fieldValue)
  }

  protected def docTextFieldEq(docs: Array[Row], fieldName: String, fieldValue: String)
  : Boolean = {
    docs.exists(x => x.getList[String](x.fieldIndex(fieldName))
      .asScala.forall(_.contains(fieldValue)))
  }

  // Check if sequence is sorted in descending order
  protected def sortedDesc(seq : Seq[Double]) : Boolean = {
    if (seq.isEmpty) true else seq.zip(seq.tail).forall(x => x._1 >= x._2)
  }

  // Check if sequence is sorted in descending order
  protected def sortedDescSparkScoreDocs(seq : Seq[SparkScoreDoc]) : Boolean = {
    if (seq.isEmpty) true else seq.zip(seq.tail).forall(x => x._1.score >= x._2.score)
  }

}
