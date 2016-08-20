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
package org.zouzias.spark.lucenerdd.models

import com.twitter.algebird.MapMonoid
import org.apache.lucene.document.Document

import scala.collection.JavaConverters._

/**
 * Wrapper around Lucene document
 *
 * If [[Document]] were serializable, this class would not exist.
 * @param doc
 */
class SparkDoc(doc: Document) extends Serializable {

  private val stringFields = doc.getFields().asScala.map( field =>
    if (field.stringValue() != null && field.name() != null) {
      Map((field.name(), List(field.stringValue())))
    }
    else {
      Map.empty[String, List[String]]
    }
  ).reduce(SparkDoc.stringMonoid.plus)

  private val numberFields = doc.getFields().asScala.map( field =>
    if (field.numericValue() != null && field.name() != null) {
      Map((field.name(), List(field.numericValue())))
    }
    else {
      Map.empty[String, List[Number]]
    }
  ).reduce(SparkDoc.numberMonoid.plus)

  def getFields: Set[String] = {
    getTextFields ++ getNumericFields
  }

  def getTextFields: Set[String] = {
    stringFields.keySet
  }

  def getNumericFields: Set[String] = {
    numberFields.keySet
  }

  def textField(fieldName: String): List[String] = {
    stringFields.getOrElse(fieldName, List.empty[String])
  }

  def numericField(fieldName: String): List[Number] = {
    numberFields.getOrElse(fieldName, List.empty[Number])
  }

  override def toString: String = {
    val builder = new StringBuilder
    if ( numberFields.nonEmpty) builder.append("Numeric fields:")
    numberFields.foreach { case (name, values) =>
      builder.append(s"$name:[${values.mkString(",")}]")
    }
    if (stringFields.nonEmpty) builder.append("Text fields:")
    stringFields.foreach { case (name, values) =>
      builder.append(s"$name:[${values.mkString(",")}]")
    }
    builder.result()
  }
}

object SparkDoc extends Serializable {

  private lazy val stringMonoid = new MapMonoid[String, List[String]]()
  private lazy val numberMonoid = new MapMonoid[String, List[Number]]()

  def apply(doc: Document): SparkDoc = {
    new SparkDoc(doc)
  }
}
