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
 *
 * @param doc Lucene document
 */
class SparkDoc(doc: Document) extends Serializable {

  private lazy val numberFields: Map[String, Number] = doc.getFields().asScala
    .flatMap( field =>
    if (field.numericValue() != null && field.name() != null) {
      Some((field.name(), field.numericValue()))
    }
    else {
      None
    }
  ).toMap[String, Number]

  private lazy val stringFields: Map[String, String] = doc.getFields().asScala
    .flatMap( field =>
      if (field.name() != null &&
        field.stringValue() != null &&
        !numberFields.keySet.contains(field.name())) {
        // add if not contained in numeric fields
        Some((field.name(), field.stringValue()))
      }
      else {
        None
      }
    ).toMap[String, String]

  def getFields: Set[String] = {
    getTextFields ++ getNumericFields
  }

  def getTextFields: Set[String] = {
    stringFields.keySet
  }

  def getNumericFields: Set[String] = {
    numberFields.keySet
  }

  def field(fieldName: String): Any = {
    numberFields.getOrElse(fieldName, stringFields.getOrElse(fieldName, Nil))
  }

  def textField(fieldName: String): Option[String] = {
    stringFields.get(fieldName)
  }

  def numericField(fieldName: String): Option[Number] = {
    numberFields.get(fieldName)
  }

  override def toString: String = {
    val builder = new StringBuilder
    if ( numberFields.nonEmpty) builder.append("Numeric fields:")
    numberFields.foreach { case (name, value) =>
      builder.append(s"$name:[${value}]")
    }
    if (stringFields.nonEmpty) builder.append("Text fields:")
    stringFields.foreach { case (name, value) =>
      builder.append(s"$name:[${value}]")
    }
    builder.result()
  }
}

object SparkDoc extends Serializable {
  def apply(doc: Document): SparkDoc = {
    new SparkDoc(doc)
  }
}
