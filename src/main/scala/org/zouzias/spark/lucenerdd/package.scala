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
package org.zouzias.spark

import org.apache.lucene.document.{DoubleDocValuesField, _}
import org.apache.lucene.facet.FacetField

import scala.reflect.ClassTag

package object lucenerdd {

  private val Stored = Field.Store.YES
  private val DefaultFieldName = "_1"

  private def addTextFacetField(doc: Document, fieldName: String, fieldValue: String): Unit = {
    if ( fieldValue.nonEmpty) { // Issues with empty strings on facets
      doc.add(new FacetField(s"${fieldName}${LuceneRDD.FacetTextFieldSuffix}",
        fieldValue))
    }
  }

  implicit def intToDocument(v: Int): Document = {
    val doc = new Document
    doc.add(new IntField(DefaultFieldName, v, Stored))
    addTextFacetField(doc, DefaultFieldName, v.toString)
    doc
  }

  implicit def longToDocument(v: Long): Document = {
    val doc = new Document
    doc.add(new LongField(DefaultFieldName, v, Stored))
    addTextFacetField(doc, DefaultFieldName, v.toString)
    doc
  }

  implicit def doubleToDocument(v: Double): Document = {
    val doc = new Document
    doc.add(new DoubleField(DefaultFieldName, v, Stored))
    addTextFacetField(doc, DefaultFieldName, v.toString)
    doc
  }

  implicit def floatToDocument(v: Float): Document = {
    val doc = new Document
    doc.add(new FloatField(DefaultFieldName, v, Stored))
    addTextFacetField(doc, DefaultFieldName, v.toString)
    doc
  }

  implicit def stringToDocument(s: String): Document = {
    val doc = new Document
    doc.add(new TextField(DefaultFieldName, s, Stored))
    addTextFacetField(doc, DefaultFieldName, s)
    doc
  }

  private def tupleTypeToDocument[T: ClassTag](doc: Document, index: Int, s: T): Document = {
    typeToDocument(doc, s"_${index}", s)
  }

  def typeToDocument[T: ClassTag](doc: Document, fieldName: String, s: T): Document = {
    s match {
      case x: String =>
        doc.add(new TextField(fieldName, x, Stored))
        addTextFacetField(doc, fieldName, x)
      case x: Long =>
        doc.add(new LongField(fieldName, x, Stored))
        doc.add(new NumericDocValuesField(s"${fieldName}${LuceneRDD.FacetNumericFieldSuffix}",
          x))
      case x: Int =>
        doc.add(new IntField(fieldName, x, Stored))
        doc.add(new NumericDocValuesField(s"${fieldName}${LuceneRDD.FacetNumericFieldSuffix}",
          x.toLong))
      case x: Float =>
        doc.add(new FloatField(fieldName, x, Stored))
        doc.add(new FloatDocValuesField(s"${fieldName}${LuceneRDD.FacetNumericFieldSuffix}",
          x))
      case x: Double =>
        doc.add(new DoubleField(fieldName, x, Stored))
        doc.add(new DoubleDocValuesField(s"${fieldName}${LuceneRDD.FacetNumericFieldSuffix}",
          x))
    }
    doc
  }

  implicit def iterablePrimitiveToDocument[T: ClassTag](iter: Iterable[T]): Document = {
    val doc = new Document
    iter.foreach( item => tupleTypeToDocument(doc, 1, item))
    doc
  }

  implicit def mapToDocument[T: ClassTag](map: Map[String, T]): Document = {
    val doc = new Document
    map.keys.foreach{ case key =>
      typeToDocument(doc, key, map.get(key).get)
    }
    doc
  }

  /**
   * Implicit convertion for all product types, such as case classes and Tuples
   * @param s
   * @tparam T
   * @return
   */
  implicit def productTypeToDocument[T <: Product : ClassTag](s: T): Document = {
    val doc = new Document

    val fieldNames = s.getClass.getDeclaredFields.map(_.getName).toIterator
    val fieldValues = s.productIterator
    fieldValues.zip(fieldNames).foreach{ case (elem, fieldName) =>
      typeToDocument(doc, fieldName, elem)
    }

    doc
  }
}
