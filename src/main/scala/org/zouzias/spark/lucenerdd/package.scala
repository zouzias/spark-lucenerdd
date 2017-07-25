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

import org.apache.lucene.document._
import org.apache.spark.sql.Row
import org.zouzias.spark.lucenerdd.config.LuceneRDDConfigurable

import scala.reflect.ClassTag

package object lucenerdd extends LuceneRDDConfigurable {

  private val Stored = Field.Store.YES
  private val DefaultFieldName = "_1"

  implicit def intToDocument(v: Int): Document = {
    val doc = new Document
    if (v != null) {
      doc.add(new IntPoint(DefaultFieldName, v))
      doc.add(new StoredField(DefaultFieldName, v))
    }
    doc
  }

  implicit def longToDocument(v: Long): Document = {
    val doc = new Document
    if (v != null) {
      doc.add(new LongPoint(DefaultFieldName, v))
      doc.add(new StoredField(DefaultFieldName, v))
    }
    doc
  }

  implicit def doubleToDocument(v: Double): Document = {
    val doc = new Document
    if (v != null) {
      doc.add(new DoublePoint(DefaultFieldName, v))
      doc.add(new StoredField(DefaultFieldName, v))
    }
    doc
  }

  implicit def floatToDocument(v: Float): Document = {
    val doc = new Document
    if (v != null) {
      doc.add(new FloatPoint(DefaultFieldName, v))
      doc.add(new StoredField(DefaultFieldName, v))
    }
    doc
  }

  implicit def stringToDocument(s: String): Document = {
    val doc = new Document

    if (s != null) doc.add(new Field(DefaultFieldName, s, CustomStringFieldType))
    doc
  }

  private def tupleTypeToDocument[T: ClassTag](doc: Document, index: Int, s: T): Document = {
    typeToDocument(doc, s"_${index}", s)
  }

  /**
    * Custom text field type, reads configuration and returns Lucene field type
    */
  private lazy val CustomStringFieldType: FieldType = {
    val fieldType = new FieldType()
    fieldType.setStoreTermVectors(StringFieldsStoreTermVector)
    fieldType.setStoreTermVectorPositions(StringFieldsStoreTermPositions)
    fieldType.setOmitNorms(StringFieldsOmitNorms)
    fieldType.setTokenized(StringFieldsAnalyzed)
    fieldType.setStored(true) // All text fields must be stored (LuceneRDDResponse requirement)
    fieldType.setIndexOptions(StringFieldsIndexOptions)
    fieldType.freeze()
    fieldType
  }

  def typeToDocument[T: ClassTag](doc: Document, fieldName: String, s: T): Document = {

    s match {
      case x: String if x != null =>
        doc.add(new Field(fieldName, x, CustomStringFieldType))
        doc.add(new StoredField(fieldName, x))
      case x: Long if x != null =>
        doc.add(new LongPoint(fieldName, x))
        doc.add(new StoredField(fieldName, x))
      case x: Int if x != null =>
        doc.add(new IntPoint(fieldName, x))
        doc.add(new StoredField(fieldName, x))
      case x: Float if x != null =>
        doc.add(new FloatPoint(fieldName, x))
        doc.add(new StoredField(fieldName, x))
      case x: Double if x != null =>
        doc.add(new DoublePoint(fieldName, x))
        doc.add(new StoredField(fieldName, x))
      case _ => Unit
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
    map.foreach{ case (key, value) =>
      typeToDocument(doc, key, value)
    }
    doc
  }

  /**
   * Implicit conversion for all product types, such as case classes and Tuples
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

  /**
   * Implicit conversion for Spark Row: used for DataFrame
   * @param row
   * @return
   */
  implicit def sparkRowToDocument(row: Row): Document = {
    val doc = new Document

    val fieldNames = row.schema.fieldNames
    fieldNames.foreach{ case fieldName =>
      val index = row.fieldIndex(fieldName)
      typeToDocument(doc, fieldName, row.get(index))
    }

    doc
  }
}
