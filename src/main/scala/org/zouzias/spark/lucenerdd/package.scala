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
import org.apache.spark.sql.types.ArrayType
import org.zouzias.spark.lucenerdd.config.LuceneRDDConfigurable

import collection.JavaConverters._
import scala.reflect.ClassTag

package object lucenerdd extends LuceneRDDConfigurable {

  private val DefaultFieldName = "_1"
  private val DefaultNotAnalyzedFieldSuffix = "_notanalyzed"

  /**
    * Returns true, if a field should be analyzed
    *
    * Note: All fields that end with [[DefaultNotAnalyzedFieldSuffix]]
    * are not analyzed
    *
    * @param fieldName Name of field
    * @return Returns true, if field must be analyzed
    */
  private def isAnalyzedField(fieldName: String): Boolean = {
    if (StringFieldsListToBeNotAnalyzed.contains(fieldName) ||
      fieldName.toLowerCase.endsWith(DefaultNotAnalyzedFieldSuffix)) {
      false
    }
    else {
      // Return the default string field analysis option
      StringFieldsDefaultAnalyzed
    }
  }

  private def listPrimitiveToDocument[T: ClassTag](doc: Document,
                                                   fieldName: String,
                                                   iter: java.util.List[T])
  : Document = {
    iter.asScala.foreach( item => typeToDocument(doc, fieldName, item))
    doc
  }

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

    if (s != null) {
      doc.add(new Field(DefaultFieldName, s, analyzedField(StringFieldsDefaultAnalyzed)))
    }
    doc
  }

  private def tupleTypeToDocument[T: ClassTag](doc: Document, index: Int, s: T): Document = {
    typeToDocument(doc, s"_${index}", s)
  }

  /**
    *
    * Decide to analyze a field based on its name
    *
    * @param tobeAnalyzed If true, analyze the field
    * @return A Lucene [[FieldType]]
    */
  private def analyzedField(tobeAnalyzed: Boolean): FieldType = {
    val fieldType = new FieldType()
    fieldType.setStoreTermVectors(StringFieldsStoreTermVector)
    fieldType.setStoreTermVectorPositions(StringFieldsStoreTermPositions)
    fieldType.setOmitNorms(StringFieldsOmitNorms)
    fieldType.setTokenized(tobeAnalyzed)
    fieldType.setStored(true) // All text fields must be stored (LuceneRDDResponse requirement)
    fieldType.setIndexOptions(StringFieldsIndexOptions)
    fieldType.freeze()
    fieldType
  }

  def typeToDocument[T: ClassTag](doc: Document, fieldName: String, s: T): Document = {
    s match {
      case x: String if x != null =>
        doc.add(new Field(fieldName, x,
          analyzedField(isAnalyzedField(fieldName)))
        )
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
      case null => Unit
      case _ =>
        throw new RuntimeException(s"Type ${s.getClass.getName} " +
          s"on field ${fieldName} is not supported")
    }
    doc
  }

  implicit def iterablePrimitiveToDocument[T: ClassTag](iter: Iterable[T]): Document = {
    val doc = new Document
    iter.foreach( item => tupleTypeToDocument(doc, 1, item))
    doc
  }

  implicit def arrayPrimitiveToDocument[T: ClassTag](iter: Array[T]): Document = {
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

    row.schema.map(field => (field.name, field.dataType))
      .foreach{ case (fieldName, dataType) =>
      val index = row.fieldIndex(fieldName)

      // TODO: Handle org.apache.spark.sql.types.MapType and more
      if (dataType.isInstanceOf[ArrayType]) {
       listPrimitiveToDocument(doc, fieldName, row.getList(index))
      }
      else {
        typeToDocument(doc, fieldName, row.get(index))
      }
    }

    doc
  }
}
