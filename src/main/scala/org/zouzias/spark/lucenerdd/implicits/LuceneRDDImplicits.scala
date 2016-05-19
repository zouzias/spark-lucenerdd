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

package org.zouzias.spark.lucenerdd.implicits

import org.apache.lucene.document._
import org.apache.lucene.facet.FacetField
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.models.LuceneText

import scala.reflect.ClassTag

/**
 * Implicit conversions of basic types to Apache Lucene documents
 *
 * Currently supports:
 * 1) Primitive types: Int, Long, Float, Double, String and [[LuceneText]]
 * 2) Tuples up to size 8 of the above types
 */
object LuceneRDDImplicits {

  private val Stored = Field.Store.YES
  private val DefaultFieldName = "_1"

  private def addTextFacetField(doc: Document, fieldName: String, fieldValue: String): Unit = {
    if ( fieldValue.nonEmpty) { // Issues with empty strings on facets
      doc.add(new FacetField(s"${fieldName}${LuceneRDD.FacetTextFieldSuffix}",
        fieldValue))
    }
  }

  private def addNumericFacetField[T: ClassTag](doc: Document,
                                                fieldName: String,
                                                fieldValue: T): Unit = {
    addTextFacetField(doc, fieldName, fieldValue.toString)  // For text-like faceting
    /*
      fieldValue match {
        case x if (x.isInstanceOf[Long]) =>
          doc.add(new NumericDocValuesField(s"${fieldName}${LuceneRDD.FacetNumericFieldSuffix}",
            x.asInstanceOf[Long]))
        case x if (x.isInstanceOf[Int]) =>
          doc.add(new NumericDocValuesField(s"${fieldName}${LuceneRDD.FacetNumericFieldSuffix}",
            x.asInstanceOf[Int].toLong))
        case x if (x.isInstanceOf[Float]) =>
          doc.add(new FloatDocValuesField(s"${fieldName}${LuceneRDD.FacetNumericFieldSuffix}",
            x.asInstanceOf[Float]))
        case x if (x.isInstanceOf[Double]) =>
          doc.add(new DoubleDocValuesField(s"${fieldName}${LuceneRDD.FacetNumericFieldSuffix}",
            x.asInstanceOf[Double].toFloat))
      }
      */
  }

  implicit def intToDocument(v: Int): Document = {
    val doc = new Document
    doc.add(new IntField(DefaultFieldName, v, Stored))
    addNumericFacetField(doc, DefaultFieldName, v.toString)
    doc
  }

  implicit def longToDocument(v: Long): Document = {
    val doc = new Document
    doc.add(new LongField(DefaultFieldName, v, Stored))
    addNumericFacetField(doc, DefaultFieldName, v)
    doc
  }

  implicit def doubleToDocument(v: Double): Document = {
    val doc = new Document
    doc.add(new DoubleField(DefaultFieldName, v, Stored))
    addNumericFacetField(doc, DefaultFieldName, v)
    doc
  }

  implicit def floatToDocument(v: Float): Document = {
    val doc = new Document
    doc.add(new FloatField(DefaultFieldName, v, Stored))
    addNumericFacetField(doc, DefaultFieldName, v)
    doc
  }

  implicit def stringToDocument(s: String): Document = {
    val doc = new Document
    doc.add(new StringField(DefaultFieldName, s, Stored))
    addTextFacetField(doc, DefaultFieldName, s)
    doc
  }

  implicit def textFieldToDocument(s: LuceneText): Document = {
    val doc = new Document
    doc.add(new TextField(DefaultFieldName, s.content, Stored))
    doc
  }

  private def tupleTypeToDocument[T: ClassTag](doc: Document, index: Int, s: T): Document = {
   typeToDocument(doc, s"_${index}", s)
  }

  private def typeToDocument[T: ClassTag](doc: Document, fieldName: String, s: T): Document = {
    s match {
      case x: LuceneText =>
        doc.add(new TextField(fieldName, x.content, Stored))
      case x: String =>
        doc.add(new StringField(fieldName, x, Stored))
        addTextFacetField(doc, fieldName, x)
      case x: Long =>
        doc.add(new LongField(fieldName, x, Stored))
        addNumericFacetField(doc, fieldName, x)
      case x: Int =>
        doc.add(new IntField(fieldName, x, Stored))
        addNumericFacetField(doc, fieldName, x)
      case x: Float =>
        doc.add(new FloatField(fieldName, x, Stored))
        addNumericFacetField(doc, fieldName, x)
      case x: Double =>
        doc.add(new DoubleField(fieldName, x, Stored))
        addNumericFacetField(doc, fieldName, x)
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

  implicit def tuple2ToDocument[T1: ClassTag, T2: ClassTag](s: (T1, T2)): Document = {
    val doc = new Document
    tupleTypeToDocument[T1](doc, 1, s._1)
    tupleTypeToDocument[T2](doc, 2, s._2)
    doc
  }

  implicit def tuple3ToDocument[T1: ClassTag,
  T2: ClassTag,
  T3: ClassTag](s: (T1, T2, T3)): Document = {
    val doc = new Document
    tupleTypeToDocument[T1](doc, 1, s._1)
    tupleTypeToDocument[T2](doc, 2, s._2)
    tupleTypeToDocument[T3](doc, 3, s._3)
    doc
  }

  implicit def tuple4ToDocument[T1: ClassTag,
  T2: ClassTag,
  T3: ClassTag,
  T4: ClassTag](s: (T1, T2, T3, T4)): Document = {
    val doc = new Document
    tupleTypeToDocument[T1](doc, 1, s._1)
    tupleTypeToDocument[T2](doc, 2, s._2)
    tupleTypeToDocument[T3](doc, 3, s._3)
    tupleTypeToDocument[T4](doc, 4, s._4)
    doc
  }

  implicit def tuple5ToDocument[T1: ClassTag,
  T2: ClassTag,
  T3: ClassTag,
  T4: ClassTag,
  T5: ClassTag](s: (T1, T2, T3, T4, T5)): Document = {
    val doc = new Document
    tupleTypeToDocument[T1](doc, 1, s._1)
    tupleTypeToDocument[T2](doc, 2, s._2)
    tupleTypeToDocument[T3](doc, 3, s._3)
    tupleTypeToDocument[T4](doc, 4, s._4)
    tupleTypeToDocument[T5](doc, 5, s._5)
    doc
  }

  implicit def tuple6ToDocument[T1: ClassTag,
  T2: ClassTag,
  T3: ClassTag,
  T4: ClassTag,
  T5: ClassTag,
  T6: ClassTag](s: (T1, T2, T3, T4, T5, T6)): Document = {
    val doc = new Document
    tupleTypeToDocument[T1](doc, 1, s._1)
    tupleTypeToDocument[T2](doc, 2, s._2)
    tupleTypeToDocument[T3](doc, 3, s._3)
    tupleTypeToDocument[T4](doc, 4, s._4)
    tupleTypeToDocument[T5](doc, 5, s._5)
    tupleTypeToDocument[T6](doc, 6, s._6)
    doc
  }

  implicit def tuple7ToDocument[T1: ClassTag,
  T2: ClassTag,
  T3: ClassTag,
  T4: ClassTag,
  T5: ClassTag,
  T6: ClassTag,
  T7: ClassTag](s: (T1, T2, T3, T4, T5, T6, T7)): Document = {
    val doc = new Document
    tupleTypeToDocument[T1](doc, 1, s._1)
    tupleTypeToDocument[T2](doc, 2, s._2)
    tupleTypeToDocument[T3](doc, 3, s._3)
    tupleTypeToDocument[T4](doc, 4, s._4)
    tupleTypeToDocument[T5](doc, 5, s._5)
    tupleTypeToDocument[T6](doc, 6, s._6)
    tupleTypeToDocument[T7](doc, 7, s._7)
    doc
  }

  implicit def tuple8ToDocument[T1: ClassTag,
  T2: ClassTag,
  T3: ClassTag,
  T4: ClassTag,
  T5: ClassTag,
  T6: ClassTag,
  T7: ClassTag,
  T8: ClassTag](s: (T1, T2, T3, T4, T5, T6, T7, T8)): Document = {
    val doc = new Document
    tupleTypeToDocument[T1](doc, 1, s._1)
    tupleTypeToDocument[T2](doc, 2, s._2)
    tupleTypeToDocument[T3](doc, 3, s._3)
    tupleTypeToDocument[T4](doc, 4, s._4)
    tupleTypeToDocument[T5](doc, 5, s._5)
    tupleTypeToDocument[T6](doc, 6, s._6)
    tupleTypeToDocument[T7](doc, 7, s._7)
    tupleTypeToDocument[T8](doc, 8, s._8)
    doc
  }

}
