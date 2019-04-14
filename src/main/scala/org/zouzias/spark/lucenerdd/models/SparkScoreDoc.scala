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

import org.apache.lucene.document.Document
import org.apache.lucene.search.{IndexSearcher, ScoreDoc}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.sql.Row
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc.inferNumericType

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

sealed trait FieldType extends Serializable
object TextType extends FieldType
object IntType extends FieldType
object DoubleType extends FieldType
object LongType extends FieldType
object FloatType extends FieldType


/**
 * A scored Lucene [[Document]]
 *
 * @param score Score of document
 * @param docId Document id
 * @param shardIndex Shard index
 * @param doc Serialized Lucene document
 */
case class SparkScoreDoc(score: Float, docId: Int, shardIndex: Int, doc: Document) {

  /**
    * Convert to [[Row]]
    *
    * @return
    */
  def toRow(): Row = {

    // Convert to Spark SQL DataFrame types
    val typeWithValue = this.doc.getFields.asScala.map { field =>
      val fieldName = field.name()

      val tp = if (field.numericValue() != null) {
        inferNumericType(Some(field.numericValue))
      }
      else if (field.numericValue() == null && field.stringValue() != null) {
        TextType
      }

      tp match {
        case TextType => (StructField(fieldName, StringType), field.stringValue())
        case IntType => (StructField(fieldName, IntegerType), field.numericValue().intValue())
        case LongType => (StructField(fieldName,
          org.apache.spark.sql.types.LongType), field.numericValue().longValue())
        case DoubleType => (StructField(fieldName,
          org.apache.spark.sql.types.DoubleType), field.numericValue().doubleValue())
        case FloatType => (StructField(fieldName,
          org.apache.spark.sql.types.FloatType), field.numericValue().floatValue())
      }
    }

    // Additional fields of [[SparkScoreDoc]] with known types
    val extraSchemaWithValue = Seq((StructField("__docid__", IntegerType), this.docId),
      (StructField("__score__", org.apache.spark.sql.types.DoubleType), this.score),
      (StructField("__shardIndex__", IntegerType), this.shardIndex))

    Row.fromSeq(typeWithValue ++ extraSchemaWithValue)
  }

  override def toString: String = {
    val builder = new StringBuilder
    builder.append(s"[score: $score/")
    builder.append(s"docId: $docId/")
    builder.append(s"doc: $doc")
    builder.result()
  }
}

object SparkScoreDoc extends Serializable {

  def apply(indexSearcher: IndexSearcher, scoreDoc: ScoreDoc): SparkScoreDoc = {
    SparkScoreDoc(scoreDoc.score, scoreDoc.doc, scoreDoc.shardIndex,
      indexSearcher.doc(scoreDoc.doc))
  }

  def apply(indexSearcher: IndexSearcher, scoreDoc: ScoreDoc, score: Float): SparkScoreDoc = {
    SparkScoreDoc(score, scoreDoc.doc, scoreDoc.shardIndex, indexSearcher.doc(scoreDoc.doc))
  }

  /**
   * Ordering by score (descending)
   */
  def descending: Ordering[Row] = new Ordering[Row]{
    override def compare(x: Row, y: Row): Int = {
      val xScore = x.getDouble(x.fieldIndex("score"))
      val yScore = y.getDouble(y.fieldIndex("score"))
      if ( xScore > yScore) {
        -1
      } else if (xScore == yScore) 0 else 1
    }
  }

  /**
   * Ordering by score (ascending)
   */
  def ascending: Ordering[Row] = new Ordering[Row]{
    override def compare(x: Row, y: Row): Int = {
      val xScore = x.getDouble(x.fieldIndex("score"))
      val yScore = y.getDouble(y.fieldIndex("score"))

      if ( xScore < yScore) -1 else if (xScore == yScore) 0 else 1
    }
  }

  /**
    * Checks the subclass of [[Number]]
    * @param num
    * @return
    */
  private def inferNumericType[T <: Number : ClassTag](num: Option[T]): FieldType = {
    num match {
      case None => TextType
      case _: Some[Integer] => IntType
      case _: Some[Long] => LongType
      case _: Some[Double] => LongType
      case _: Some[Float] => LongType
      case _ => TextType
    }
  }
}


