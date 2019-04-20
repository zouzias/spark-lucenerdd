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
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc.inferNumericType
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc.{DocIdField, ScoreField, ShardField}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

sealed trait FieldType extends Serializable
object TextType extends FieldType
object IntType extends FieldType
object DoubleType extends FieldType
object LongType extends FieldType
object FloatType extends FieldType


/**
 * A Lucene [[Document]] extended with score, docId and shard index
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
    val typeToValues = scala.collection.mutable.Map[StructField, List[Any]]().empty

    this.doc.getFields
      .asScala
      .filter(_.fieldType().stored())
      .foreach { field =>
      val fieldName = field.name()

      val tp = if (field.numericValue() != null) {
        inferNumericType(field.numericValue)
      }
      else if (field.numericValue() == null && field.stringValue() != null) {
        TextType
      }

      val item = tp match {
        case TextType => (StructField(fieldName, StringType), field.stringValue())
        case IntType => (StructField(fieldName, IntegerType), field.numericValue().intValue())
        case LongType => (StructField(fieldName,
          org.apache.spark.sql.types.LongType), field.numericValue().longValue())
        case DoubleType => (StructField(fieldName,
          org.apache.spark.sql.types.DoubleType), field.numericValue().doubleValue())
        case FloatType => (StructField(fieldName,
          org.apache.spark.sql.types.FloatType), field.numericValue().floatValue())
        case _ => (StructField(fieldName, StringType), field.stringValue())
      }

        // Append or set value
        val oldValue: List[Any] = typeToValues.getOrElse(item._1, List.empty)
        typeToValues.+=((item._1, oldValue.::(item._2)))
    }

    val arrayedTypesToValues = typeToValues.map{ case (tp, values) =>

      // If more than one values, wrap SQL type within ArrayType
      if (values.length == 1) {
        (tp, values.head)
      }
      else {
        (StructField(tp.name, ArrayType.apply(tp.dataType)), values)
      }
    }

    // Additional fields of [[SparkScoreDoc]] with known types inlucding
    // - document id
    // - documenet search score
    // - document shard index
    val extraSchemaWithValue = Seq((StructField(DocIdField, IntegerType), this.docId),
      (StructField(ScoreField, org.apache.spark.sql.types.FloatType), this.score),
      (StructField(ShardField, IntegerType), this.shardIndex))

    val allTogether = arrayedTypesToValues ++ extraSchemaWithValue

    new GenericRowWithSchema(allTogether.values.toArray, StructType(allTogether.keys.toSeq))
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

  val DocIdField = "__docid__"
  val ScoreField = "__score__"
  val ShardField = "__shardIndex__"

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
      val xScore = x.getFloat(x.fieldIndex(ScoreField))
      val yScore = y.getFloat(y.fieldIndex(ScoreField))
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
      val xScore = x.getFloat(x.fieldIndex(ScoreField))
      val yScore = y.getFloat(y.fieldIndex(ScoreField))

      if ( xScore < yScore) -1 else if (xScore == yScore) 0 else 1
    }
  }

  /**
    * Infers the subclass of [[Number]]
    * @param num A value of type [[Number]]
    * @return The [[FieldType]] of the input Number value
    */
  private def inferNumericType(num: Number): FieldType = {
    num match {
      case _: java.lang.Double => DoubleType
      case _: java.lang.Long => LongType
      case _: java.lang.Integer => IntType
      case _: java.lang.Float => FloatType
      case _ => TextType
    }
  }
}


