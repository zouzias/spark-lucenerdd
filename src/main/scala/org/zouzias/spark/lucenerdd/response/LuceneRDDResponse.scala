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
package org.zouzias.spark.lucenerdd.response

import com.twitter.algebird.TopKMonoid
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc

/**
 * LuceneRDD response
 */
private[lucenerdd] class LuceneRDDResponse
  (protected val partitionsRDD: RDD[LuceneRDDResponsePartition],
   protected val ordering: Ordering[SparkScoreDoc])
  extends RDD[SparkScoreDoc](partitionsRDD.context,
    List(new OneToOneDependency(partitionsRDD))) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext)
  : Iterator[SparkScoreDoc] = {
    firstParent[LuceneRDDResponsePartition].iterator(split, context).next().iterator()
  }

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def cache(): this.type = {
    this.persist(StorageLevel.MEMORY_ONLY)
  }

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    super.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    super.unpersist(blocking)
    this
  }

  /**
   * Use [[TopKMonoid]] to take
   * @param num
   * @return
   */
  override def take(num: Int): Array[SparkScoreDoc] = {
    val monoid = new TopKMonoid[SparkScoreDoc](num)(ordering)
    partitionsRDD.map(monoid.build(_))
      .reduce(monoid.plus).items.toArray
  }

  override def collect(): Array[SparkScoreDoc] = {
    val sz = partitionsRDD.map(_.size).sum().toInt
    val monoid = new TopKMonoid[SparkScoreDoc](sz)(ordering)
    partitionsRDD.map(monoid.build(_))
      .reduce(monoid.plus).items.toArray
  }

  def toDF(sampleRatio: Double = 0.01)(implicit sqlContext: SQLContext): DataFrame = {

    val total = this.asInstanceOf[RDD[SparkScoreDoc]].count()
    val sample = this.asInstanceOf[RDD[SparkScoreDoc]].sample(false, sampleRatio)
    val types = if (total <= 10) inferTypes(this.asInstanceOf[RDD[SparkScoreDoc]])
    else inferTypes(sample)

    val schema = types.map{ case (fieldName, tp) =>
      tp match {
        case TextType => StructField(fieldName, StringType)
        case IntType => StructField(fieldName, IntegerType)
        case LongType => StructField(fieldName, org.apache.spark.sql.types.LongType)
        case DoubleType => StructField(fieldName, org.apache.spark.sql.types.DoubleType)
        case FloatType => StructField(fieldName, org.apache.spark.sql.types.FloatType)
      }
    }.toSeq

    val rows: RDD[Row] = this.map { elem =>
      Row.fromSeq(schema.map(x => elem.doc.field(x.name)))
    }

    sqlContext.createDataFrame(rows, StructType(schema))
  }


  /**
    * Return field types
    *
    * @param d
    * @return
    */
  private def inferTypes(d: SparkScoreDoc): Iterable[(String, FieldType)] = {
    val textFieldTypes = d.doc.getTextFields.map(x => x -> TextType)
    val numFields = d.doc.getNumericFields
    val numFieldTypes = numFields
      .map(fieldName => fieldName -> inferNumericType(d.doc.numericField(fieldName)))

    textFieldTypes ++ numFieldTypes
  }

  def inferTypes(sample: Double): Map[String, FieldType] = {
    val types = this.asInstanceOf[RDD[SparkScoreDoc]].sample(false, sample)
      .flatMap(inferTypes).map(x => x -> 1L)
      .reduceByKey(_ + _)
      .map(x => (x._1._1, (x._2, x._1._2)))

    types.foldByKey((0L, TextType))( (x, y) => if (x._1 >= y._1) x else y)
      .map(x => x._1 -> x._2._2).collect().toMap[String, FieldType]
  }

  def typeFreq(docs: RDD[SparkScoreDoc]): RDD[(String, (Long, FieldType))] = {
    val types = docs
      .flatMap(inferTypes).map(x => x -> 1L)
      .reduceByKey(_ + _)
      .map(x => (x._1._1, (x._2, x._1._2)))

    types.reduceByKey((x, y) => if (x._1 >= y._1) x else y)
  }

  def inferTypes(docs: RDD[SparkScoreDoc]): Map[String, FieldType] = {
    val types = docs
      .flatMap(inferTypes).map(x => x -> 1L)
      .reduceByKey(_ + _)
      .map(x => (x._1._1, (x._2, x._1._2)))

    val mostFreqTypes = types.reduceByKey((x, y) => if (x._1 >= y._1) x else y)
    mostFreqTypes.map(x => x._1 -> x._2._2).collect().toMap[String, FieldType]
  }


  def inferNumericType(num: Option[Number]): FieldType = {
    num match {
      case None => TextType
      case Some(n) =>
        if (n.intValue() != null) {
          IntType
        }
        else if (n.longValue() != null) {
          LongType
        }
        else if (n.doubleValue() != null) {
          DoubleType
        }
        else if (n.floatValue() != null) {
          FloatType
        }
        else {
          TextType
        }
    }
  }
}

sealed trait FieldType extends Serializable
object TextType extends FieldType
object IntType extends FieldType
object DoubleType extends FieldType
object LongType extends FieldType
object FloatType extends FieldType
