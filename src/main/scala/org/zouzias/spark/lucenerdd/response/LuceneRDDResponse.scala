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
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
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

  setName("LuceneRDDResponse")

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

  /**
    * Converts [[LuceneRDDResponse]] to [[DataFrame]]
    *
    * Infers the type of each field using the most frequently appeared type per field
    *
    * @param sampleRatio Sample percentage to inspect for deciding on infered types.
    * @param sqlContext [[SQLContext]]
    * @return
    */
  def toDF(sampleRatio: Double = 0.01)(implicit sqlContext: SQLContext): DataFrame = {

    val total = this.asInstanceOf[RDD[SparkScoreDoc]].count()
    val sample = this.asInstanceOf[RDD[SparkScoreDoc]].sample(false, sampleRatio)
    val types = if (total <= 10) inferTypes(this.asInstanceOf[RDD[SparkScoreDoc]])
    else inferTypes(sample)

    // Convert to Spark SQL DataFrame types
    val schema = types.map{ case (fieldName, tp) =>
      tp match {
        case TextType => StructField(fieldName, StringType)
        case IntType => StructField(fieldName, IntegerType)
        case LongType => StructField(fieldName, org.apache.spark.sql.types.LongType)
        case DoubleType => StructField(fieldName, org.apache.spark.sql.types.DoubleType)
        case FloatType => StructField(fieldName, org.apache.spark.sql.types.FloatType)
      }
    }.toSeq

    // Additional fields of [[SparkScoreDoc]] with known types
    val schemaWithExtraFields = schema ++ Seq(StructField("__docid__", IntegerType),
      StructField("__score__", org.apache.spark.sql.types.DoubleType),
      StructField("__shardIndex__", IntegerType))

    // Convert values to Spark SQL Row
    val rows: RDD[Row] = this.map { elem =>
      Row.fromSeq(schema.map(x => elem.doc.field(x.name))
        ++ Seq(elem.docId, elem.score, elem.shardIndex))
    }

    sqlContext.createDataFrame(rows, StructType(schemaWithExtraFields))
  }


  /**
    * Infer the types of each field of a [[SparkScoreDoc]]
    *
    * @param d
    * @return
    */
  private def inferTypes(d: SparkScoreDoc): Iterable[(String, FieldType)] = {
    val textFieldTypes = d.doc.getTextFields.map(x => x -> TextType)
    val numFieldTypes = d.doc.getNumericFields
      .map(fieldName => fieldName -> inferNumericType(d.doc.numericField(fieldName)))

    textFieldTypes ++ numFieldTypes
  }

  /**
    * Infers types of scored documents, i.e., [[SparkScoreDoc]]
    *
    * @param docs RDD of [[SparkScoreDoc]]
    * @return Map of field name to its inferred type
    */
  def inferTypes(docs: RDD[SparkScoreDoc]): Map[String, FieldType] = {
    val types = docs
      .flatMap(inferTypes).map(x => x -> 1L)
      .reduceByKey(_ + _)
      .map{ case (key, value) => (key._1, (value, key._2))}

    val mostFreqTypes = types.reduceByKey((x, y) => if (x._1 >= y._1) x else y)
    mostFreqTypes.map(x => x._1 -> x._2._2).collect().toMap[String, FieldType]
  }

  /**
    * Checks the subclass of [[Number]]
    * @param num
    * @return
    */
  private def inferNumericType(num: Option[Number]): FieldType = {
    num match {
      case None => TextType
      case Some(n) =>
        if (n.isInstanceOf[Integer]) { IntType }
        else if (n.isInstanceOf[Long]) { LongType }
        else if (n.isInstanceOf[Double]) { DoubleType }
        else if (n.isInstanceOf[Float]) { FloatType }
        else { TextType }
    }
  }
}
