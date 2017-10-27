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
package org.zouzias.spark.lucenerdd.spatial.point.partition

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.{Document, StoredField}
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, ScoreDoc, Sort}
import org.apache.lucene.spatial.query.{SpatialArgs, SpatialOperation}
import org.joda.time.DateTime
import org.locationtech.spatial4j.distance.DistanceUtils
import org.locationtech.spatial4j.shape.{Point, Shape}
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.query.LuceneQueryHelpers
import org.zouzias.spark.lucenerdd.response.LuceneRDDResponsePartition
import org.zouzias.spark.lucenerdd.spatial.commons.strategies.SpatialStrategy
import org.zouzias.spark.lucenerdd.spatial.shape.ShapeLuceneRDD.PointType
import org.zouzias.spark.lucenerdd.store.IndexWithTaxonomyWriter

import scala.reflect.{ClassTag, classTag}


private[point] class PointLuceneRDDPartition[V]
(private val iter: Iterator[(PointType, V)],
 private val indexAnalyzerName: String,
 private val queryAnalyzerName: String)
(override implicit val vTag: ClassTag[V])
(implicit docConversion: V => Document)
  extends AbstractPointLuceneRDDPartition[V]
    with IndexWithTaxonomyWriter
    with SpatialStrategy {

  /* We index only points in [[PointLuceneRDDPartition]] */
  strategy.setPointsOnly(true)

  override def indexAnalyzer(): Analyzer = getAnalyzer(Some(indexAnalyzerName))

  private val QueryAnalyzer: Analyzer = getAnalyzer(Some(queryAnalyzerName))

  private def decorateWithLocation(doc: Document, point: Point): Document = {

    strategy.createIndexableFields(point).foreach{ field =>
        doc.add(field)
        doc.add(new StoredField(strategy.getFieldName, shapeToString(point)))
    }

    doc
  }

  private val (iterOriginal, iterIndex) = iter.duplicate

  private val startTime = new DateTime(System.currentTimeMillis())
  logInfo(s"Indexing process initiated at ${startTime}...")
  iterIndex.foreach { case (p, value) =>
    // (implicitly) convert type V to a Lucene document
    val doc = docConversion(value)
    val docWithLocation = decorateWithLocation(doc, ctx.makePoint(p._1, p._2))
    indexWriter.addDocument(FacetsConfig.build(taxoWriter, docWithLocation))
  }
  private val endTime = new DateTime(System.currentTimeMillis())
  logInfo(s"Indexing process completed at ${endTime}...")
  logInfo(s"Indexing process took ${(endTime.getMillis - startTime.getMillis) / 1000} seconds...")

  // Close the indexWriter and taxonomyWriter (for faceted search)
  closeAllWriters()

  private val indexReader = DirectoryReader.open(IndexDir)
  private val indexSearcher = new IndexSearcher(indexReader)

  override def size: Long = iterOriginal.size.toLong

  /**
    * Restricts the entries to those satisfying a predicate
    *
    * @param pred
    * @return
    */
  override def filter(pred: (PointType, V) => Boolean):
  AbstractPointLuceneRDDPartition[V] = {
    PointLuceneRDDPartition(iterOriginal.filter(x => pred(x._1, x._2)),
      indexAnalyzerName, queryAnalyzerName)
  }

  override def isDefined(key: PointType): Boolean = iterOriginal.exists(_._1 == key)

  override def iterator: Iterator[(PointType, V)] = iterOriginal

  private def docLocation(scoreDoc: ScoreDoc): Option[Shape] = {
    val shapeString = indexReader.document(scoreDoc.doc)
      .getField(strategy.getFieldName)
      .stringValue()

    try{
      Some(stringToShape(shapeString))
    }
    catch {
      case _: Throwable => None
    }
  }

  override def circleSearch(center: PointType, radius: Double, k: Int, operationName: String)
  : LuceneRDDResponsePartition = {
    logInfo(s"circleSearch [center:${center}, operation:${operationName}]")
    val args = new SpatialArgs(SpatialOperation.get(operationName),
      ctx.makeCircle(center._1, center._2,
        DistanceUtils.dist2Degrees(radius, DistanceUtils.EARTH_MEAN_RADIUS_KM)))

    val query = strategy.makeQuery(args)
    val docs = indexSearcher.search(query, k)
    LuceneRDDResponsePartition(docs.scoreDocs.map(SparkScoreDoc(indexSearcher, _)).toIterator)
  }

  override def knnSearch(point: PointType, k: Int, searchString: String)
  : LuceneRDDResponsePartition = {
    logInfo(s"knnSearch [center:${point}, searchQuery:${searchString}]")

    // Match all, order by distance ascending
    val pt = ctx.makePoint(point._1, point._2)

    // the distance (in km)
    val valueSource = strategy.makeDistanceValueSource(pt, DistanceUtils.DEG_TO_KM)

    // false = ascending dist
    val distSort = new Sort(valueSource.getSortField(false)).rewrite(indexSearcher)

    val query = LuceneQueryHelpers.parseQueryString(searchString, QueryAnalyzer)
    val docs = indexSearcher.search(query, k, distSort)

    // Here we sorted on it, and the distance will get
    // computed redundantly.  If the distance is only needed for the top-X
    // search results then that's not a big deal. Alternatively, try wrapping
    // the ValueSource with CachingDoubleValueSource then retrieve the value
    // from the ValueSource now. See LUCENE-4541 for an example.
    val result = docs.scoreDocs.map { scoreDoc => {
      val location = docLocation(scoreDoc)
      location match {
        case Some(shape) =>
          SparkScoreDoc(indexSearcher, scoreDoc,
            ctx.calcDistance(pt, shape.getCenter).toFloat)
        case None =>
          SparkScoreDoc(indexSearcher, scoreDoc, -1F)
      }
    }
    }

    LuceneRDDResponsePartition(result.toIterator)
  }

  override def spatialSearch(shapeAsString: String, k: Int, operationName: String)
  : LuceneRDDResponsePartition = {
    logInfo(s"spatialSearch [shape:${shapeAsString} and operation:${operationName}]")
    val shape = stringToShape(shapeAsString)
    spatialSearch(shape, k, operationName)
  }

  private def spatialSearch(shape: Shape, k: Int, operationName: String)
  : LuceneRDDResponsePartition = {
    val args = new SpatialArgs(SpatialOperation.get(operationName), shape)
    val query = strategy.makeQuery(args)
    val docs = indexSearcher.search(query, k)
    LuceneRDDResponsePartition(docs.scoreDocs.map(SparkScoreDoc(indexSearcher, _)).toIterator)
  }

  override def spatialSearch(point: PointType, k: Int, operationName: String)
  : LuceneRDDResponsePartition = {
    val shape = ctx.makePoint(point._1, point._2)
    spatialSearch(shape, k, operationName)
  }

  override def bboxSearch(center: PointType, radius: Double, k: Int, operationName: String)
  : LuceneRDDResponsePartition = {
    logInfo(s"bboxSearch [center:${center}, radius: ${radius} and operation:${operationName}]")
    val x = center._1
    val y = center._2
    val radiusKM = DistanceUtils.dist2Degrees(radius, DistanceUtils.EARTH_MEAN_RADIUS_KM)
    val shape = ctx.makeRectangle(x - radiusKM, x + radiusKM, y - radiusKM, y + radiusKM)
    spatialSearch(shape, k, operationName)
  }

  override def bboxSearch(lowerLeft: PointType, upperRight: PointType, k: Int, opName: String)
  : LuceneRDDResponsePartition = {
    val lowerLeftPt = ctx.makePoint(lowerLeft._1, lowerLeft._2)
    val upperRightPt = ctx.makePoint(upperRight._1, upperRight._2)
    val shape = ctx.makeRectangle(lowerLeftPt, upperRightPt)
    spatialSearch(shape, k, opName)
  }
}

object PointLuceneRDDPartition {

  /**
    * Constructor for [[PointLuceneRDDPartition]]
    *
    * @param iter Iterator over data
    * @param indexAnalyzer Index analyzer
    * @param queryAnalyzer Query analyzer
    * @param docConv Implicit convertion to Lucene document
    * @tparam V
    * @return
    */
  def apply[V: ClassTag](iter: Iterator[(PointType, V)],
                                      indexAnalyzer: String,
                                      queryAnalyzer: String)
                                     (implicit docConv: V => Document)
  : PointLuceneRDDPartition[V] = {
    new PointLuceneRDDPartition[V](iter,
      indexAnalyzer, queryAnalyzer)(classTag[V]) (docConv)
  }
}
