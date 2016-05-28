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

import java.io.StringReader

import com.spatial4j.core.distance.DistanceUtils
import com.spatial4j.core.shape.Shape
import org.apache.lucene.document.{Document, StoredField}
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, ScoreDoc, Sort}
import org.apache.lucene.spatial.query.{SpatialArgs, SpatialOperation}
import org.zouzias.spark.lucenerdd.analyzers.WSAnalyzer
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.query.LuceneQueryHelpers
import org.zouzias.spark.lucenerdd.spatial.grids.GridLoader
import org.zouzias.spark.lucenerdd.spatial.strategies.SpatialStrategy
import org.zouzias.spark.lucenerdd.store.IndexWithTaxonomyWriter

import scala.reflect._

private[lucenerdd] class ShapeLuceneRDDPartition[K, V]
  (private val iter: Iterator[(K, V)])
  (override implicit val kTag: ClassTag[K],
   override implicit val vTag: ClassTag[V])
  (implicit shapeConversion: K => Shape,
   docConversion: V => Document)
  extends AbstractShapeLuceneRDDPartition[K, V]
    with WSAnalyzer
    with IndexWithTaxonomyWriter
    with GridLoader
    with SpatialStrategy {

  private def decorateWithLocation(doc: Document, shapes: Iterable[Shape]): Document = {

    // Potentially more than one shape in this field is supported by some
    // strategies; see the Javadoc of the SpatialStrategy impl to see.
    shapes.foreach{ case shape =>
      strategy.createIndexableFields(shape).foreach{ case field =>
        doc.add(field)
      }

      doc.add(new StoredField(strategy.getFieldName(), shapeToString(shape)))
    }

    doc
  }

  private val (iterOriginal, iterIndex) = iter.duplicate

  iterIndex.foreach { case (key, value) =>
    // (implicitly) convert type K to Shape and V to a Lucene document
    val doc = docConversion(value)
    val shape = shapeConversion(key)
    val docWithLocation = decorateWithLocation(doc, Seq(shape))
    indexWriter.addDocument(FacetsConfig.build(taxoWriter, docWithLocation))
  }

  // Close the indexWriter and taxonomyWriter (for faceted search)
  closeAllWriters()

  private val indexReader = DirectoryReader.open(IndexDir)
  private val indexSearcher = new IndexSearcher(indexReader)
  private val taxoReader = new DirectoryTaxonomyReader(TaxonomyDir)

  override def size: Long = iterOriginal.size

  /**
   * Restricts the entries to those satisfying a predicate
   *
   * @param pred
   * @return
   */
  override def filter(pred: (K, V) => Boolean): AbstractShapeLuceneRDDPartition[K, V] = {
    ShapeLuceneRDDPartition(iterOriginal.filter(x => pred(x._1, x._2)))
  }

  override def isDefined(key: K): Boolean = iterOriginal.exists(_._1 == key)

  override def close(): Unit = {
    indexReader.close()
    taxoReader.close()
  }

  override def iterator: Iterator[(K, V)] = iterOriginal

  private def docLocation(scoreDoc: ScoreDoc): Option[Shape] = {
    val shapeString = indexReader.document(scoreDoc.doc)
                  .getField(strategy.getFieldName())
                  .stringValue()

   try{
     Some(shapeReader.read(new StringReader(shapeString)))
   }
    catch {
      case _: Throwable => None
    }
  }

  override def circleSearch(center: (Double, Double), radius: Double, k: Int, operationName: String)
  : Iterable[SparkScoreDoc] = {
    val args = new SpatialArgs(SpatialOperation.get(operationName),
        ctx.makeCircle(center._1, center._2,
        DistanceUtils.dist2Degrees(radius, DistanceUtils.EARTH_MEAN_RADIUS_KM)))

    val query = strategy.makeQuery(args)
    val docs = indexSearcher.search(query, k)
    docs.scoreDocs.map(SparkScoreDoc(indexSearcher, _))
  }

  override def knnSearch(point: (Double, Double), k: Int): List[SparkScoreDoc] = {

    // Match all, order by distance ascending
    val pt = ctx.makePoint(point._1, point._2)

    // the distance (in km)
    val valueSource = strategy.makeDistanceValueSource(pt, DistanceUtils.DEG_TO_KM)

    // false = ascending dist
    val distSort = new Sort(valueSource.getSortField(false)).rewrite(indexSearcher)

    val docs = indexSearcher.search(LuceneQueryHelpers.MatchAllDocs, k, distSort)

    // To get the distance, we could compute from stored values like earlier.
    // However in this example we sorted on it, and the distance will get
    // computed redundantly.  If the distance is only needed for the top-X
    // search results then that's not a big deal. Alternatively, try wrapping
    // the ValueSource with CachingDoubleValueSource then retrieve the value
    // from the ValueSource now. See LUCENE-4541 for an example.
    docs.scoreDocs.flatMap { case scoreDoc => {
        val location = docLocation(scoreDoc)
        location.map { case shape =>
          SparkScoreDoc(indexSearcher, scoreDoc,
            ctx.calcDistance(pt, shape.getCenter).toFloat)
        }
      }
    }.toList
  }

  override def spatialSearch(shapeAsString: String, k: Int, operationName: String)
  : Iterable[SparkScoreDoc] = {
    val shape = shapeReader.read(new StringReader(shapeAsString))
    val args = new SpatialArgs(SpatialOperation.get(operationName), shape)
    val query = strategy.makeQuery(args)
    val docs = indexSearcher.search(query, k)
    docs.scoreDocs.map(SparkScoreDoc(indexSearcher, _))
  }
}

object ShapeLuceneRDDPartition {

  def apply[K: ClassTag, V: ClassTag]
  (iter: Iterator[(K, V)])
  (implicit shapeConv: K => Shape,
   docConv: V => Document): ShapeLuceneRDDPartition[K, V] = {
    new ShapeLuceneRDDPartition[K, V](iter) (classTag[K], classTag[V]) (shapeConv, docConv)
  }
}
