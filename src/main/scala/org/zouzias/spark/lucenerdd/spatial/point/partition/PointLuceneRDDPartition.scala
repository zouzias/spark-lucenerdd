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

import com.spatial4j.core.context.SpatialContext
import com.spatial4j.core.distance.DistanceUtils
import com.spatial4j.core.shape.{Point, Shape}
import org.apache.lucene.document.{Document, StoredField}
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, ScoreDoc, Sort}
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree
import org.zouzias.spark.lucenerdd.analyzers.WSAnalyzer
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.query.LuceneQueryHelpers
import org.zouzias.spark.lucenerdd.store.IndexWithTaxonomyWriter

import scala.reflect._

private[lucenerdd] class PointLuceneRDDPartition[K, V]
  (private val iter: Iterator[(K, V)])
  (override implicit val kTag: ClassTag[K],
   override implicit val vTag: ClassTag[V])
  (implicit locationConversion: K => (Double, Double),
   docConversion: V => Document)
  extends AbstractPointLuceneRDDPartition[K, V]
    with WSAnalyzer
    with IndexWithTaxonomyWriter{

  /**
   * The Spatial4j {@link SpatialContext} is a sort of global-ish singleton
   * needed by Lucene spatial.  It's a facade to the rest of Spatial4j, acting
   * as a factory for {@link Shape}s and provides access to reading and writing
   * them from Strings.
   */
  private val ctx: SpatialContext = SpatialContext.GEO

  // results in sub-meter precision for geohash
  private val maxLevels = 11

  // This can also be constructed from SpatialPrefixTreeFactory
  private val grid = new GeohashPrefixTree(ctx, maxLevels)

  /**
   * The Lucene spatial {@link SpatialStrategy} encapsulates an approach to
   * indexing and searching shapes, and providing distance values for them.
   * It's a simple API to unify different approaches. You might use more than
   * one strategy for a shape as each strategy has its strengths and weaknesses.
   * <p />
   * Note that these are initialized with a field name.
   */
  private val strategy = new RecursivePrefixTreeStrategy(grid,
    PointLuceneRDDPartition.LocationDefaultField)

  private def decorateWithLocation(doc: Document, shapes: Iterable[Shape]): Document = {

    // Potentially more than one shape in this field is supported by some
    // strategies; see the Javadoc of the SpatialStrategy impl to see.
    shapes.foreach{ case shape =>
      strategy.createIndexableFields(shape).foreach{ case field =>
        doc.add(field)
      }

      // store it too; the format is up to you
      // (assume point in this example)
      val pt: Point = shape.asInstanceOf[Point]
      doc.add(new StoredField(strategy.getFieldName(), s"${pt.getX()} ${pt.getY()}"))
    }

    doc
  }

  private val (iterOriginal, iterIndex) = iter.duplicate

  iterIndex.foreach { case (key, value) =>
    // (implicitly) convert type K to [[Point]] and V to a Lucene document
    val doc = docConversion(value)
    val pt = locationConversion(key)
    val point = ctx.makePoint(pt._1, pt._2)
    val docWithLocation = decorateWithLocation(doc, Seq(point))
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
  override def filter(pred: (K, V) => Boolean): AbstractPointLuceneRDDPartition[K, V] = ???

  override def isDefined(key: K): Boolean = ???


  override def close(): Unit = {
    indexReader.close()
    taxoReader.close()
  }

  override def iterator: Iterator[(K, V)] = iterOriginal


  /**
   * Generic Lucene Query using QueryParser
   *
   * @param searchString Lucene query string, i.e., textField:hello*
   * @param topK         Number of documents to return
   * @return
   */
  override def query(searchString: String, topK: Int)
  : Iterable[SparkScoreDoc] = ???


  private def docLocation(scoreDoc: ScoreDoc): Option[(Double, Double)] = {
    val pointStr = indexReader.document(scoreDoc.doc)
                  .getField(strategy.getFieldName())
                  .stringValue()
    val coords = pointStr.split(' ')
    val coordDoubles = coords.map(_.toDouble)
    if (coordDoubles.length == 2) {
      Some((coordDoubles(0), coordDoubles(1)))
    }
    else {
      None
    }
  }

  override def knn(point: (Double, Double), k: Int): List[SparkScoreDoc] = {

    // Match all, order by distance ascending
    val pt = ctx.makePoint(point._1, point._2)

    // the distance (in km)
    val valueSource = strategy.makeDistanceValueSource(pt, DistanceUtils.DEG_TO_KM)


    // false = ascending dist
    val distSort = new Sort(valueSource.getSortField(false)).rewrite(indexSearcher)

    val docs = indexSearcher.search(LuceneQueryHelpers.MatchAllDocs, k, distSort)

    // scalastyle:off println
    docs.scoreDocs.map(x => (x.score, indexSearcher.doc(x.doc))).reverse.foreach(println)
    // scalastyle:on println


    // To get the distance, we could compute from stored values like earlier.
    // However in this example we sorted on it, and the distance will get
    // computed redundantly.  If the distance is only needed for the top-X
    // search results then that's not a big deal. Alternatively, try wrapping
    // the ValueSource with CachingDoubleValueSource then retrieve the value
    // from the ValueSource now. See LUCENE-4541 for an example.
    docs.scoreDocs.flatMap { case scoreDoc => {
        val location = docLocation(scoreDoc)
        location.map { case (x, y) =>
          SparkScoreDoc(indexSearcher, scoreDoc,
            ctx.calcDistance(pt, x, y).toFloat)
        }
      }
    }.toList
  }
}

object PointLuceneRDDPartition {
  val LocationDefaultField = "__location__"

  def apply[K: ClassTag, V: ClassTag]
  (iter: Iterator[(K, V)])
  (implicit keyToPoint: K => (Double, Double),
   docConversion: V => Document): PointLuceneRDDPartition[K, V] = {
    new PointLuceneRDDPartition[K, V](iter)(classTag[K], classTag[V])(keyToPoint, docConversion)
  }
}
