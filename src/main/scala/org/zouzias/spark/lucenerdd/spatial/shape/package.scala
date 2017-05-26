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
package org.zouzias.spark.lucenerdd.spatial

import java.io.StringReader

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.locationtech.spatial4j.shape.Shape
import org.zouzias.spark.lucenerdd.spatial.shape.context.ContextLoader


package object shape extends ContextLoader{

  private val GeometryFactory = new GeometryFactory()

  implicit def convertToPoint(point: (Double, Double)): Shape = {
    ctx.makePoint(point._1, point._2)
  }

  /**
   * ***Experimental***
   *
   * Implicitly convert shape from its string representation
   *
   * @param shapeAsString
   * @return
   */
  implicit def WKTToShape(shapeAsString: String): Shape = {
    try {
      shapeReader.read(new StringReader(shapeAsString))
    }
    catch {
      case e: Exception => ctx.makePoint(0.0, 0.0)
    }
  }

  implicit def rectangleToShape(rect: (Double, Double, Double, Double)): Shape = {
    val minX = rect._1
    val maxX = rect._2
    val minY = rect._3
    val maxY = rect._4
    ctx.makeRectangle(minX, maxX, minY, maxY)
  }

  implicit def circleToShape(circle: ((Double, Double), Double)): Shape = {
    val x = circle._1._1
    val y = circle._1._2
    val radius = circle._2
    ctx.makeCircle(x, y, radius)
  }

  implicit def listPolygonToShape(rect: List[(Double, Double)]): Shape = {
    val coordinates = rect.map(p => new Coordinate(p._1, p._2)).toArray
    val polygon = GeometryFactory.createPolygon(coordinates)
    ctx.makeShape(polygon)
  }

  implicit def arrayPolygonToShape(rect: Array[(Double, Double)]): Shape = {
    val coordinates = rect.map(p => new Coordinate(p._1, p._2))
    val polygon = GeometryFactory.createPolygon(coordinates)
    ctx.makeShape(polygon)
  }
}
