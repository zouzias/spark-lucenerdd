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

import org.locationtech.spatial4j.shape.{Point, Shape}
import org.zouzias.spark.lucenerdd.spatial.commons.context.ContextLoader
import org.zouzias.spark.lucenerdd.spatial.point.PointLuceneRDD.PointType

package object point extends ContextLoader{


  implicit def convertToPoint(point: (Double, Double)): Point = {
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
  implicit def wktToShape(shapeAsString: String): PointType = {
    try {
      val centroid = shapeReader.read(new StringReader(shapeAsString))
                      .getCenter
      (centroid.getX, centroid.getY)
    }
    catch {
      case _: Exception => (0.0, 0.0)
    }
  }
}
