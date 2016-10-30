

import java.io.StringReader

import com.spatial4j.core.context.jts.JtsSpatialContext
import com.spatial4j.core.io.ShapeIO
import org.apache.spark.rdd.RDD
import org.zouzias.spark.lucenerdd.spatial.shape._
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.spatial.shape.rdds.ShapeLuceneRDD

import scala.reflect.ClassTag

sc.setLogLevel("INFO")

// Load all countries
val allCountries = spark.read.parquet("data/countries-poly.parquet").select("name", "shape").map(row => (row.getString(1), row.getString(0)))

// Load all cities
val capitals = spark.read.parquet("data/capitals.parquet").select("name", "shape").map(row => (row.getString(1), row.getString(0)))

def parseDouble(s: String): Double = try { s.toDouble } catch { case _: Throwable => 0.0 }

def coords(city: (String, String)): (Double, Double) = {
  val str = city._1
  val nums = str.dropWhile(x => x.compareTo('(') != 0).drop(1).dropRight(1)
  val coords = nums.split(" ").map(_.trim)
  (parseDouble(coords(0)), parseDouble(coords(1)))
}

val shapes = ShapeLuceneRDD(allCountries)
shapes.cache


val linked = shapes.linkByRadius(capitals.rdd, coords, 50, 10)
linked.cache

linked.map(x => (x._1, x._2.map(_.doc.textField("_1")))).foreach(println)