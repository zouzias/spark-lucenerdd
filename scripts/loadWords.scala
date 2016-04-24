import scala.io.Source
import org.zouzias.spark.rdd.lucenerdd.impl.RamLuceneRDDPartition
import org.zouzias.spark.rdd.lucenerdd.LuceneRDD._
import org.zouzias.spark.rdd.lucenerdd.LuceneRDD
val words = Source.fromFile("src/test/resources/words.txt").getLines()
val rdd = sc.parallelize(words.toSeq)
val luceneRDD = LuceneRDD(rdd, RamLuceneRDDPartition.stringConversion)
luceneRDD.count
