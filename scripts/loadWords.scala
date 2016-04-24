import scala.io.Source
import org.zouzias.spark.rdd.lucenerdd.impl.RamLuceneRDDPartition
import org.zouzias.spark.rdd.lucenerdd.impl.RamLuceneRDDPartition._
import org.zouzias.spark.rdd.lucenerdd.LuceneRDD._
import org.zouzias.spark.rdd.lucenerdd.LuceneRDD
val words = Source.fromFile("src/test/resources/words.txt").getLines().toSeq
val rdd = sc.parallelize(words)
val luceneRDD = LuceneRDD(rdd)
luceneRDD.count
