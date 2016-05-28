# spark-lucenerdd

[![Master](https://travis-ci.org/zouzias/spark-lucenerdd.svg?branch=master)](https://travis-ci.org/zouzias/spark-lucenerdd)
[![Coverage Status](https://coveralls.io/repos/github/zouzias/spark-lucenerdd/badge.svg?branch=master)](https://coveralls.io/github/zouzias/spark-lucenerdd?branch=master)

Scala 2.10 & 2.11 supported:  [![Maven](https://img.shields.io/maven-central/v/org.zouzias/spark-lucenerdd_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/org.zouzias/spark-lucenerdd_2.11/)

Spark RDD with Apache [Lucene](https://lucene.apache.org)'s query capabilities.

The main abstraction is a special type of `RDD` called `LuceneRDD`, which instantiates a Lucene index on each Spark executor (a.k.a. worker).

`LuceneRDD`'s responsibility is to collect and aggregate the search results from the Spark executors to the Spark driver. Currently, the following queries are supported:

|Operation| Syntax| Description |
|-------|---------------------|----------|
|Term query     | `LuceneRDD.termQuery(field, query, topK)`| Exact term search |
|Fuzzy query | `LuceneRDD.fuzzyQuery(field, query, maxEdits, topK)`| Fuzzy term search |
|Phrase query | `LuceneRDD.phraseQuery(field, query, topK)` | Phrase search |
|Prefix query | `LuceneRDD.prefixSearch(field, prefix, topK)` | Prefix search |
|Query parser | `LuceneRDD.query(queryString, topK)` | Query parser search|
|Faceted search| `LuceneRDD.facetQuery(queryString, field, topK)` | Faceted Search |

Using the query parser, you can perform prefix queries, fuzzy queries, prefix queries, etc. 
For more information on using Lucene's query parser, see [Query Parser](https://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html). 

For example, using the query parser you can perform prefix queries on the field named textField and prefix query 
`spar` as `LuceneRDD.query("textField:spar*", 10)`.

### Project Status and Limitations

Currently the Lucene index is only stored in memory and the following Lucene queries are supported under `LuceneRDD`:

Implicit conversions for tuples of size up to 7 with the types (Int, Float, Double, Long, String) are supported. (For phrase queries, the auxiliary class `org.zouzias.spark.lucenerdd.models.LuceneText` must be used.) For tuples, the field names are by default set to "_1", "_2", etc following Scala's naming conventions for these fields.

### Development

Install Java, [SBT](http://www.scala-sbt.org) and clone the project

```bash
git clone https://github.com/zouzias/spark-lucenerdd.git
cd spark-lucenerdd
sbt compile assembly
```

The above will create an assembly jar containing spark-lucenerdd functionality under `target/scala-*/spark-lucenerdd-assembly-*.jar`

To make the spark-lucenerdd available, you have to assembly the project and add the JAR on you Spark shell or submit scripts.

### Example usage

Download and install Apache Spark locally.

Setup your SPARK_HOME environment variable to your extracted spark directory, i.e., with Spark 1.5.2 extracted in your home directory, do

```bash
HOME_DIR=`echo ~`
export SPARK_HOME=${HOME_DIR}/spark-1.5.2-bin-2.6.0
```

```bash
./spark-shell.sh # Starts spark shell using spark-lucenerdd JAR
```

Now, `LuceneRDD` is available in Spark shell. In spark shell, type

```scala-2
scala> :load scripts/loadWords.scala
```
to instantiate an `LuceneRDD[String]` object containing the words from `src/test/resources/words.txt`

#### Term query

To perform a exact term query, do
```scala-2
scala> val results = luceneRDD.termQuery("_1", "hello", 10)
scala> results.foreach(println)
SparkScoreDoc(12.393539,129848,0,Numeric fields:Text fields:_1:[hello])
...
```

#### Prefix query

To perform a prefix query, do
```scala-2
scala> val results = luceneRDD.prefixQuery("_1", "hel", 10)
scala> results.foreach(println)
SparkScoreDoc(1.0,129618,0,Numeric fields:Text fields:_1:[held])
SparkScoreDoc(1.0,129617,0,Numeric fields:Text fields:_1:[helcotic])
SparkScoreDoc(1.0,129616,0,Numeric fields:Text fields:_1:[helcosis])
...
```

#### Fuzzy query

To perform a fuzzy query, do
```scala-2
scala> val results = luceneRDD.fuzzyQuery("_1", "aba", 1)
scala> results.foreach(println)
SparkScoreDoc(7.155413,175248,0,Numeric fields:Text fields:_1:[yaba])
SparkScoreDoc(7.155413,33820,0,Numeric fields:Text fields:_1:[paba])
...
```
