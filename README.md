# spark-lucenerdd

[![Master](https://travis-ci.org/zouzias/spark-lucenerdd.svg?branch=master)](https://travis-ci.org/zouzias/spark-lucenerdd)
[![Coverage Status](https://coveralls.io/repos/github/zouzias/spark-lucenerdd/badge.svg?branch=master)](https://coveralls.io/github/zouzias/spark-lucenerdd?branch=master)
[![Maven](https://img.shields.io/maven-central/v/org.zouzias/spark-lucenerdd_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/org.zouzias/spark-lucenerdd_2.11/)

Spark RDD with Apache [Lucene](https://lucene.apache.org)'s query capabilities. Both Scala 2.10 and 2.11 are supported.

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

Here are a few examples using `LuceneRDD` for full text search and entity linkage. The entity linkage is done via Lucene's flexible query language.

* [Text search](https://github.com/zouzias/spark-lucenerdd/wiki/Text-search-with-LuceneRDD)
* [Spatial search](https://github.com/zouzias/spark-lucenerdd/wiki/Spatial-search-using-ShapeLuceneRDD)
* [Entity linkage](https://github.com/zouzias/spark-lucenerdd/wiki/Record-Linkage-with-LuceneRDD)

For more, check the [wiki](https://github.com/zouzias/spark-lucenerdd/wiki)
