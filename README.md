# spark-lucenerdd

[![Master](https://travis-ci.org/zouzias/spark-lucenerdd.svg?branch=master)](https://travis-ci.org/zouzias/spark-lucenerdd)
[![codecov](https://codecov.io/gh/zouzias/spark-lucenerdd/branch/master/graph/badge.svg)](https://codecov.io/gh/zouzias/spark-lucenerdd)
[![Maven](https://img.shields.io/maven-central/v/org.zouzias/spark-lucenerdd_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/org.zouzias/spark-lucenerdd_2.11/)
[![Javadocs](http://javadoc.io/badge/org.zouzias/spark-lucenerdd_2.11.svg)](http://javadoc.io/doc/org.zouzias/spark-lucenerdd_2.11/)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/spark-lucenerdd/Lobby)


[Spark](http://spark.apache.org) RDD with Apache [Lucene](https://lucene.apache.org)'s query capabilities.

The main abstractions are special types of `RDD` called `LuceneRDD` and `ShapeLuceneRDD`, which instantiate a Lucene index on each Spark executor. These `RDD`s distribute search queries and aggregate search results between the Spark driver and its executors. Currently, the following queries are supported:

|Operation| Syntax| Description |
|-------|---------------------|----------|
|Term Query     | `LuceneRDD.termQuery(field, query, topK)`| Exact term search |
|Fuzzy Query | `LuceneRDD.fuzzyQuery(field, query, maxEdits, topK)`| Fuzzy term search |
|Phrase Query | `LuceneRDD.phraseQuery(field, query, topK)` | Phrase search |
|Prefix Query | `LuceneRDD.prefixSearch(field, prefix, topK)` | Prefix search |
|Query Parser | `LuceneRDD.query(queryString, topK)` | Query parser search|
|[Record Linkage](https://github.com/zouzias/spark-lucenerdd/wiki/Record-Linkage)| `LuceneRDD.link(otherEntity: RDD[T], linkageFct: T => searchQuery, topK)`| Record linkage via Lucene queries|
|Circle Search| `ShapeLuceneRDD.circleSearch((x,y), radius, topK)` | Search within radius |
|Bbox Search| `ShapeLuceneRDD.bboxSearch(lowerLeft, upperLeft, topK)` | Bounding box |
|Spatial Linkage| `ShapeLuceneRDD.linkByRadius(RDD[T], linkage: T => (x,y), radius, topK)` | Spatial radius linkage|

Using the query parser, you can perform prefix queries, fuzzy queries, prefix queries, etc and any combination of those.
For more information on using Lucene's query parser, see [Query Parser](https://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html).

### [Examples](https://github.com/zouzias/spark-lucenerdd-examples)

Here are a few examples using `LuceneRDD` for full text search, spatial search and record linkage. All examples exploit Lucene's flexible query language. For spatial search, `lucene-spatial` and `jts` are required.

* [Text Search](https://github.com/zouzias/spark-lucenerdd/wiki/Full-text-search)
* [Spatial Search](https://github.com/zouzias/spark-lucenerdd/wiki/Spatial-search)
* [Record Linkage](https://github.com/zouzias/spark-lucenerdd/wiki/Record-Linkage)

For more, check the [wiki](https://github.com/zouzias/spark-lucenerdd/wiki). More examples are available at [examples](https://github.com/zouzias/spark-lucenerdd-examples) and performance evaluation examples on AWS can be found [here](https://github.com/zouzias/spark-lucenerdd-aws).

## Presentations

For an overview of the library, check these [ScalaIO 2016 Slides](http://www.slideshare.net/zouzias/lucenerdd-for-geospatial-search-and-entity-linkage).

## Linking

You can link against this library (for Spark 1.4+) in your program at the following coordinates:

Using SBT:

```
libraryDependencies += "org.zouzias" %% "spark-lucenerdd" % "0.3.6"
```

Using Maven:

```xml
<dependency>
    <groupId>org.zouzias</groupId>
    <artifactId>spark-lucenerdd_2.11</artifactId>
    <version>0.3.6</version>
</dependency>
```

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

```
$ bin/spark-shell --packages org.zouzias:spark-lucenerdd_2.11:0.3.6
```

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

### Compatibility
The project has the following compatibility with Apache Spark:

Artifact                  | Release Date    | Spark compatibility | Notes | Status
------------------------- | --------------- | -------------------------- | ----- | ----
0.3.7-SNAPSHOT            |                 | >= 2.4.0, JVM 8  | [develop](https://github.com/zouzias/spark-lucenerdd/tree/develop) | Under Development
0.3.6                     |  2019-03-11     | >= 2.4.0, JVM 8  | [tag v0.3.6](https://github.com/zouzias/spark-lucenerdd/tree/v0.3.6) | Released
0.3.5                     |  2019-02-7     | >= 2.4.0, JVM 8  | [tag v0.3.5](https://github.com/zouzias/spark-lucenerdd/tree/v0.3.5) | Released
0.3.4                     |  2018-11-27     | >= 2.4.0, JVM 8  | [tag v0.3.4](https://github.com/zouzias/spark-lucenerdd/tree/v0.3.4) | Released
0.2.8                     | 2017-05-30      |  2.1.x, JVM 7      | [tag v0.2.8](https://github.com/zouzias/spark-lucenerdd/tree/v0.2.8) | Released
0.1.0                     | 2016-09-26      | 1.4.x, 1.5.x, 1.6.x| [tag v0.1.0](https://github.com/zouzias/spark-lucenerdd/tree/v0.1.0) | Cross-released with 2.10/2.11

### Project Status and Limitations

Implicit conversions for the primitive types (Int, Float, Double, Long, String) are supported. Moreover, implicit conversions for all product types (i.e., tuples and case classes) of the above primitives are supported. Implicits for tuples default the field names to "_1", "_2", "_3, ... following Scala's naming conventions for tuples.

### Custom Case Classes

If you want to use your own custom class with `LuceneRDD` you can do it provided that your class member types are one of the primitive types (Int, Float, Double, Long, String).

For more details, see `LuceneRDDCustomcaseClassImplicits` under the tests directory.

## Development

### Docker

A docker compose script is setup with some preliminary notebook in Zeppelin, run

```
docker-compose up
```

For more LuceneRDD examples on Zeppelin, check these [examples](https://github.com/zouzias/spark-lucenerdd-examples)
### Build from Source

Install Java, [SBT](http://www.scala-sbt.org) and clone the project

```bash
git clone https://github.com/zouzias/spark-lucenerdd.git
cd spark-lucenerdd
sbt compile assembly
```

The above will create an assembly jar containing spark-lucenerdd functionality under `target/scala-*/spark-lucenerdd-assembly-*.jar`
