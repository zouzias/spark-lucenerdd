# spark-lucenerdd

[![Master](https://travis-ci.org/zouzias/spark-lucenerdd.svg?branch=master)](https://travis-ci.org/zouzias/spark-lucenerdd) [![Coverage Status](https://coveralls.io/repos/github/zouzias/spark-lucenerdd/badge.svg?branch=master)](https://coveralls.io/github/zouzias/spark-lucenerdd?branch=master)

Spark RDD with Apache [Lucene](https://lucene.apache.org)'s query capabilities.

### Development

Install Java, [SBT](http://www.scala-sbt.org) and clone the project

```bash
git clone https://github.com/zouzias/spark-lucenerdd.git
cd spark-lucenerdd
sbt compile assembly
```

The above will create an assembly jar containing spark-lucenerdd functionality under `target/scala-*/spark-lucenerdd-*.jar`

To make the spark-lucenerdd available, you have to assembly the project and add the JAR on you Spark shell or submit scripts.

### Example usage

Download and install Apache Spark locally.

Setup your SPARK_HOME enviroment variable to your extracted spark directory, say you extracted Spark 1.5.2 with Hadoop 2.6.0 in your home directory, do

```bash
HOME_DIR=`echo ~`
export SPARK_HOME=${HOME_DIR}/spark-1.5.2-bin-2.6.0
```

```bash
./spark-shell.sh # Starts spark shell using spark-lucenerdd
```

Now, `LuceneRDD` is available in Spark shell. type

```scala-2
:load scripts/loadWords.scala
```
to instantiate an `LuceneRDD[String]` object containing the words from `src/test/resources/words.txt`
