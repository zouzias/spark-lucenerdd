# spark-lucenerdd

[![Master](https://travis-ci.org/zouzias/spark-lucenerdd.svg?branch=master)](https://travis-ci.org/zouzias/spark-lucenerdd)

Spark RDD with Apache [Lucene](https://lucene.apache.org)'s query capabilities.


### Development

Install Java, [SBT]() and clone the project the project

```bash
git clone https://github.com/zouzias/spark-lucenerdd.git
cd spark-lucenerdd
```

Next, to create the assembly jar, type

```bash
sbt compile assembly
```

To make the spark-lucenerdd available, you have to assembly the project and add the JAR on you Spark shell or submit scripts.

