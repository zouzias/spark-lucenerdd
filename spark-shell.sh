#!/usr/bin/env bash

CURRENT_DIR=`pwd`

# Read the version from version.sbt
SPARK_LUCENERDD_VERSION=`cat version.sbt | awk '{print $5}' | xargs`

echo "==============================================="
echo "Loading LuceneRDD with version ${SPARK_LUCENERDD_VERSION}"
echo "==============================================="

# Assumes that spark is installed under home directory
HOME_DIR=`echo ~`
#export SPARK_LOCAL_IP=localhost
SPARK_HOME=${HOME_DIR}/spark-2.0.1-bin-hadoop2.7

# spark-lucenerdd assembly JAR
MAIN_JAR=${CURRENT_DIR}/target/scala-2.11/spark-lucenerdd-assembly-${SPARK_LUCENERDD_VERSION}.jar

# Run spark shell locally
${SPARK_HOME}/bin/spark-shell   --jars "${MAIN_JAR}" \
				--conf "spark.executor.memory=1g" \
				--conf "spark.driver.memory=512m" \
				--conf "spark.rdd.compress=true" \
				--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
				--conf "spark.kryoserializer.buffer=24mb" \
				--master local[*]
