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
SPARK_HOME=${HOME_DIR}/spark-1.5.2-bin-2.6.0

# spark-lucenerdd assembly JAR
MAIN_JAR=${CURRENT_DIR}/target/scala-2.11/spark-lucenerdd-assembly-${SPARK_LUCENERDD_VERSION}.jar
SPARK_CSV_JAR=${CURRENT_DIR}/spark-csv_2.11-1.4.0.jar
COMMON_CSV_JAR=${CURRENT_DIR}/commons-csv-1.1.jar

# Run spark shell locally
${SPARK_HOME}/bin/spark-shell   --jars "${MAIN_JAR},${SPARK_CSV_JAR},${COMMON_CSV_JAR}" \
				--conf "spark.executor.memory=512m" \
				--conf "spark.driver.memory=512m" \
				--master local[2]
