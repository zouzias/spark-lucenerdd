#!/usr/bin/env bash

# Assumes that spark is installed under home directory

CURRENT_DIR=`pwd`
HOME_DIR=`echo ~`

# To access HDFS
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

#export SPARK_LOCAL_IP=localhost

SPARK_HOME=${HOME_DIR}/spark-1.5.2-bin-2.6.0
#SPARK_HOME=${HOME_DIR}/spark-1.5.1-bin-hadoop2.6

MAIN_JAR=${CURRENT_DIR}/target/scala-2.11/spark-lucenerdd-assembly-0.0.2-SNAPSHOT.jar

# Run it using yarn
${SPARK_HOME}/bin/spark-shell   --jars "${MAIN_JAR}" \
				--conf "spark.executor.memory=512m" \
				--conf "spark.driver.memory=512m" \
				--master local[2]
