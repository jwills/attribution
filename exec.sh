#!/bin/bash

HP=${HADOOP_PREFIX:?"Need to set HADOOP_PREFIX to client-side hadoop base dir"}

BASEDIR=$(dirname $0)
TARGET=$BASEDIR/target
MAINJAR=$TARGET/attribution-0.1.0.jar
LJ=$TARGET/lib/*.jar

if [ ! -e $MAINJAR ]
then
  echo "You need to run 'mvn clean install' to install the client jars"
  exit 1
fi

LIBJARS=$(echo ${LJ} | tr " " ",")
export LIBJARS=$MAINJAR,$LIBJARS
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
$HP/bin/hadoop jar $MAINJAR com.cloudera.science.attribution.MultiTouchAttributionModel -libjars ${LIBJARS} $@
