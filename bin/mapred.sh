#!/bin/bash

mr() {
    cd $HADOOP_HOME
    mkdir -p input
    echo test apache hadoop hadoop sqoop hue mapreduce sqoop oozie http > input/in.txt
    hdfs dfs -rm -f -r input
    hdfs dfs -mkdir input
    hdfs dfs -put input/in.txt input
    hdfs dfs -rm -f -r output
    hadoop jar $jarfile wordcount input output
    hdfs dfs -cat output/*
}

ver=$(hadoop version | grep Hadoop | awk '{print $NF}')
jarfile=$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-$ver.jar
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "/user/$USER"
"$HADOOP_HOME/bin/hdfs" dfs -ls -R "/user"
mr