#!/bin/sh

# cd out/production/mapreduce
# jar cvf ../../../wordcount.jar *.class
# cd ../../..


# mkdir -p input
# echo hive apache hadoop hadoop sqoop hue mapreduce sqoop oozie http > input/in.txt
# hdfs dfs -mkdir -p input
# hdfs dfs -put input/in.txt input
hdfs dfs -rm -f -r output
hadoop jar wordcount.jar WordCount input output
hdfs dfs -cat output/*
