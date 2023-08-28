#!/bin/sh

cd $HBASE_HOME
# hdfs dfs -put student.csv input/
# hdfs dfs -rm -r -f output/student

bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
	-Dimporttsv.separator=, \
	-Dimporttsv.columns='HBASE_ROW_KEY,cf:name,cf:gender,cf:phone,cf:mail' \
	-Dimporttsv.bulk.output=hdfs://ubuntu:8020/user/sunxo/output/student \
	manga:student \
	input/student.csv


bin/hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles \
	hdfs://ubuntu:8020/user/sunxo/output/student \
	manga:student
