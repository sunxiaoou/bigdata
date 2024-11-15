#! /bin/sh

if [ $# -lt 1 ]
then
    echo "Usage: $0 xml_dir"
    exit 1
fi

xml_dir=$1
mkdir -p "$xml_dir"
parent=$PWD
cd "$HADOOP_HOME"/etc/hadoop || exit
cp -p core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml "$parent/$xml_dir"
cd "$HBASE_HOME"/conf || exit
cp -p hbase-site.xml "$parent/$xml_dir"