#!/bin/sh

if [ $# -lt 2 ]
then
    echo "Usage: $0 %Y-%m-%d %H:%i:%s"
    exit 1
fi


log=${0%.*}.log
echo "last_ts($1 $2)" > $log
imp_dir=/user/hive/warehouse/instant_price

HADOOP_HOME=~/work/hadoop
HADOOP_COMMON_HOME=$HADOOP_HOME
HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_HOME HADOOP_COMMON_HOME HADOOP_MAPRED_HOME
export PATH=$HADOOP_HOME/bin:$PATH

cd ~/work/sqoop-1.4.7
bin/sqoop import \
    --connect jdbc:mysql://localhost:3306/finance --username manga --password manga \
    --driver com.mysql.cj.jdbc.Driver \
    --table instant_price \
    --num-mappers 1 \
    --fields-terminated-by '\t' \
    --target-dir $imp_dir \
    --incremental append \
    --check-column ts \
    --last-value "$1 $2" >> $log 2>&1

