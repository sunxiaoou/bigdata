#!/bin/sh

if [ -d $HBASE_HOME ]; then
    echo "Cleaning logs under $HBASE_HOME/logs"
    rm -f $HBASE_HOME/logs/*.log $HBASE_HOME/logs/*.out
fi

if [ -d $HADOOP_HOME ]; then
    echo "Cleaning logs under $HADOOP_HOME/logs"
    rm -f $HADOOP_HOME/logs/*.log $HADOOP_HOME/logs/*.out $HADOOP_HOME/logs/*.audit
fi

if [ -d $ZOOKEEPER_HOME ]; then
    echo "Cleaning logs under $ZOOKEEPER_HOME/logs"
    rm -f $ZOOKEEPER_HOME/logs/*.out
fi