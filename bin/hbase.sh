#!/bin/sh

strt() {
    cd $ZOOKEEPER_HOME
    bin/zkServer.sh start
    cd $KAFKA_HOME
    bin/kafka-server-start.sh -daemon config/server.properties

    pidDir=/run/hadoop
    if [ ! -d $pidDir ]
    then
        user=`whoami`
        sudo mkdir -p $pidDir
        sudo chown $user:$user $pidDir
    fi

    cd $HADOOP_HOME
    rm -f logs/*
    sbin/start-dfs.sh
    sbin/start-yarn.sh
    sbin/mr-jobhistory-daemon.sh start historyserver
    cd $HBASE_HOME
    rm -f logs/*
    bin/start-hbase.sh
}

stop() {
    cd $HBASE_HOME
    bin/stop-hbase.sh
    cd $HADOOP_HOME
    sbin/mr-jobhistory-daemon.sh stop historyserver
    sbin/stop-yarn.sh
    sbin/stop-dfs.sh
    cd $KAFKA_HOME
    bin/kafka-server-stop.sh
    cd $ZOOKEEPER_HOME
    bin/zkServer.sh stop
    # rm -rf /tmp/kafka-logs $ZOOKEEPER_HOME/tmp/version-2
}


if [ $# -lt 1 ]
then
    echo "Usage: $0 strt | stop"
    exit 1
fi

echo $1
$1
