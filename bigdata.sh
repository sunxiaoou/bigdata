#!/bin/sh

strt() {
    cd ~/work/hadoop-2.8.0
    rm -f logs/*
    sbin/start-dfs.sh
    sbin/start-yarn.sh
    sbin/mr-jobhistory-daemon.sh start historyserver
    cd ~/work/hive-2.3.9
    rm -f nohup.out logs/*
    nohup bin/hive --service metastore &
    nohup bin/hive --service hiveserver2 &
    cd ~/work/spark-3.2.1
    rm -f logs/*
    sbin/start-history-server.sh
    cd ~/work/oozie-5.2.1
    rm -f logs/*
    bin/oozied.sh start
}

stop() {
    cd ~/work/oozie-5.2.1
    bin/oozied.sh stop
    cd ~/work/spark-3.2.1
	sbin/stop-history-server.sh
    hives=`jps | grep RunJar | sed ':a; N; $!ba; s/RunJar\|\n//g'`
    kill $hives
    cd ~/work/hadoop-2.8.0
    sbin/mr-jobhistory-daemon.sh stop historyserver
    sbin/stop-yarn.sh
    sbin/stop-dfs.sh
}


if [ $# -lt 1 ]
then
    echo "Usage: $0 strt | stop"
    exit 1
fi

echo $1
$1
