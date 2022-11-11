#!/bin/sh

strt() {
    cd ~/bigdata/hadoop-2.8.0
    rm -f logs/*
    sbin/start-dfs.sh
    sbin/start-yarn.sh
    sbin/mr-jobhistory-daemon.sh start historyserver
    cd ~/bigdata/hive-2.3.9
    rm -f nohup.out logs/*
    nohup bin/hive --service metastore &
    nohup bin/hive --service hiveserver2 &
    cd ~/bigdata/oozie-5.2.1
    rm -f logs/*
    bin/oozied.sh start
}

stop() {
    cd ~/bigdata/oozie-5.2.1
    bin/oozied.sh stop
    hives=`jps | grep RunJar | sed ':a; N; $!ba; s/RunJar\|\n//g'`
    kill $hives
    cd ~/bigdata/hadoop-2.8.0
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
