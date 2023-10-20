#!/bin/sh

hosts="hadoop3 hadoop4 hadoop5"

zk_strt() {
    for host in $hosts
    do
        echo "============= zk start on $host ============="
        ssh $host $ZOOKEEPER_HOME/bin/zkServer.sh start
    done
}

zk_stop() {
    for host in $hosts
    do
        echo "============= zk stop on $host ============="
        ssh $host $ZOOKEEPER_HOME/bin/zkServer.sh stop
    done
}

hadoop_strt() {
    echo "============= dfs start from hadoop3 ============="
    ssh hadoop3 $HADOOP_HOME/sbin/start-dfs.sh
    echo "============= yarn start from hadoop4 ============="
    ssh hadoop4 $HADOOP_HOME/sbin/start-yarn.sh
    echo "============= history start on hadoop3 ============="
    ssh hadoop3 $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver
}

hadoop_stop() {
    echo "============= history stop on hadoop3 ============="
    ssh hadoop3 $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver
    echo "============= yarn stop from hadoop4 ============="
    ssh hadoop4 $HADOOP_HOME/sbin/stop-yarn.sh
    echo "============= dfs stop from hadoop3 ============="
    ssh hadoop3 $HADOOP_HOME/sbin/stop-dfs.sh
}

hbase_strt() {
    echo "============= hbase start from hadoop3 ============="
    $HBASE_HOME/bin/start-hbase.sh
}

hbase_stop() {
    echo "============= hbase stop from hadoop3 ============="
    $HBASE_HOME/bin/stop-hbase.sh
}

if [ $# -lt 2 ]
then
    echo "Usage: `basename $0` zk|hadoop|hbase strt|stop"
    exit 1
fi

echo $1_$2
$1_$2
