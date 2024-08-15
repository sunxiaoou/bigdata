#!/bin/sh

hosts="hadoop3 hadoop4 hadoop5"

create_PIDs_dir() {
    user=`whoami`
    for host in $hosts
    do
        echo "======== mkdir /run/hadoop on $host ========"
        ssh $host sudo mkdir -p /run/hadoop
        ssh $host sudo chown $user:$user /run/hadoop
    done
}

strt_zk() {
    for host in $hosts
    do
        echo "============= zk start on $host ============="
        ssh $host $ZOOKEEPER_HOME/bin/zkServer.sh start
    done
}

stop_zk() {
    for host in $hosts
    do
        echo "============= zk stop on $host ============="
        ssh $host $ZOOKEEPER_HOME/bin/zkServer.sh stop
    done
}

strt_hadoop() {
    echo "============= dfs start from hadoop3 ============="
    ssh hadoop3 $HADOOP_HOME/sbin/start-dfs.sh
    echo "============= yarn start from hadoop4 ============="
    ssh hadoop4 $HADOOP_HOME/sbin/start-yarn.sh
    echo "============= history start on hadoop3 ============="
    ssh hadoop3 $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver
}

stop_hadoop() {
    echo "============= history stop on hadoop3 ============="
    ssh hadoop3 $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver
    echo "============= yarn stop from hadoop4 ============="
    ssh hadoop4 $HADOOP_HOME/sbin/stop-yarn.sh
    echo "============= dfs stop from hadoop3 ============="
    ssh hadoop3 $HADOOP_HOME/sbin/stop-dfs.sh
}

strt_hbase() {
    echo "============= hbase start from hadoop3 ============="
    $HBASE_HOME/bin/start-hbase.sh
}

stop_hbase() {
    echo "============= hbase stop from hadoop3 ============="
    $HBASE_HOME/bin/stop-hbase.sh
}

if [ $# -lt 1 ]; then
    echo "Usage: `basename $0` mkdir"
    echo "       `basename $0` strt|stop zk|hadoop|hbase"
    exit 1
elif [ $# -lt 2 ]; then
    echo "create /run/hadoop/ for PIDs"
    create_PIDs_dir
else
    echo $1_$2
    $1_$2
fi
