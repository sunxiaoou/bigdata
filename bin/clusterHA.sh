#!/bin/sh

#---------------+----------------------+----------------------+----------------------+
#               | node1                | node2                | node3                |
#---------------+----------------------+----------------------+----------------------+
# ZK            | QuorumPeerMain:2181  | QuorumPeerMain:2181  | QuorumPeerMain:2181  |
#---------------+----------------------+----------------------+----------------------+
# JN            | JournalNode:8485     | JournalNode:8485     | JournalNode:8485     |
#---------------+----------------------+----------------------+----------------------+
# DFS           | NameNode:8020        | NameNode:8020        |                      |
#               | DfsZkFc:8019         | DfsZkFc:8019         |                      |
#               | DataNode:9866        | DataNode:9866        | DataNode:9866        |
#--------------+----------------------+----------------------+--------------- -------+
# Yarn          |                      |                      | ResourceManager:8032 |
#               | NodeManager          | NodeManager          | NodeManager          |
#               |                      |                      | JobHistoryServ:19888 |
#---------------+----------------------+----------------------+----------------------|
# HBase         | HMaster:16000        |                      |                      |
#               | HRegionServer:16020  | HRegionServer:16020  | HRegionServer:16020  |
#---------------+----------------------+----------------------+----------------------|

node1=centos3
node2=centos1
node3=centos2
hosts="$node1 $node2 $node3"

crtPidDir() {
    for host in $hosts; do
        echo "======== creating /run/hadoop on $host ========"
        ssh $host sudo mkdir -p /run/hadoop
        ssh $host sudo chown $USER:$USER /run/hadoop
    done
}

clrLog() {
    for host in $hosts; do
        echo "======== clearing logs on $host ========"
        ssh $host find "/run/hadoop" -type f -exec rm -f {} +
        ssh $host find $ZOOKEEPER_HOME/logs -type f -exec rm -f {} +
        ssh $host find $HADOOP_HOME/logs -type f -exec rm -f {} +
        ssh $host find $HBASE_HOME/logs -type f -exec rm -f {} +
    done
}

start_zk() {
    for host in $hosts; do
        echo "============= [ZK] Starting on $host ============="
        ssh $host $ZOOKEEPER_HOME/bin/zkServer.sh start
    done
}

stop_zk() {
    for host in $hosts; do
        echo "============= [ZK] stoping on $host ============="
        ssh $host $ZOOKEEPER_HOME/bin/zkServer.sh stop
    done
}

start_jn() {
    for host in $hosts; do
        echo "============= [JN] starting on $host ============="
        ssh $host $HADOOP_HOME/bin/hdfs --daemon start journalnode
    done
}

stop_jn() {
    for host in $hosts; do
        echo "============= [JN] stoping on $host ============="
        ssh $host $HADOOP_HOME/bin/hdfs --daemon stop journalnode
    done
}

start_dfs() {
    format=false
    name_dir=$(hdfs getconf -confKey dfs.namenode.name.dir | cut -d',' -f1 | sed 's#^file://##')
    if [ ! -f "$name_dir/current/VERSION" ]; then
        echo "======== [NN] formatting on $node1 ========"
        ssh $node1 $HADOOP_HOME/bin/hdfs namenode -format
        echo "======== [FC] formatting on $node1 ========"
        ssh $node1 $HADOOP_HOME/bin/hdfs zkfc -formatZK
        format=true
    fi

    echo "============= [NN] starting on $node1 ============="
    ssh $node1 $HADOOP_HOME/bin/hdfs --daemon start namenode
    echo "============= [FC] starting on $node1 ============="
    ssh $node1 $HADOOP_HOME/bin/hdfs --daemon start zkfc

    if $format; then
        echo "============= [NN] standing by on $node2 ============="
        ssh $node2 $HADOOP_HOME/bin/hdfs namenode -bootstrapStandby
    fi

    echo "============= [NN] starting on $node2 ============="
    ssh $node2 $HADOOP_HOME/bin/hdfs --daemon start namenode
    echo "============= [FC] starting on $node2 ============="
    ssh $node2 $HADOOP_HOME/bin/hdfs --daemon start zkfc

    for host in $hosts; do
        echo "============= [DN] starting on $host ============="
        ssh $host $HADOOP_HOME/bin/hdfs --daemon start datanode
    done
}

stop_dfs() {
    for host in $hosts; do
        echo "============= [DN] stoping on $host ============="
        ssh $host $HADOOP_HOME/bin/hdfs --daemon stop datanode
    done

    echo "============= [FC] stop on $node2 ============="
    ssh $node2 $HADOOP_HOME/bin/hdfs --daemon stop zkfc
    echo "============= [NN] stop on $node2 ============="
    ssh $node2 $HADOOP_HOME/bin/hdfs --daemon stop namenode

    echo "============= [FC] stop on $node1 ============="
    ssh $node1 $HADOOP_HOME/bin/hdfs --daemon stop zkfc
    echo "============= [NN] stop on $node1 ============="
    ssh $node1 $HADOOP_HOME/bin/hdfs --daemon stop namenode
}

start_yarn() {
    echo "============= [RM] starting on $node3 ============="
    ssh $node3 $HADOOP_HOME/bin/yarn --daemon start resourcemanager

    for host in $hosts; do
        echo "============= [NM] starting on $host ============="
        ssh $host $HADOOP_HOME/bin/yarn --daemon start nodemanager
    done

    echo "============= [HS] starting on $node3 ============="
    ssh $node3 $HADOOP_HOME/bin/mapred --daemon start historyserver
}

stop_yarn() {
    echo "============= [HS] stoping on $node3 ============="
    ssh $node3 $HADOOP_HOME/bin/mapred --daemon stop historyserver

    for host in $hosts; do
        echo "============= [NM] stoping on $host ============="
        ssh $host $HADOOP_HOME/bin/yarn --daemon stop nodemanager
    done

    echo "============= [RM] stoping on $node3 ============="
    ssh $node3 $HADOOP_HOME/bin/yarn --daemon stop resourcemanager
}

start_hbase() {
    echo "============= [HM] starting on $node1 ============="
    ssh $node1 $HBASE_HOME/bin/hbase-daemon.sh start master

    for host in $hosts; do
        echo "============= [HR] starting on $host ============="
        ssh $host $HBASE_HOME/bin/hbase-daemon.sh start regionserver
    done
}

stop_hbase() {
    for host in $hosts; do
        echo "============= [HR] stoping on $host ============="
        ssh $host $HBASE_HOME/bin/hbase-daemon.sh stop regionserver
    done

    echo "============= [HM] stoping on $node1 ============="
    ssh $node1 $HBASE_HOME/bin/hbase-daemon.sh stop master
}

if [ $# -lt 1 ]; then
    echo "Usage: $(basename $0) crtPidDir|clrLog"
    echo "       $(basename $0) start|stop zk|jn|dfs|yarn|hbase"
    exit 1
elif [ $# -lt 2 ]; then
    $1
else
    echo $1_$2
    $1_$2
fi

