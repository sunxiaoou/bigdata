#!/bin/sh

strt() {
    cd $ZOOKEEPER_HOME
    bin/zkServer.sh start

    cd $KAFKA_HOME
    bin/kafka-server-start.sh -daemon config/server.properties

    pidDir=/run/hadoop
    if [ ! -d $pidDir ]
    then
        sudo mkdir -p $pidDir
        sudo chown $USER:$USER $pidDir
    fi

    cd $HADOOP_HOME
    rm -f logs/*
    sbin/start-dfs.sh

    if [ $AUTH_TYPE = "kerberos" ]; then
        echo "Kerberos enabled. Doing kinit..."
        kinit -kt $KERB5_HOME/keytabs/hadoop.keytab yarn/$HOSTNAME@EXAMPLE.COM
    fi
    sbin/start-yarn.sh
    sbin/mr-jobhistory-daemon.sh start historyserver

    cd $HBASE_HOME
    rm -f logs/*
    bin/start-hbase.sh
}

stop() {
    cd $HBASE_HOME
    if [ $AUTH_TYPE = "kerberos" ]; then
        echo "Kerberos enabled. Doing kinit..."
        kinit -kt $KERB5_HOME/keytabs/hadoop.keytab yarn/$HOSTNAME@EXAMPLE.COM
    fi
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

AUTH_TYPE=$(xmllint --xpath "string(//property[name='hadoop.security.authentication']/value)" $HADOOP_HOME/etc/hadoop/core-site.xml 2>/dev/null)
echo $1
$1