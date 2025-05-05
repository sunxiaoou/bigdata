#!/bin/bash

CLASS_PATH="$HBASE_HOME/lib/*:$HBASE_HOME/lib/jdbc/*:$HBASE_HOME/lib/client-facing-thirdparty/*:.:target/bigdata-1.0-SNAPSHOT.jar"
JAVA_OPTS="-Dlog4j.configuration=file:log4j.properties"
Z_PRINCIPAL="zookeeper/hadoop.hadoop.com"
PRINCIPAL="loader_hive1@HADOOP.COM"
KEYTAB="hb_mrs/loader_hive1.keytab"


function list2() {
    java -cp "$CLASS_PATH" "$JAVA_OPTS" xo.hbase.Snapshot --zPrincipal $Z_PRINCIPAL \
        --principal $PRINCIPAL --keytab $KEYTAB --db hb_mrs --table manga:fruit --action list
}

function scan2() {
    java -cp "$CLASS_PATH" "$JAVA_OPTS" xo.hbase.Fruit -z $Z_PRINCIPAL -p $PRINCIPAL -k $KEYTAB -h hb_mrs -a scan
}

function create() {
    java -cp "$CLASS_PATH" "$JAVA_OPTS" xo.hbase.Snapshot --db hb_u --table manga:fruit --action create
}

function clone2() {
    java -cp "$CLASS_PATH" "$JAVA_OPTS" xo.hbase.Snapshot --zPrincipal $Z_PRINCIPAL \
        --principal $PRINCIPAL --keytab $KEYTAB --db hb_mrs --table manga:fruit --action clone
}

function distcp2() {
    java -cp "$CLASS_PATH" "$JAVA_OPTS" xo.hbase.Snapshot --zPrincipal $Z_PRINCIPAL \
        --principal $PRINCIPAL --keytab $KEYTAB --db hb_u --db2 hb_mrs --table manga:fruit --action distcp
}

function export2() {
    java -cp "$CLASS_PATH" "$JAVA_OPTS" xo.hbase.Snapshot --zPrincipal $Z_PRINCIPAL \
        --principal $PRINCIPAL --keytab $KEYTAB --db hb_u --db2 hb_mrs --table manga:fruit --action export
}

function startSvr() {
    java -cp "$CLASS_PATH" "$JAVA_OPTS" xo.hbase.ExportSnapshotServer
}

function repl() {
    java -cp "$CLASS_PATH" "$JAVA_OPTS" xo.hbase.ReplicateSnapshot
}

### Main script execution
if [ $# -lt 1 ]
then
    echo "Usage: $0 create | export2 | clone2 | list2 | scan2 | repl"
    exit 1
fi

if [ -z "$HBASE_HOME" ]; then
    echo "HBASE_HOME must be set"
    exit 1
fi

cd "$BIGDATA_CLIENT_HOME"/java
echo "$1"
$1