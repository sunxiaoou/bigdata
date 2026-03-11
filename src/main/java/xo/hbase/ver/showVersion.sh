#!/bin/bash

HBASE_HOME=/opt/hbase-2.5.11-hadoop3-client
export CLASSPATH=.:$HBASE_HOME/lib/*:$HBASE_HOME/lib/client-facing-thirdparty/*

sed -i '' '1{/^[[:space:]]*package/d;}' ShowVersion.java
javac -cp $CLASSPATH ShowVersion.java
java -Dlog4j.configuration=file:log4j.properties -cp $CLASSPATH ShowVersion \
    -h hbk_h4 -p hbase/hadoop4@XO.COM -k hadoop.keytab
