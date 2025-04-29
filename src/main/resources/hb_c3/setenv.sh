#!/bin/sh

WORK_DIR=~/learn/java/bigdata
export HADOOP_CONF_DIR=$WORK_DIR/src/main/resources/hb_c3
export HDFS_CONF_DIR=$HADOOP_CONF_DIR
export HBASE_CONF_DIR=$HADOOP_CONF_DIR
export KRB5_CONFIG=/etc/krb5.conf
export HADOOP_OPTS="-Djava.security.krb5.conf=$KRB5_CONFIG\
 -Dzookeeper.server.principal=zookeeper/centos3@EXAMPLE.COM\
 -Djava.security.auth.login.config=$HBASE_CONF_DIR/client.jaas\
 -Djava.net.preferIPv4Stack=true\
 -Djava.net.preferIPv6Addresses=false\
 -Xmx128m\
 -Djava.io.tmpdir=$WORK_DIR/tmp"
export HBASE_OPTS="-Djava.security.krb5.conf=$KRB5_CONFIG\
 -Djavax.security.auth.useSubjectCredsOnly=false\
 -Dzookeeper.server.principal=zookeeper/centos3@EXAMPLE.COM\
 -Djava.security.auth.login.config=$HBASE_CONF_DIR/client.jaas\
 -Djava.io.tmpdir=$WORK_DIR/tmp\
 -Dorg.xerial.snappy.tempdir=$WORK_DIR/tmp"