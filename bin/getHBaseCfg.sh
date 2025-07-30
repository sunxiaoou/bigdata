#!/bin/bash

# Main function to execute all steps
main() {
    if [[ -z "$HADOOP_HOME" || -z "$HBASE_HOME" ]]; then
        echo "HADOOP_HOME_HOME and HBASE_HOME must be set"
        exit 1
    fi

    mkdir -p /tmp/hbcfg

    AUTH_TYPE=$(hbase org.apache.hadoop.hbase.util.HBaseConfTool hadoop.security.authentication)
    if [[ "$AUTH_TYPE" == "kerberos" ]]; then
        if [[ -z "$KERB5_HOME" || -z "$ZOOKEEPER_HOME" ]]; then
            echo "KERB5_HOME and ZOOKEEPER_HOME must be set for Kerberos authentication"
            exit 1
        fi
        cp -p /etc/krb5.conf /tmp/hbcfg
        cp -p $KERB5_HOME/keytabs/hadoop.keytab /tmp/hbcfg
        cd $ZOOKEEPER_HOME/conf
        sed -e "s|Server|Client|" zoo-server.jaas > /tmp/hbcfg/zoo-client.jaas
    fi

    cd $HADOOP_HOME/etc/hadoop
    cp -p core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml /tmp/hbcfg
    if [[ "$AUTH_TYPE" == "kerberos" ]]; then
        cp -p ssl-client.xml ssl-server.xml /tmp/hbcfg
        sudo cp -p container-executor.cfg /tmp/hbcfg
        user=$USER
        sudo chown $user /tmp/hbcfg/container-executor.cfg
    fi

    cd $HBASE_HOME/conf
    cp -p hbase-site.xml hbase-env.sh /tmp/hbcfg
    if [[ "$AUTH_TYPE" == "kerberos" ]]; then
        cp -p client.jaas /tmp/hbcfg
    fi
}

# Run main function
main
