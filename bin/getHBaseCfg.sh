#!/bin/bash

# Main function to execute all steps
main() {
    if [[ -z "$HADOOP_HOME" || -z "$HBASE_HOME" ]]; then
        echo "HADOOP_HOME_HOME and HBASE_HOME must be set"
        exit 1
    fi

    mkdir -p /tmp/hbcfg
    cd "$HADOOP_HOME"/etc/hadoop
    cp -p core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml /tmp/hbcfg
    cd $HBASE_HOME/conf
    cp -p hbase-site.xml hbase-env.sh /tmp/hbcfg
}

# Run main function
main
