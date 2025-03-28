#!/bin/bash

TAR_DIR="$HOME/Downloads/bigdata"
ZOOKEEPER_TAR="apache-zookeeper-3.8.1-bin.tar.gz"
KAFKA_TAR="kafka_2.13-3.3.1.tgz"
HADOOP_TAR="hadoop-2.10.2.tar.gz"
HBASE_TAR="hbase-2.4.16-bin.tar.gz"
DEFAULT_PARENT_DIR="$HOME/bigdata"
GROUP=$(id -gn)
PID_DIR=/run/hadoop


# Step 1: Install Zookeeper
install_zookeeper() {
    echo "Installing Zookeeper..."

    # Extract Zookeeper
    parent=$(dirname "$ZOOKEEPER_HOME")
    sudo mkdir -p "$parent"
    sudo tar xf "$TAR_DIR/$ZOOKEEPER_TAR" -C "$parent"
    sudo mv "$parent"/${ZOOKEEPER_TAR%.tar.gz} "$ZOOKEEPER_HOME"
    sudo chown -R "$USER:$GROUP" "$ZOOKEEPER_HOME"

    # Configure zoo.cfg
    zk_conf="$ZOOKEEPER_HOME/conf"
    cp "$zk_conf/zoo_sample.cfg" "$zk_conf/zoo.cfg"
    sed -i -e "s|/tmp/zookeeper|${ZOOKEEPER_HOME}/tmp|" \
        -e "s|#autopurge.purgeInterval=1|autopurge.purgeInterval=1|" \
        "$zk_conf/zoo.cfg"

    # Create necessary directories and start Zookeeper
    mkdir -p "$ZOOKEEPER_HOME/tmp"
    "$ZOOKEEPER_HOME/bin/zkServer.sh" start
}

# Step 2: Install Kafka
install_kafka() {
    echo "Installing Kafka..."

    # Extract Kafka
    parent=$(dirname "$KAFKA_HOME")
    sudo mkdir -p "$parent"
    sudo tar xf "$TAR_DIR/$KAFKA_TAR" -C "$parent"
    sudo mv "$parent"/${KAFKA_TAR%.tgz} "$KAFKA_HOME"
    sudo chown -R "$USER:$GROUP" "$KAFKA_HOME"

    # Configure server.properties
    kfk_conf="$KAFKA_HOME/config"
    orig.sh "$kfk_conf/server.properties"
    sed -i "s|/tmp/kafka-logs|${KAFKA_HOME}/tmp|" "$kfk_conf"/server.properties

    # Create necessary directories and start Kafka
    mkdir -p "$KAFKA_HOME"/tmp
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$KAFKA_HOME"/config/server.properties
    sleep 2
    if grep -P "ERROR|\tat" "$KAFKA_HOME"/logs/*.log; then
        echo "Error found in Kafka logs"
        exit 1
    fi
}

# Step 3: Install Hadoop
install_hadoop() {
    echo "Installing Hadoop..."

    # Extract Hadoop
    parent=$(dirname "$HADOOP_HOME")
    sudo mkdir -p "$parent"
    sudo tar xf "$TAR_DIR/$HADOOP_TAR" -C "$parent"
    sudo chown -R "$USER:$GROUP" "$HADOOP_HOME"

    # Configure Hadoop environment
    ha_conf="$HADOOP_HOME/etc/hadoop"
    orig.sh "$ha_conf/hadoop-env.sh" "$ha_conf/mapred-env.sh" "$ha_conf/yarn-env.sh"
    sed -i -e "s|\${JAVA_HOME}|${JAVA_HOME}|" -e "s|\${HADOOP_PID_DIR}|${PID_DIR}|" \
         "$ha_conf/hadoop-env.sh"
    sed -i "s|#export HADOOP_MAPRED_PID_DIR=|export HADOOP_MAPRED_PID_DIR=${PID_DIR}|" \
         "$ha_conf/mapred-env.sh"
    sed -i "\$a\export YARN_PID_DIR=${PID_DIR}" "$ha_conf/yarn-env.sh"
    sudo mkdir -p $PID_DIR
    sudo chown -R "$USER:$GROUP" "$PID_DIR"

    cat > "$ha_conf/core-site.xml" <<EOL
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://$HOSTNAME:8020</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>$HADOOP_HOME/data/tmp</value>
    </property>
</configuration>
EOL

    cat > "$ha_conf/hdfs-site.xml" <<EOL
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>$HOSTNAME:50070</value>
    </property>
    <property>
        <name>dfs.datanode.address</name>
        <value>$HOSTNAME:50010</value>
    </property>
    <property>
        <name>dfs.datanode.http.address</name>
        <value>$HOSTNAME:50075</value>
    </property>
    <property>
        <name>dfs.datanode.ipc.address</name>
        <value>$HOSTNAME:50020</value>
    </property>
</configuration>
EOL

    cat > "$ha_conf/mapred-site.xml" <<EOL
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>$HOSTNAME:10020</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>$HOSTNAME:19888</value>
    </property>
</configuration>
EOL

    cat > "$ha_conf/yarn-site.xml" <<EOL
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>$HOSTNAME</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>604800</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.class</name>
        <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>
    </property>
</configuration>
EOL

    # Format HDFS and start Hadoop
    mkdir -p "$HADOOP_HOME/data/tmp"
    "$HADOOP_HOME/bin/hdfs" namenode -format
    "$HADOOP_HOME/sbin/start-dfs.sh"
    "$HADOOP_HOME/sbin/start-yarn.sh"
    "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /tmp/logs/
    "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh" start historyserver
    if grep -P "ERROR|\tat" "$HADOOP_HOME"/logs/*.log; then
        echo "Error found in Hadoop logs"
        exit 1
    fi
}

# Step 4: Install HBase
install_hbase() {
    echo "Installing HBase..."

    # Extract HBase
    parent=$(dirname "$HBASE_HOME")
    sudo mkdir -p "$parent"
    sudo tar xf "$TAR_DIR/$HBASE_TAR" -C "$parent"
    sudo chown -R "$USER:$GROUP" "$HBASE_HOME"
    dup_jar=$HBASE_HOME/lib/client-facing-thirdparty/slf4j-reload4j-1.7.33.jar
    if [ -f "$dup_jar" ]; then
        mv "$dup_jar" "$dup_jar".bak
    fi

    # Configure HBase environment
    hb_conf="$HBASE_HOME/conf"
    orig.sh "$hb_conf/hbase-env.sh"
    sed -i -e "s|# export JAVA_HOME=/usr/java/jdk1.8.0/|export JAVA_HOME=${JAVA_HOME}|" \
        -e "s|# export HBASE_PID_DIR=/var/hadoop/pids|export HBASE_PID_DIR=${PID_DIR}|" \
        -e "s|# export HBASE_MANAGES_ZK=true|export HBASE_MANAGES_ZK=false|" \
        "$hb_conf/hbase-env.sh"
    sudo mkdir -p $PID_DIR
    sudo chown -R "$USER:$GROUP" "$PID_DIR"

    cat > "$hb_conf/hbase-site.xml" <<EOL
<configuration>
    <property>
        <name>hbase.cluster.distributed</name>
        <value>true</value>
    </property>
    <property>
        <name>hbase.rootdir</name>
        <value>hdfs://$HOSTNAME:8020/hbase</value>
    </property>
    <property>
        <name>hbase.zookeeper.quorum</name>
        <value>$HOSTNAME</value>
    </property>
        <property>
        <name>hbase.replication.cluster.id</name>
        <value>$HOSTNAME</value>
    </property>
    <property>
        <name>hbase.replication.bulkload.enabled</name>
        <value>true</value>
    </property>
</configuration>
EOL

    # Start HBase
    "$HBASE_HOME/bin/start-hbase.sh"
    if grep -P "ERROR|\tat" "$HBASE_HOME"/logs/*.log; then
        echo "Error found in HBase logs"
        exit 1
    fi
}

# Step 5: Verify Installation
verify_installation() {
    echo "Verifying Zookeeper..."
    pid=$(jps | grep -w QuorumPeerMain | awk '{print $1}')
    if [ -z "$pid" ]; then
        echo "QuorumPeerMain doesn't exist" 
        return
    fi    
    netstat -lnpt | grep -i TCP | grep "$pid"

    echo "Verifying Hadoop and HDFS..."
    "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "/user/$USER"
    "$HADOOP_HOME/bin/hdfs" dfs -ls -R "/user"
    mapred.sh

    echo "Verifying HBase..."
    echo "create_namespace 'manga'" | "$HBASE_HOME/bin/hbase" shell
}

# Main function to execute all steps
main() {
    if [ -z "$JAVA_HOME" ]; then
        echo "JAVA_HOME must be set"
        exit 1
    fi

    if [ -z "$ZOOKEEPER_HOME" ]; then
        ver=$(echo $ZOOKEEPER_TAR | sed -n 's/.*\-\([0-9]\+\.[0-9]\+\.[0-9]\+\)\-.*/\1/p')
        ZOOKEEPER_HOME=$DEFAULT_PARENT_DIR/zookeeper-$ver
    fi

    if [ -z "$KAFKA_HOME" ]; then
        ver=$(echo $KAFKA_TAR | sed -n 's/.*\-\([0-9]\+\.[0-9]\+\.[0-9]\+\).*/\1/p')
        KAFKA_HOME=$DEFAULT_PARENT_DIR/kafka-$ver
    fi

    if [ -z "$HADOOP_HOME" ]; then
        ver=$(echo $HADOOP_TAR | sed -n 's/.*\-\([0-9]\+\.[0-9]\+\.[0-9]\+\).*/\1/p')
        HADOOP_HOME=$DEFAULT_PARENT_DIR/hadoop-$ver
    fi

    if [ -z "$HBASE_HOME" ]; then
        ver=$(echo $HBASE_TAR | sed -n 's/.*\-\([0-9]\+\.[0-9]\+\.[0-9]\+\).*/\1/p')
        HBASE_HOME=$DEFAULT_PARENT_DIR/hbase-$ver
    fi

    install_zookeeper
    install_kafka
    install_hadoop
    install_hbase
    verify_installation

    ip=$(hostname -I | awk '{print $1}')
    echo "HBase related urls"
    echo "http://$ip:50070/explorer.html#/"
    echo "http://$ip:8088/cluster"
    echo "http://$ip:16010/master-status"
}

# Run main function
main
