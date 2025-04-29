#!/bin/bash

# Step 1: Modify Zookeeper
modify_zookeeper() {
    echo "modifying Zookeeper..."
    if [ -z "$ZOOKEEPER_HOME" ]; then
        echo "ERROR: ZOOKEEPER_HOME environment variable is not set!"
        exit 1
    fi

    # Create JAAS file for Zookeeper
    cat > "$ZOOKEEPER_HOME/conf/zoo-server.jaas" << EOF
Server {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="$KERB5_HOME/keytabs/hadoop.keytab"
  storeKey=true
  useTicketCache=false
  principal="zookeeper/$HOSTNAME@EXAMPLE.COM";
};
EOF
    local zooCfg="$ZOOKEEPER_HOME/conf/zoo.cfg"
    orig.sh "$zooCfg"
    sed '$a\
authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider\
requireClientAuthScheme=sasl\
jaasLoginRenew=3600000' "$zooCfg.orig" > "$zooCfg"

    local zkEnv="$ZOOKEEPER_HOME/bin/zkEnv.sh"
    orig.sh "$zkEnv"
    sed "/export SERVER_JVMFLAGS=/i \\
# Kerberos Configuration Addition\\
JAAS=\$ZOOCFGDIR/zoo-server.jaas\\
if [ -f \"\$JAAS\" ]; then\\
    SERVER_JVMFLAGS=\"-Djava.security.auth.login.config=\$JAAS \$SERVER_JVMFLAGS\"\\
fi" "$zkEnv.orig" > "$zkEnv"
}

process_site_file() {
    local base="$1"
    local file
    if [ "hbase-site.xml" = "$base" ]; then
        file=$HBASE_HOME/conf/$base
    else
        file=$HADOOP_HOME/etc/hadoop/$base
    fi
    orig.sh "$file"
    case $base in
    "core-site.xml")
        sed "/<\/configuration>/i \\
    <property>\\
        <name>hadoop.security.authentication</name>\\
        <value>kerberos</value>\\
    </property>\\
    <property>\\
        <name>hadoop.security.authorization</name>\\
        <value>true</value>\\
    </property>\\
    <property>\\
        <name>hadoop.http.authentication.kerberos.principal</name>\\
        <value>HTTP/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>hadoop.http.authentication.kerberos.keytab</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>hadoop.security.auth_to_local</name>\\
        <value>\\
            RULE:[2:\$1/\$2@\$0](hdfs\\\/.*@EXAMPLE\\\.COM)s/.*/$USER/\\
            RULE:[2:\$1/\$2@\$0](yarn\\\/.*@EXAMPLE\\\.COM)s/.*/$USER/\\
            RULE:[2:\$1/\$2@\$0](mapred\\\/.*@EXAMPLE\\\.COM)s/.*/$USER/\\
            RULE:[2:\$1/\$2@\$0](hbase\\\/.*@EXAMPLE\\\.COM)s/.*/$USER/\\
            DEFAULT\\
        </value>\\
    </property>" \
    "${file}.orig" > "${file}"
        ;;
    "hdfs-site.xml")
        sed "/<\/configuration>/i \\
    <property>\\
        <name>dfs.block.access.token.enable</name>\\
        <value>true</value>\\
    </property>\\
    <property>\\
        <name>dfs.namenode.kerberos.principal</name>\\
        <value>hdfs/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>dfs.namenode.keytab.file</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>dfs.secondary.namenode.kerberos.principal</name>\\
        <value>hdfs/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>dfs.secondary.namenode.keytab.file</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>dfs.datanode.kerberos.principal</name>\\
        <value>hdfs/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>dfs.datanode.keytab.file</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>dfs.http.policy</name>\\
        <value>HTTPS_ONLY</value>\\
    </property>\\
    <property>\\
        <name>dfs.data.transfer.protection</name>\\
        <value>authentication</value>\\
    </property>\\
    <property>\\
        <name>dfs.web.authentication.kerberos.principal</name>\\
        <value>HTTP/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>dfs.web.authentication.kerberos.keytab</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>dfs.namenode.kerberos.internal.spnego.principal</name>\\
        <value>HTTP/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>dfs.secondary.namenode.kerberos.internal.spnego.principal</name>\\
        <value>HTTP/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>" \
    "${file}.orig" > "${file}"
        ;;
    "mapred-site.xml")
        sed "/<\/configuration>/i \\
    <property>\\
        <name>mapreduce.jobhistory.principal</name>\\
        <value>mapred/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>mapreduce.jobhistory.keytab</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>mapreduce.jobhistory.http.policy</name>\\
        <value>HTTPS_ONLY</value>\\
    </property>\\
    <property>\\
        <name>mapreduce.jobhistory.webapp.https.principal</name>\\
        <value>HTTP/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>mapreduce.jobhistory.webapp.https.keytab</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>" \
    "${file}.orig" > "${file}"
        ;;
    "yarn-site.xml")
        sed "/<\/configuration>/i \\
    <property>\\
        <name>yarn.resourcemanager.principal</name>\\
        <value>yarn/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>yarn.resourcemanager.keytab</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>yarn.nodemanager.principal</name>\\
        <value>yarn/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>yarn.nodemanager.keytab</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>yarn.nodemanager.webapp.https.principal</name>\\
        <value>HTTP/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>yarn.nodemanager.webapp.https.keytab</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>yarn.nodemanager.container-executor.class</name>\\
        <value>org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor</value>\\
    </property>\\
    <property>\\
        <name>yarn.nodemanager.linux-container-executor.group</name>\\
        <value>$GROUP</value>\\
    </property>" \
    "${file}.orig" > "${file}"
        ;;
    "hbase-site.xml")
        sed "/<\/configuration>/i \\
    <property>\\
        <name>hbase.security.authentication</name>\\
        <value>kerberos</value>\\
    </property>\\
    <property>\\
        <name>hbase.master.kerberos.principal</name>\\
        <value>hbase/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>hbase.master.keytab.file</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>hbase.regionserver.kerberos.principal</name>\\
        <value>hbase/$HOSTNAME@EXAMPLE.COM</value>\\
    </property>\\
    <property>\\
        <name>hbase.regionserver.keytab.file</name>\\
        <value>$KERB5_HOME/keytabs/hadoop.keytab</value>\\
    </property>\\
    <property>\\
        <name>hbase.zookeeper.property.authProvider.1</name>\\
        <value>org.apache.zookeeper.server.auth.SASLAuthenticationProvider</value>\\
    </property>\\
    <property>\\
        <name>hbase.zookeeper.property.kerberos.removeHostFromPrincipal</name>\\
        <value>true</value>\\
    </property>\\
    <property>\\
        <name>hbase.zookeeper.property.kerberos.removeRealmFromPrincipal</name>\\
        <value>true</value>\\
    </property>"\
    "${file}.orig" > "${file}"
        ;;
    esac
}

process_ssl_file() {
    local base="$1"
    local type=${base#ssl-}
    type=${type%.xml}
    local file=$HADOOP_HOME/etc/hadoop/${base}
    sed -E "
        /<name>ssl\.$type\.truststore\.location<\/name>/ {
            n
            s|<value></value>|<value>$TRUSTSTORE</value>|
        }
        /<name>ssl\.$type\.truststore\.password<\/name>/ {
            n
            s|<value></value>|<value>$PASSWORD</value>|
        }
        /<name>ssl\.$type\.keystore\.location<\/name>/ {
            n
            s|<value></value>|<value>$KEYSTORE</value>|
        }
        /<name>ssl\.$type\.keystore\.password<\/name>/ {
            n
            s|<value></value>|<value>$PASSWORD</value>|
        }
        /<name>ssl\.$type\.keystore\.keypassword<\/name>/ {
            n
            s|<value></value>|<value>$PASSWORD</value>|
        } "\
        "${file}.example" > "${file}"
}

# Step 2: Modify Hadoop
modify_hadoop() {
    echo "Modifying Hadoop..."
    if [ -z "$HADOOP_HOME" ]; then
        echo "ERROR: HADOOP_HOME environment variable is not set!"
        exit 1
    fi

    for conf in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml; do
        process_site_file "$conf"
    done
    process_ssl_file ssl-client.xml
    process_ssl_file ssl-server.xml
    file=$HADOOP_HOME/etc/hadoop/container-executor.cfg
    orig.sh "$file"
    sed -e "s|^yarn.nodemanager.linux-container-executor.group=|yarn.nodemanager.linux-container-executor.group=$GROUP |" \
        "${file}.orig" > "${file}"

    sudo chown root:$USER $HADOOP_HOME/
    sudo chown root:$USER $HADOOP_HOME/bin
    sudo chown root:$USER $HADOOP_HOME/bin/container-executor
    sudo chmod 6050 $HADOOP_HOME/bin/container-executor
    sudo chown root:$USER $HADOOP_HOME/etc/
    sudo chown root:$USER $HADOOP_HOME/etc/hadoop/
    sudo chown root:$USER $HADOOP_HOME/etc/hadoop/container-executor.cfg
    sudo chmod 400 $HADOOP_HOME/etc/hadoop/container-executor.cfg
}

# Step 3: Modify HBase
modify_hbase() {
    echo "Modifying HBase..."
    if [ -z "$HBASE_HOME" ]; then
        echo "ERROR: HBASE_HOME environment variable is not set!"
        exit 1
    fi

    cat > "$HBASE_HOME/conf/client.jaas" << EOF
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  storeKey=true
  useKeyTab=true
  useTicketCache=false
  keyTab="$KERB5_HOME/keytabs/hadoop.keytab"
  principal="hbase/$HOSTNAME@EXAMPLE.COM";
};
EOF
    cat > "$HBASE_HOME/conf/server.jaas" << EOF
Server {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  useTicketCache=false
  keyTab="$KERB5_HOME/keytabs/hadoop.keytab"
  principal="hbase/$HOSTNAME@EXAMPLE.COM";
};
EOF

    sed -i.bak "/^# export HBASE_OPTS$/a\\
export HBASE_OPTS=\"-Djava.security.auth.login.config=\${HBASE_HOME}/conf/client.jaas\"\\
export HBASE_MASTER_OPTS=\"-Djava.security.auth.login.config=\${HBASE_HOME}/conf/server.jaas\"\\
export HBASE_REGIONSERVER_OPTS=\"-Djava.security.auth.login.config=\${HBASE_HOME}/conf/server.jaas\""\
    "$HBASE_HOME/conf/hbase-env.sh"

    process_site_file "hbase-site.xml"
}

# Main function to execute all steps
main() {
    if [ -z "$JAVA_HOME" ]; then
        echo "ERROR: JAVA_HOME environment variable is not set!"
        exit 1
    fi

    if [ -z "$KERB5_HOME" ]; then
        echo "ERROR: KERB5_HOME environment variable is not set!"
        exit 1
    fi

    TRUSTSTORE="$KERB5_HOME/ca/truststore"
    KEYSTORE="$KERB5_HOME/ca/keystore"
    PASSWORD="maXiaoc1"
    GROUP=$(id -gn)

    modify_zookeeper
    modify_hadoop
    modify_hbase
}

# Run main function
main
