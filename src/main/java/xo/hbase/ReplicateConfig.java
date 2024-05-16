package xo.hbase;

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ReplicateConfig {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateConfig.class);
    private static final String PROPERTIES_FILE = "replicate.properties";

    private static final String REPLICATE_SERVER_NAME = "replicate.server.name";
    private static final String REPLICATE_SERVER_HOST = "replicate.server.host";
    private static final String REPLICATE_SERVER_PEER = "replicate.server.peer";
    private static final String REPLICATE_SERVER_RPCSVR_ZNODE = "replicate.server.rpcSvr.zNode";
    private static final String REPLICATE_SERVER_QUORUM_HOST = "replicate.server.quorum.host";
    private static final String REPLICATE_SERVER_QUORUM_PORT = "replicate.server.quorum.port";
    private static final String REPLICATE_SERVER_SINK = "replicate.server.sink";

    private static final String SOURCE_HBASE_CONFPATH = "source.hbase.confPath";
    private static final String SOURCE_HBASE_QUORUM_HOST = "source.hbase.quorum.host";
    private static final String SOURCE_HBASE_QUORUM_PORT = "source.hbase.quorum.port";
    private static final String SOURCE_HBASE_QUORUM_PATH = "source.hbase.quorum.path";
    private static final String SOURCE_HBASE_MAP_TYPE = "source.hbase.map.type";
    private static final String SOURCE_HBASE_MAP_NAMESPACES = "source.hbase.map.namespaces";
    private static final String SOURCE_HBASE_MAP_TABLES = "source.hbase.map.tables";

    private static final String SINK_FILE_NAME = "sink.file.name";
    private static final String SINK_FILE_CAPACITY = "sink.file.capacity";
    private static final String SINK_FILE_NUMBER = "sink.file.number";

    private static final String TARGET_HADOOP_USER = "target.hadoop.user";
    private static final String TARGET_HADOOP_HDFS_HOST = "target.hadoop.hdfs.host";
    private static final String TARGET_HADOOP_HDFS_PORT = "target.hadoop.hdfs.port";
    private static final String TARGET_HBASE_QUORUM_HOST = "target.hbase.quorum.host";
    private static final String TARGET_HBASE_QUORUM_PORT = "target.hbase.quorum.port";
    private static final String TARGET_HBASE_QUORUM_PATH = "target.hbase.quorum.path";

    private static final String SINK_KAFKA_BOOTSTRAP_SERVERS = "sink.kafka.bootstrap.servers";
    private static final String SINK_KAFKA_BATCH_SIZE = "sink.kafka.batch.size";
    private static final String SINK_KAFKA_REQUEST_TIMEOUT_MS = "sink.kafka.request.timeout.ms";
    private static final String SINK_KAFKA_RETRIES = "sink.kafka.retries";
    private static final String SINK_KAFKA_RETRY_BACKOFF_MS = "sink.kafka.retry.backoff.ms";
    private static final String SINK_KAFKA_TRANSACTION_TIMEOUT_MS = "sink.kafka.transaction.timeout.ms";
    private static final String SINK_KAFKA_SECURITY_PROTOCOL = "sink.kafka.security.protocol";
    private static final String SINK_KAFKA_TOPIC_TABLE_MAP = "sink.kafka.topic.table.map";
    private static final String SINK_KAFKA_SERIALIZER = "sink.kafka.serializer";

    // kafka consumer
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    private static final String KAFKA_GROUP_ID = "kafka.group.id";
    private static final String KAFKA_ENABLE_AUTO_COMMIT = "kafka.enable.auto.commit";
    private static final String KAFKA_AUTO_COMMIT_INTERVAL_MS = "kafka.auto.commit.interval.ms";
    private static final String KAFKA_TOPICS = "kafka.topics";

    private final Properties properties;

    private ReplicateConfig() {
        LOG.info(System.getProperty("user.dir"));
        String Path = System.getProperty("replicate.properties.file");
        this.properties = new Properties();
        try (InputStream inputStream = ReplicateConfig.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                throw new IOException("Unable to load the properties file: " + PROPERTIES_FILE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Singleton instance
    private static final ReplicateConfig instance = new ReplicateConfig();

    public static ReplicateConfig getInstance() {
        return instance;
    }

    public String getReplicateServerName() {
        return properties.getProperty(REPLICATE_SERVER_NAME);
    }

    public String getReplicateServerHost() {
        return properties.getProperty(REPLICATE_SERVER_HOST);
    }

    public String getReplicateServerQuorumHost() {
        return properties.getProperty(REPLICATE_SERVER_QUORUM_HOST);
    }

    public int getReplicateServerQuorumPort() {
        return Integer.parseInt(properties.getProperty(REPLICATE_SERVER_QUORUM_PORT));
    }

    public String getReplicateServerPeer() {
        return properties.getProperty(REPLICATE_SERVER_PEER);
    }

    public String getReplicateServerRpcSvrZNode() {
        return properties.getProperty(REPLICATE_SERVER_RPCSVR_ZNODE);
    }

    public String getReplicateServerSink() {
        return properties.getProperty(REPLICATE_SERVER_SINK);
    }

    public String getTargetHadoopUser() {
        return properties.getProperty(TARGET_HADOOP_USER);
    }

    public String getTargetHadoopHdfsHost() {
        return properties.getProperty(TARGET_HADOOP_HDFS_HOST);
    }

    public int getTargetHadoopHdfsPort() {
        return Integer.parseInt(properties.getProperty(TARGET_HADOOP_HDFS_PORT));
    }

    public String getSourceHBaseConfPath() {
        return properties.getProperty(SOURCE_HBASE_CONFPATH);
    }

    public String getSourceHBaseQuorumHost() {
        return properties.getProperty(SOURCE_HBASE_QUORUM_HOST);
    }

    public int getSourceHBaseQuorumPort() {
        return Integer.parseInt(properties.getProperty(SOURCE_HBASE_QUORUM_PORT));
    }

    public String getSourceHBaseQuorumPath() {
        return properties.getProperty(SOURCE_HBASE_QUORUM_PATH);
    }

    public String getSourceHBaseMapType() {
        return properties.getProperty(SOURCE_HBASE_MAP_TYPE);
    }

    public Map<String, String> getMap(String property) {
        final String TABLE_MAP_DELIMITER = "=";
        Map<String, String> tableMap = new HashMap<>();
        String[] mappings = StringUtils.getStrings(properties.getProperty(property));
        for (String mapping: mappings) {
            String[] s = mapping.split(TABLE_MAP_DELIMITER);
            tableMap.put(s[0], s[1]);
        }
        return tableMap;
    }

    public Map<String, String> getSourceHBaseMapNamespaces() {
        return getMap(SOURCE_HBASE_MAP_NAMESPACES);
    }

    public Map<String, String> getSourceHBaseMapTables() {
        return getMap(SOURCE_HBASE_MAP_TABLES);
    }

    public String getSinkFileName() {
        return properties.getProperty(SINK_FILE_NAME);
    }

    public int getSinkFileCapacity() {
        return Integer.parseInt(properties.getProperty(SINK_FILE_CAPACITY));
    }

    public short getSinkFileNumber() {
        return Short.parseShort(properties.getProperty(SINK_FILE_NUMBER));
    }

    public String getTargetHBaseQuorumHost() {
        return properties.getProperty(TARGET_HBASE_QUORUM_HOST);
    }

    public int getTargetHBaseQuorumPort() {
        return Integer.parseInt(properties.getProperty(TARGET_HBASE_QUORUM_PORT));
    }

    public String getTargetHBaseQuorumPath() {
        return properties.getProperty(TARGET_HBASE_QUORUM_PATH);
    }

    public String getSinkKafkaBootstrapServers() {
        return properties.getProperty(SINK_KAFKA_BOOTSTRAP_SERVERS);
    }

    public int getSinkKafkaBatchSize() {
        return Integer.parseInt(properties.getProperty(SINK_KAFKA_BATCH_SIZE));
    }

    public int getSinkKafkaRequestTimeoutMs() {
        return Integer.parseInt(properties.getProperty(SINK_KAFKA_REQUEST_TIMEOUT_MS));
    }

    public int getSinkKafkaRetries() {
        return Integer.parseInt(properties.getProperty(SINK_KAFKA_RETRIES));
    }

    public int getSinkKafkaRetryBackoffMs() {
        return Integer.parseInt(properties.getProperty(SINK_KAFKA_RETRY_BACKOFF_MS));
    }

    public int getSinkKafkaTransactionTimeoutMs() {
        return Integer.parseInt(properties.getProperty(SINK_KAFKA_TRANSACTION_TIMEOUT_MS));
    }

    public String getSinkKafkaSecurityProtocol() {
        return properties.getProperty(SINK_KAFKA_SECURITY_PROTOCOL);
    }

    public Map<String, String> getSinkKafkaTopicTableMap() {
        return getMap(SINK_KAFKA_TOPIC_TABLE_MAP);
    }

    public String getSinkKafkaSerializer() {
        return properties.getProperty(SINK_KAFKA_SERIALIZER, "protobuf");
    }

    public String getKafkaBootstrapServers() {
        return properties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
    }

    public String getKafkaGroupId() {
        return properties.getProperty(KAFKA_GROUP_ID);
    }

    public boolean getKafkaEnableAutoCommit() {
        return Boolean.parseBoolean(properties.getProperty(KAFKA_ENABLE_AUTO_COMMIT));
    }

    public int getKafkaAutoCommitIntervalMs() {
        return Integer.parseInt(properties.getProperty(KAFKA_AUTO_COMMIT_INTERVAL_MS));
    }

    public String getKafkaTopics() {
        return properties.getProperty(KAFKA_TOPICS);
    }
}