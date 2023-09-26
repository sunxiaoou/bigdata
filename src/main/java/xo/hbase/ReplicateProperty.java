package xo.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ReplicateProperty {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateProperty.class);
    private static final String PROPERTIES_FILE = "replicate_svr.properties";

    private static final String REPLICATE_SERVER_NAME = "replicate.server.name";
    private static final String REPLICATE_SERVER_HOST = "replicate.server.host";
    private static final String REPLICATE_SERVER_PORT = "replicate.server.port";
    private static final String REPLICATE_SERVER_QUORUM_HOST = "replicate.server.quorum.host";
    private static final String REPLICATE_SERVER_QUORUM_PORT = "replicate.server.quorum.port";
    private static final String REPLICATE_SERVER_QUORUM_PATH = "replicate.server.quorum.path";

    private static final String REPLICATE_SERVER_SINK_FACTORY = "replicate.server.sink.factory";

    private static final String SINK_FILE_NAME = "sink.file.name";
    private static final String SINK_HBASE_QUORUM_HOST = "sink.hbase.quorum.host";
    private static final String SINK_HBASE_QUORUM_PORT = "sink.hbase.quorum.port";
    private static final String SINK_HBASE_QUORUM_PATH = "sink.hbase.quorum.path";
    private static final String SINK_KAFKA_BOOTSTRAP_SERVERS = "sink.kafka.bootstrap.servers";
    private static final String SINK_KAFKA_BATCH_SIZE = "sink.kafka.batch.size";
    private static final String SINK_KAFKA_REQUEST_TIMEOUT_MS = "sink.kafka.request.timeout.ms";
    private static final String SINK_KAFKA_RETRIES = "sink.kafka.retries";
    private static final String SINK_KAFKA_RETRY_BACKOFF_MS = "sink.kafka.retry.backoff.ms";
    private static final String SINK_KAFKA_TRANSACTION_TIMEOUT_MS = "sink.kafka.transaction.timeout.ms";
    private static final String SINK_KAFKA_SECURITY_PROTOCOL = "sink.kafka.security.protocol";
    private static final String SINK_KAFKA_TOPIC_TABLE_MAP = "sink.kafka.topic-table-map";

    private final Properties properties;

    private ReplicateProperty() {
        this.properties = new Properties();
        try (InputStream inputStream = ReplicateProperty.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
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
    private static final ReplicateProperty instance = new ReplicateProperty();

    public static ReplicateProperty getInstance() {
        return instance;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getReplicateServerName() {
        return properties.getProperty(REPLICATE_SERVER_NAME);
    }

    public String getReplicateServerHost() {
        return properties.getProperty(REPLICATE_SERVER_HOST);
    }

    public int getReplicateServerPort() {
        return Integer.parseInt(properties.getProperty(REPLICATE_SERVER_PORT));
    }

    public String getReplicateServerQuorumHost() {
        return properties.getProperty(REPLICATE_SERVER_QUORUM_HOST);
    }

    public int getReplicateServerQuorumPort() {
        return Integer.parseInt(properties.getProperty(REPLICATE_SERVER_QUORUM_PORT));
    }

    public String getReplicateServerQuorumPath() {
        return properties.getProperty(REPLICATE_SERVER_QUORUM_PATH);
    }

    public String getReplicateServerSinkFactory() {
        return properties.getProperty(REPLICATE_SERVER_SINK_FACTORY);
    }

    public String getSinkFileName() {
        return properties.getProperty(SINK_FILE_NAME);
    }

    public String getSinkHBaseQuorumHost() {
        return properties.getProperty(SINK_HBASE_QUORUM_HOST);
    }

    public int getSinkHBaseQuorumPort() {
        return Integer.parseInt(properties.getProperty(SINK_HBASE_QUORUM_PORT));
    }

    public String getSinkHBaseQuorumPath() {
        return properties.getProperty(SINK_HBASE_QUORUM_PATH);
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

    public String getSinkKafkaTopicTableMap() {
        return properties.getProperty(SINK_KAFKA_TOPIC_TABLE_MAP);
    }


    public static void main(String[] args) {
        ReplicateProperty config = ReplicateProperty.getInstance();
//        System.out.println(config.properties);

        System.out.println("Server Name: " + config.getReplicateServerName());
        System.out.println("Server Host: " + config.getReplicateServerHost());
        System.out.println("Server Port: " + config.getReplicateServerPort());
    }
}