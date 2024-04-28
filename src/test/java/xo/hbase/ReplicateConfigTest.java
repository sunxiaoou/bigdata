package xo.hbase;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReplicateConfigTest {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateConfigTest.class);

    ReplicateConfig config = ReplicateConfig.getInstance();

    @Test
    public void test() {
        LOG.info("Server Name: " + config.getReplicateServerName());
        LOG.info("Server Host: " + config.getReplicateServerHost());
        LOG.info("Tables Map Type: " + config.getSourceHBaseMapType());
        LOG.info("Server HBase Host: " + config.getSourceHBaseQuorumHost().split(",")[0]);
        LOG.info("Tables Map: " + config.getSourceHBaseMapTables());
        LOG.info("Namespaces Map: " + config.getSourceHBaseMapNamespaces());
        LOG.info("Replicate Sink: " + config.getReplicateServerSink());
        LOG.info("Topic Table Map: " + config.getSinkKafkaTopicTableMap());
    }
}