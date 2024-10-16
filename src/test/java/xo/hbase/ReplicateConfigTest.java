package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


public class ReplicateConfigTest {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateConfigTest.class);

    ReplicateConfig config = ReplicateConfig.getInstance();

    @Test
    public void test() {
        LOG.info("Server Name: " + config.getReplicateServerName());
        LOG.info("Server Host: " + config.getReplicateServerHost());
        LOG.info("Tables Map Type: " + config.getSourceHBaseMapType());
        LOG.info("Source HBase confPath: " + config.getSourceHBaseConfPath());
        LOG.info("Source HBase Host: " + config.getSourceHBaseQuorumHost().split(",")[0]);
        LOG.info("Tables Map: " + config.getSourceHBaseMapTables());
        LOG.info("Namespaces Map: " + config.getSourceHBaseMapNamespaces());
        LOG.info("Replicate Sink: " + config.getReplicateServerSink());
        LOG.info("Target HBase confPath: " + config.getTargetHBaseConfPath());
        LOG.info("Topic Table Map: " + config.getSinkKafkaTopicTableMap());
    }

    @Test
    public void addResources() throws IOException {
        File confDir = new File(System.getProperty("user.dir") + "/src/main/resources/"
                + config.getSourceHBaseConfPath());
        assert confDir.exists() && confDir.isDirectory();
        Configuration conf = HBaseConfiguration.create();
        for (String confFile : FileUtil.list(confDir)) {
            if (new File(confDir, confFile).isFile() && confFile.endsWith(".xml")) {
                conf.addResource(new Path(confDir.getPath(), confFile));
            }
            LOG.info("addResource({}/{})", confDir, confFile);
        }
    }
}