package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class HBaseSink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);
    private final ReplicationSink sink;

    public HBaseSink(ReplicateConfig config) throws IOException {
        super(config);
        Configuration conf = HBaseConfiguration.create();
        // resources/hbase-site.xml can be used alternatively
        File confDir = new File(System.getProperty("user.dir"), config.getTargetHBaseConfPath());
        if (confDir.exists() && confDir.isDirectory()) {
            for (String confFile : FileUtil.list(confDir)) {
                if (new File(confDir, confFile).isFile() && confFile.endsWith(".xml")) {
                    conf.addResource(new Path(confDir.getPath(), confFile));
                }
                LOG.info("config sink with resource ({}/{})", confDir, confFile);
            }
        } else {
            String fs = String.format("hdfs://%s:%d", config.getTargetHadoopHdfsHost(),
                    config.getTargetHadoopHdfsPort());
            String hbDir = fs + "/hbase";
            conf.set("fs.defaultFS", fs);
            String zHost = config.getTargetHBaseQuorumHost();
            String zPort = String.valueOf(config.getTargetHBaseQuorumPort());
            String zNode = config.getTargetHBaseQuorumPath();
            String repConf = System.getProperty("user.dir");
            conf.set(HConstants.HBASE_DIR, hbDir);
            conf.set(HConstants.ZOOKEEPER_QUORUM, zHost);
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, zPort);
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zNode);
            conf.set(HConstants.REPLICATION_CONF_DIR, repConf);     // can omitted
            LOG.info("config sink with hbDir({}) zk({}:{}:{}) repConf({})", hbDir, zHost, zPort, zNode,
                    repConf);
        }
        this.sink = new ReplicationSink(conf, null);
    }

    @Override
    public boolean put(HBaseData hBaseData) {
        List<AdminProtos.WALEntry> entryProtos = hBaseData.getEntryProtos();
        CellScanner cellScanner = CellUtil.createCellScanner(hBaseData.getCells().iterator());
        try {
            sink.replicateEntries(entryProtos,
                    cellScanner,
                    hBaseData.getReplicationClusterId(),
                    hBaseData.getSourceBaseNamespaceDirPath(),
                    hBaseData.getSourceHFileArchiveDirPath());
            LOG.info("put {} entryProto(s) already", entryProtos.size());
        } catch (IOException e) {
            LOG.error("Failed to replicate entryProto(s) - {}", e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public void flush() {
    }

    @Override
    public List<WAL.Entry> filter(List<WAL.Entry> filter) {
        return null;
    }
}
