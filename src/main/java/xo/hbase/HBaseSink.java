package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HBaseSink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);
    private final ReplicationSink sink;

    public HBaseSink(ReplicateConfig config) throws IOException {
        super(config);
        Configuration conf = HBaseConfiguration.create();
        // resources/hbase-site.xml can be used alternatively
        conf.set("fs.defaultFS",
                String.format("hdfs://%s:%s", config.getTargetHadoopHdfsHost(), config.getTargetHadoopHdfsPort()));
        conf.set(HConstants.ZOOKEEPER_QUORUM, config.getTargetHBaseQuorumHost());
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(config.getTargetHBaseQuorumPort()));
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, config.getTargetHBaseQuorumPath());
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
