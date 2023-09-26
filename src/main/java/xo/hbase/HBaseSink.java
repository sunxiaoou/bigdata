package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.wal.WAL;

import java.io.IOException;
import java.util.List;

public class HBaseSink extends AbstractSink {
    private final ReplicationSink sink;

    public HBaseSink(ReplicateConfig config) throws IOException {
        super(config);
        Configuration conf = HBaseConfiguration.create();
        // resources/hbase-site.xml can be used alternatively
        conf.set(HConstants.ZOOKEEPER_QUORUM, config.getSinkHBaseQuorumHost());
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(config.getSinkHBaseQuorumPort()));
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, config.getSinkHBaseQuorumPath());
        this.sink = new ReplicationSink(conf, null);
    }

    @Override
    public void put(List<AdminProtos.WALEntry> entries, CellScanner cellScanner) {
        try {
            sink.replicateEntries(entries,
                    cellScanner,
                    null,
                    null,
                    null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
    }

    @Override
    public List<WAL.Entry> filter(List<WAL.Entry> filter) {
        return null;
    }
}
