package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.wal.WAL;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class HBaseSink extends AbstractSink {
    final private String SINK_HBASE_QUORUM_HOST = "sink.hbase.quorum.host";
    final private String SINK_HBASE_QUORUM_PORT = "sink.hbase.quorum.port";
    final private String SINK_HBASE_QUORUM_PATH = "sink.hbase.quorum.path";

    ReplicationSink sink;

    public HBaseSink(Properties properties) throws IOException {
        super(properties);
        Configuration conf = HBaseConfiguration.create();
        // resources/hbase-site.xml can be used alternatively
        conf.set("hbase.zookeeper.quorum", getProperties().getProperty(SINK_HBASE_QUORUM_HOST));
        conf.set("hbase.zookeeper.property.clientPort", getProperties().getProperty(SINK_HBASE_QUORUM_PORT));
        conf.set("zookeeper.znode.parent", getProperties().getProperty(SINK_HBASE_QUORUM_PATH));
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
