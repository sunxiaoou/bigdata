package xo.hbase;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DummySink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(DummySink.class);

    public DummySink(ReplicateConfig config) throws IOException {
        super(config);
    }

    @Override
    public boolean put(List<AdminProtos.WALEntry> entryProtos,
                       CellScanner cellScanner,
                       String replicationClusterId,
                       String sourceBaseNamespaceDirPath,
                       String sourceHFileArchiveDirPath) {
        for (WAL.Entry entry: AbstractSink.merge(entryProtos, cellScanner)) {
            LOG.info(entry.toString());
            LOG.info("replicationClusterId({})", replicationClusterId);
            LOG.info("sourceBaseNamespaceDirPath({})", sourceBaseNamespaceDirPath);
            LOG.info("sourceHFileArchiveDirPath({})", sourceHFileArchiveDirPath);
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
