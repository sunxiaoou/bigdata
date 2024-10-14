package xo.hbase;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
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
    public boolean put(HBaseData hBaseData) {
        LOG.info("replicationClusterId({})", hBaseData.getReplicationClusterId());
        LOG.info("sourceBaseNamespaceDirPath({})", hBaseData.getSourceBaseNamespaceDirPath());
        LOG.info("sourceHFileArchiveDirPath({})", hBaseData.getSourceHFileArchiveDirPath());
        CellScanner cellScanner = CellUtil.createCellScanner(hBaseData.getCells().iterator());
        for (WAL.Entry entry: AbstractSink.merge(hBaseData.getEntryProtos(), cellScanner)) {
            LOG.info(entry.toString());
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
