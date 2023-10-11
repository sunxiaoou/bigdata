package xo.hbase;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);
    protected final ReplicateConfig config;

    public AbstractSink(ReplicateConfig config) {
        this.config = config;
    }

    protected List<WAL.Entry> merge(List<AdminProtos.WALEntry> entryProtos, CellScanner scanner) {
        List<WAL.Entry> list = new ArrayList<>();
        for (AdminProtos.WALEntry entryProto: entryProtos) {
            WALProtos.WALKey keyProto = entryProto.getKey();
            HBaseProtos.UUID id = keyProto.getClusterIdsList().get(0);
            long sequence = keyProto.getLogSequenceNumber();
            WALKeyImpl key = new WALKeyImpl(
                    keyProto.getEncodedRegionName().toByteArray(),
                    TableName.valueOf(keyProto.getTableName().toByteArray()),
                    sequence,
                    keyProto.getWriteTime(),
                    new UUID(id.getMostSigBits(), id.getLeastSigBits()));
            int count = entryProto.getAssociatedCellCount();
            WALEdit edit = new WALEdit(count, false);
            for (int i = 0; i < count; i ++) {
                try {
                    if (!scanner.advance())
                        break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                edit.add(scanner.current());
            }
            WAL.Entry entry = new WAL.Entry(key, edit);
//            if (LOG.isDebugEnabled()) {
//                LOG.debug("entry: " + entry.toString());
//            } else {
//                LOG.info("sequenceId({})", sequence);
//            }
            list.add(entry);
        }
        return list;
    }

    public abstract void put(List<AdminProtos.WALEntry> entryProtos, CellScanner cellScanner);

    public abstract void flush();

    public abstract List<WAL.Entry> filter(List<WAL.Entry> filter);
}
