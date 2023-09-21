package xo.hbase;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public abstract class AbstractSink {
    private final Properties properties;

    public AbstractSink(Properties properties) {
        this.properties = properties;
    }

    public Properties getProperties() {
        return properties;
    }

    protected List<WAL.Entry> merge(List<AdminProtos.WALEntry> entries, CellScanner scanner) {
        List<WAL.Entry> list = new ArrayList<>();
        for (AdminProtos.WALEntry entry: entries) {
            WALProtos.WALKey walKey = entry.getKey();
            HBaseProtos.UUID id = walKey.getClusterIdsList().get(0);
            WALKeyImpl key = new WALKeyImpl(
                    walKey.getEncodedRegionName().toByteArray(),
                    TableName.valueOf(walKey.getTableName().toByteArray()),
                    walKey.getLogSequenceNumber(),
                    walKey.getWriteTime(),
                    new UUID(id.getMostSigBits(), id.getLeastSigBits()));
            int count = entry.getAssociatedCellCount();
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
            list.add(new WAL.Entry(key, edit));
        }
        return list;
    }

    public abstract void put(List<AdminProtos.WALEntry> entries, CellScanner cellScanner);

    public abstract void flush();

    public abstract List<WAL.Entry> filter(List<WAL.Entry> filter);
}
