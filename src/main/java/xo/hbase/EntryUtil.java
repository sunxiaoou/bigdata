package xo.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class EntryUtil implements Serializable, Cloneable {
    public static List<WAL.Entry> getEntries(List<AdminProtos.WALEntry> entryProtos, CellScanner scanner) {
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
            list.add(new WAL.Entry(key, edit));
        }
        return list;
    }

    private static WALKey cloneKey(WALKey key) {
        return new WALKeyImpl(
                key.getEncodedRegionName(),
                key.getTableName(),
                key.getSequenceId(),
                key.getWriteTime(),
                ((WALKeyImpl) key).getClusterIds().get(0));
    }

    private static KeyValue cloneKV(KeyValue kv) {
        return new KeyValue(
                CellUtil.cloneRow(kv),
                CellUtil.cloneFamily(kv),
                CellUtil.cloneQualifier(kv),
                kv.getTimestamp(),
                KeyValue.Type.valueOf(kv.getType().toString()),
                CellUtil.cloneValue(kv));
    }

    private static WALEdit cloneEdit(WALEdit edit) {
        ArrayList<Cell> cells = edit.getCells();
        WALEdit copy = new WALEdit(cells.size(), edit.isReplay());
        for (Cell cell: cells) {
            copy.add(cloneKV((KeyValue) cell));
        }
        return copy;
    }

    public static WAL.Entry cloneEntry(WAL.Entry entry) {
        return new WAL.Entry(
                (WALKeyImpl) cloneKey(entry.getKey()),
                cloneEdit(entry.getEdit()));
    }
}