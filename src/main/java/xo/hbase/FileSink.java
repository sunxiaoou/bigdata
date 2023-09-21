package xo.hbase;

import xo.protobuf.EntryFile;
import xo.protobuf.EntryProto;
import xo.protobuf.ProtoBuf;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.wal.WAL;

import java.util.List;
import java.util.Properties;

public class FileSink extends AbstractSink {
    final private String SINK_FILE_NAME = "sink.file.name";

    public FileSink(Properties properties) {
        super(properties);
    }

    @Override
    public void put(List<AdminProtos.WALEntry> entryProtos, CellScanner cellScanner) {
        List<WAL.Entry> entries = merge(entryProtos, cellScanner);
        String filePath = getProperties().getProperty(SINK_FILE_NAME);
        for (WAL.Entry entry: entries) {
            EntryProto.Entry entryProto = ProtoBuf.entry2Proto(entry);
            EntryFile.append(filePath, entryProto);
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
