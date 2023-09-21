package xo.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class EntryFile {
    private static final Logger LOG = LoggerFactory.getLogger(EntryFile.class);

    public static void append(String filePath, EntryProto.Entry entryProto) {
        try (FileOutputStream fos = new FileOutputStream(filePath, true)) {
            CodedOutputStream output = CodedOutputStream.newInstance(fos);

            int size = entryProto.getSerializedSize();
            output.writeInt32NoTag(size);
            entryProto.writeTo(output);

            output.flush();
            fos.getFD().sync();  // Ensure the data is flushed to disk
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Pair<Integer, Integer> readAll(String filePath) {
        int entryCount = 0;
        int cellCount = 0;

        try (FileInputStream fis = new FileInputStream(filePath)) {
            CodedInputStream input = CodedInputStream.newInstance(fis);

            while (!input.isAtEnd()) {
                int size = input.readInt32();
                EntryProto.Entry entryProto = EntryProto.Entry.parseFrom(input.readRawBytes(size));
                WAL.Entry entry = ProtoBuf.proto2Entry(entryProto);
                LOG.info(entry.toString());
                entryCount ++;
                cellCount += entry.getEdit().getCells().size();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Pair<>(entryCount, cellCount);
    }

    public static void main(String[] args) {
        LOG.info(readAll("target/entry.dat").toString());
    }
}
