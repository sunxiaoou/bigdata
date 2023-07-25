package xo.protobuf;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ProtoBuf {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoBuf.class);

    public static boolean testKV() {
        KeyValue kv1 = new KeyValue(
                        Bytes.toBytes("aaa"),
                        Bytes.toBytes("f1"),
                        Bytes.toBytes("q1"),
                        new byte[30]);
        CellProtos.Cell cellProto = ProtobufUtil.toCell(kv1, false);
        ByteString row = cellProto.getRow();
        Cell cell = ProtobufUtil
                .toCell(ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY), cellProto, false);
        return CellComparatorImpl.COMPARATOR.compare(kv1, cell) == 0;
    }

    public static EntryProto.Key key2Proto(WALKey key) {
        return EntryProto.Key.newBuilder()
                .setEncodedRegionName(com.google.protobuf.ByteString.copyFrom(key.getEncodedRegionName()))
                .setTablename(com.google.protobuf.ByteString.copyFrom(key.getTableName().toBytes()))
                .setSequenceId(key.getSequenceId())
                .setWriteTime(key.getWriteTime()).build();
    }

    public static WALKey proto2Key(EntryProto.Key keyProto) {
        return new WALKeyImpl(
                keyProto.getEncodedRegionName().toByteArray(),
                TableName.valueOf(keyProto.getTablename().toByteArray()),
                keyProto.getSequenceId(),
                keyProto.getWriteTime(),
                null);
    }

    public static EntryProto.KeyValue keyValue2Proto(KeyValue kv) {
        return EntryProto.KeyValue.newBuilder()
                .setRow(com.google.protobuf.ByteString
                        .copyFrom(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()))
                .setFamily(com.google.protobuf.ByteString
                        .copyFrom(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength()))
                .setQualifier(com.google.protobuf.ByteString
                        .copyFrom(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()))
                .setTimestamp(kv.getTimestamp())
                .setKeyType(EntryProto.CellType.valueOf(kv.getTypeByte()))
                .setValue(com.google.protobuf.ByteString
                        .copyFrom(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()))
                .build();
    }

    public static KeyValue proto2KeyValue(EntryProto.KeyValue kvProto) {
        return new KeyValue(
                kvProto.getRow().toByteArray(),
                kvProto.getFamily().toByteArray(),
                kvProto.getQualifier().toByteArray(),
                kvProto.getTimestamp(),
                KeyValue.Type.codeToType((byte) kvProto.getKeyType().getNumber()),
                kvProto.getValue().toByteArray());
    }

    public static WALEdit createEdit(List<KeyValue> keyValues, boolean replay) {
        WALEdit edit = new WALEdit(keyValues.size(), replay);
        for (KeyValue keyValue : keyValues) {
            edit.add(keyValue);
        }
        return edit;
    }

    public static boolean compareEdit(WALEdit edit, WALEdit edit2) {
        return edit.getCells().equals(edit2.getCells()) && edit.isReplay() == edit2.isReplay();
    }

    public static EntryProto.Edit edit2Proto(WALEdit edit) {
        EntryProto.Edit.Builder builder = EntryProto.Edit.newBuilder();
        builder.setReplay(edit.isReplay());
        for (Cell cell: edit.getCells()) {
            builder.addCells(keyValue2Proto((KeyValue) cell));
        }
        return builder.build();
    }

    public static WALEdit proto2Edit(EntryProto.Edit editProto) {
        List<KeyValue> keyValues = new ArrayList<>();
        for (EntryProto.KeyValue kvProto: editProto.getCellsList()) {
            keyValues.add(proto2KeyValue(kvProto));
        }
        return createEdit(keyValues, editProto.getReplay());
    }

    public static boolean compareEntry(WAL.Entry entry, WAL.Entry entry2) {
        return entry.getKey().equals(entry2.getKey()) && compareEdit(entry.getEdit(), entry2.getEdit());
    }

    public static EntryProto.Entry entry2Proto(WAL.Entry entry) {
        return EntryProto.Entry.newBuilder()
                .setKey(key2Proto(entry.getKey()))
                .setEdit(edit2Proto(entry.getEdit()))
                .build();
    }

    public static WAL.Entry proto2Entry(EntryProto.Entry entryProto) {
        return new WAL.Entry((WALKeyImpl) proto2Key(entryProto.getKey()),
                proto2Edit((entryProto.getEdit())));
    }

    public static boolean testKey() {
        WALKey key = new WALKeyImpl(Bytes.toBytes("encode_region_name"), TableName.valueOf("manga:fruit"),
                42, System.currentTimeMillis(), null);
        LOG.info(key.toString());
        EntryProto.Key keyProto = key2Proto(key);
        WALKey key2 = proto2Key(keyProto);
        LOG.info(key2.toString());
        return key2.equals(key);
    }

    public static boolean testKeyValue() {
        KeyValue kv = new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("name"),
                System.currentTimeMillis(), KeyValue.Type.Put, new byte[30]);
        LOG.info(kv.toString());
        EntryProto.KeyValue kvProto = keyValue2Proto(kv);
        KeyValue kv2 = proto2KeyValue(kvProto);
        LOG.info(kv2.toString());
        return kv2.equals(kv);
    }

    public static boolean testEdit() {
        List<KeyValue> keyValues = new ArrayList<>();
        keyValues.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("name"),
                new byte[30]));
        keyValues.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("price"),
                new byte[30]));
        WALEdit edit = createEdit(keyValues, false);
        LOG.info(edit.toString());
        EntryProto.Edit editProto = edit2Proto(edit);
//        LOG.info(editProto.toString());
        WALEdit edit2 = proto2Edit(editProto);
        LOG.info(edit2.toString());

        return compareEdit(edit, edit2);
    }

    public static boolean testEntry() {
        WALKeyImpl key = new WALKeyImpl(Bytes.toBytes("encode_region_name"), TableName.valueOf("manga:fruit"),
                42, System.currentTimeMillis(), null);
        List<KeyValue> keyValues = new ArrayList<>();

        keyValues.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("name"),
                new byte[30]));
        keyValues.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("price"),
                new byte[30]));
        WALEdit edit = createEdit(keyValues, false);

        WAL.Entry entry = new WAL.Entry(key, edit);
        LOG.info(entry.toString());
        EntryProto.Entry entryProto = entry2Proto(entry);
        WAL.Entry entry2 = proto2Entry(entryProto);
        LOG.info(entry2.toString());

        return compareEntry(entry, entry2);
    }

    public static void main(String[] args) {
//        System.out.println(testKV());
//        System.out.println(testKey());
//        System.out.println(testKeyValue());
//        System.out.println(testEdit());
        System.out.println(testEntry());
    }
}
