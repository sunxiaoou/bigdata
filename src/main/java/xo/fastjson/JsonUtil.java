package xo.fastjson;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
//import xo.protobuf.ProtoBuf;

import java.util.*;
import java.util.stream.Collectors;

public class JsonUtil {
    @Data
    private static class Key4Json {
        private String encodedRegionName;
        private String tableName;
        private Long sequenceId;
        private Date writeTime;
        private List<UUID> clusterIds;

        Key4Json() {}

        Key4Json(WALKey key) {
            this.encodedRegionName = Bytes.toString(key.getEncodedRegionName());
            this.tableName = String.valueOf(key.getTableName());
            this.sequenceId = key.getSequenceId();
            this.writeTime = new Date(key.getWriteTime());
            this.clusterIds = ((WALKeyImpl) key).getClusterIds();
        }

        WALKey getKey() {
            return new WALKeyImpl(Bytes.toBytes(encodedRegionName), TableName.valueOf(tableName),
                    sequenceId, writeTime.getTime(), clusterIds.get(0));
        }
    }

    @Data
    private static class Cell4Json {
        private String row;
        private String family;
        private String qualifier;
        private Date timestamp;
        private String type;
        private byte[] value;

        private byte[] extractBytes(byte[] arr, int offset, int length) {
            return Arrays.copyOfRange(arr, offset, offset + length);
        }

        Cell4Json() {}

        Cell4Json(Cell cell) {
            this.row = new String(extractBytes(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
            this.family = new String(extractBytes(cell.getFamilyArray(), cell.getFamilyOffset(),
                    cell.getFamilyLength()));
            this.qualifier = new String(extractBytes(cell.getQualifierArray(), cell.getQualifierOffset(),
                    cell.getQualifierLength()));
            this.type = cell.getType().toString();
            this.timestamp = new Date(cell.getTimestamp());
            this.value = extractBytes(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        }

        Cell getCell() {
            return new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    timestamp.getTime(), KeyValue.Type.valueOf(type), value);
        }
    }

    @Data
    private static class Edit4Json {
        private boolean replay;
        private List<Cell4Json> cells;
        private Set<String> families;

        Edit4Json() {}

        Edit4Json(WALEdit edit) {
            this.replay = edit.isReplay();
            this.cells = edit.getCells().stream().map(Cell4Json::new).collect(Collectors.toList());
            this.families = edit.getFamilies().stream().map(String::new).collect(Collectors.toSet());
        }

        WALEdit getEdit() {
            WALEdit edit = new WALEdit(cells.size(), replay);
            for (Cell4Json cell4Json : cells) {
                edit.add(cell4Json.getCell());
            }
            return edit;
        }
    }

    @Data
    private static class Entry4Json {
        private Edit4Json edit;
        private Key4Json key;

        Entry4Json() {}

        Entry4Json(WAL.Entry entry) {
            this.edit = new Edit4Json(entry.getEdit());
            this.key = new Key4Json(entry.getKey());
        }

        WAL.Entry getEntry() {
            return new WAL.Entry((WALKeyImpl) key.getKey(), edit.getEdit());
        }
    }

    public static String key2Json(WALKey key) {
        Key4Json key4Json = new Key4Json(key);
//        return JSON.toJSONString(key4Json, SerializerFeature.WriteDateUseDateFormat);
        return JSON.toJSONString(key4Json);
    }

    public static WALKey json2Key(String json) {
        Key4Json key4Json = JSON.parseObject(json, Key4Json.class);
        return key4Json.getKey();
    }

    public static String cells2Json(List<Cell> cells) {
        List<Cell4Json> list = cells.stream().map(Cell4Json::new).collect(Collectors.toList());
        return JSON.toJSONString(list);
    }

    public static List<Cell> json2Cells(String json) {
        List<Cell4Json> list = JSON.parseArray(json, Cell4Json.class);
        return list.stream().map(Cell4Json::getCell).collect(Collectors.toList());
    }

    public static String edit2Json(WALEdit edit) {
        Edit4Json edit4Json = new Edit4Json(edit);
        return JSON.toJSONString(edit4Json);
    }

    public static WALEdit json2Edit(String json) {
        Edit4Json edit4Json = JSON.parseObject(json, Edit4Json.class);
        return edit4Json.getEdit();
    }

    public static String entry2Json(WAL.Entry entry) {
        Entry4Json entry4Json = new Entry4Json(entry);
        return JSON.toJSONString(entry4Json);
    }

    public static WAL.Entry json2Entry(String json) {
        Entry4Json entry4Json = JSON.parseObject(json, Entry4Json.class);
        return entry4Json.getEntry();
    }

    public static void main(String[] args) {
        long current = System.currentTimeMillis();
        WALKeyImpl key = new WALKeyImpl(Bytes.toBytes("encode_region_name"), TableName.valueOf("manga:fruit"),
                42, current, HConstants.DEFAULT_CLUSTER_ID);
        List<Cell> cells = new ArrayList<>();
        cells.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("name"),
                current, Bytes.toBytes("🍐")));
        cells.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("price"),
                current, Bytes.toBytes(115)));
        WALEdit edit = new WALEdit(cells.size(), false);
        for (Cell cell : cells) {
            edit.add(cell);
        }
        WAL.Entry entry = new WAL.Entry(key, edit);

        String json;

//        System.out.println(key);
//        json = key2Json(key);
//        System.out.println(json);
//        System.out.println(json2Key(json));

//        System.out.println(cells);
//        json = cells2Json(cells);
//        System.out.println(json);
//        System.out.println(json2Cells(json));

//        System.out.println(edit);
//        json = edit2Json(edit);
//        System.out.println(json);
//        System.out.println(ProtoBuf.compareEdit(edit, json2Edit(json)));

        System.out.println(entry);
        json = entry2Json(entry);
        System.out.println(json);
//        System.out.println(ProtoBuf.compareEntry(entry, json2Entry(json)));
    }
}