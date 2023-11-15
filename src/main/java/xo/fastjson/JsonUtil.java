package xo.fastjson;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.Data;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;

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
    }

    @Data
    private static class Edit4Json {
        private boolean replay;
        private List<Cell4Json> cells;
        private Set<String> families;

        Edit4Json(WALEdit edit) {
            this.replay = edit.isReplay();
            this.cells = edit.getCells().stream().map(Cell4Json::new).collect(Collectors.toList());
            this.families = edit.getFamilies().stream().map(String::new).collect(Collectors.toSet());
        }
    }

    @Data
    private static class Entry4Json {
        private Edit4Json edit;
        private Key4Json key;

        Entry4Json(WAL.Entry entry) {
            this.edit = new Edit4Json(entry.getEdit());
            this.key = new Key4Json(entry.getKey());
        }
    }

    public static String key2Json(WALKey key) {
        Key4Json key4Json = new Key4Json(key);
        return JSON.toJSONString(key4Json, SerializerFeature.WriteDateUseDateFormat);
    }

    public static WALKey json2Key(String json) {
        Key4Json key4Json = JSON.parseObject(json, JsonUtil.Key4Json.class);
        return key4Json.getKey();
    }

    public static String cells2Json(List<Cell> cells) {
        List<Cell4Json> list = cells.stream().map(Cell4Json::new).collect(Collectors.toList());
        return JSON.toJSONString(list, SerializerFeature.WriteDateUseDateFormat);
    }

    public static String edit2Json(WALEdit edit) {
        Edit4Json edit4Json = new Edit4Json(edit);
        return JSON.toJSONString(edit4Json, SerializerFeature.WriteDateUseDateFormat);
    }

    public static String entry2Json(WAL.Entry entry) {
        Entry4Json entry4Json = new Entry4Json(entry);
        return JSON.toJSONString(entry4Json, SerializerFeature.WriteDateUseDateFormat);
    }

    public static void main(String[] args) {
        WALKeyImpl key = new WALKeyImpl(Bytes.toBytes("encode_region_name"), TableName.valueOf("manga:fruit"),
                42, System.currentTimeMillis(), HConstants.DEFAULT_CLUSTER_ID);
        List<Cell> cells = new ArrayList<>();
        cells.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("name"),
                Bytes.toBytes("🍐")));
        cells.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("price"),
                Bytes.toBytes(115)));
        WALEdit edit = new WALEdit(cells.size(), false);
        for (Cell cell : cells) {
            edit.add(cell);
        }
        WAL.Entry entry = new WAL.Entry(key, edit);

        System.out.println(key);
        String json = key2Json(key);
        System.out.println(json);
        System.out.println(json2Key(json));

//        System.out.println(cells2Json(cells));
//        System.out.println(edit2Json(edit));
        System.out.println(entry);
//        System.out.println(entry2Json(entry));
    }
}
