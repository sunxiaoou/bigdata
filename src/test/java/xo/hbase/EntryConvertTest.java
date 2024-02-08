package xo.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.fastjson.JsonUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class EntryConvertTest {
    private static final Logger LOG = LoggerFactory.getLogger(EntryConvertTest.class);
    private static WALKeyImpl key;
    private static WALEdit edit;
    private static WAL.Entry entry;
    private static String json;


    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        long current = System.currentTimeMillis();
        key = new WALKeyImpl(Bytes.toBytes("encode_region_name"), TableName.valueOf("manga:fruit"),
                42, current, HConstants.DEFAULT_CLUSTER_ID);
        List<Cell> cells = new ArrayList<>();
        cells.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("name"),
                current, Bytes.toBytes("🍐")));
        cells.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("price"),
                current, Bytes.toBytes(115)));
        edit = new WALEdit(cells.size(), false);
        for (Cell cell : cells) {
            edit.add(cell);
        }
        entry = new WAL.Entry(key, edit);
    }

    @Test
    public void key2Json() {
        System.out.println(key);
        json = JsonUtil.key2Json(key);
        System.out.println(json);
        assertEquals(JsonUtil.json2Key(json), key);
    }

    @Test
    public void edit2Json() {
        System.out.println(edit);
        json = JsonUtil.edit2Json(edit);
        System.out.println(json);
        WALEdit edit2 = JsonUtil.json2Edit(json);
        assertEquals(edit2.getCells(), edit.getCells());
        assertEquals(edit2.isReplay(), edit.isReplay());
        assertEquals(edit2.getFamilies(), edit.getFamilies());
    }

    private void compare(WAL.Entry entry2) {
        assertEquals(entry2.getKey(), entry.getKey());
        assertEquals(entry2.getEdit().isReplay(), entry.getEdit().isReplay());
        assertEquals(entry2.getEdit().getCells(), entry.getEdit().getCells());
        assertEquals(entry2.getEdit().getFamilies(), entry.getEdit().getFamilies());
    }

    @Test
    public void entry2Json() {
        System.out.println(entry);
        json = JsonUtil.entry2Json(entry);
        System.out.println(json);
        WAL.Entry entry2 = JsonUtil.json2Entry(json);
        compare(entry2);
    }

    @Test
    public void entryClone() {
        System.out.println(entry);
        WAL.Entry entry2 = EntryUtil.cloneEntry(entry);
        assertEquals(entry2.getKey(), entry.getKey());
        System.out.println(entry2);
        compare(entry2);
    }

    @Test
    public void entry2Change() {
        OneRowChange change = ChangeUtil.entry2OneRowChange(entry);
        System.out.println(change);
    }
}
