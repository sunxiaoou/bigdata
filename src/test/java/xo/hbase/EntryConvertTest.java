package xo.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
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

import static org.junit.Assert.*;

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

    @Test
    public void testToHBasePut() {
        OneRowChange orc = new OneRowChange();
        orc.setSchemaName("manga");
        orc.setTableName("fruit");
        orc.setAction(OneRowChange.ActionType.INSERTONDUP);
        orc.setTableId(-1);

        ArrayList<OneRowChange.ColumnSpec> specs = new ArrayList<>();
        specs.add(orc.new ColumnSpec("ROW", 1, 0, 0, null, false));
        specs.add(orc.new ColumnSpec("cf.name", 2, java.sql.Types.VARBINARY, 0, null, false));
        specs.add(orc.new ColumnSpec("cf.price", 3, java.sql.Types.VARBINARY, 0, null, false));
        orc.setColumnSpec(specs);

        ArrayList<OneRowChange.ColumnVal> vals = new ArrayList<>();
        OneRowChange.ColumnVal val = orc.new ColumnVal();
        val.setValue("107"); // ROW
        vals.add(val);
        val = orc.new ColumnVal();
        val.setValue("🍐"); // cf.name
        vals.add(val);
        val = orc.new ColumnVal();
        val.setValue("115"); // cf.price
        vals.add(val);
        ArrayList<ArrayList<OneRowChange.ColumnVal>> colVals = new ArrayList<>();
        colVals.add(vals);
        orc.setColumnValues(colVals);
        Put put = ChangeUtil.toHBasePut(orc);
        assertArrayEquals("107".getBytes(), put.getRow());
        assertTrue(put.has("cf".getBytes(), "name".getBytes(), "🍐".getBytes()));
        assertTrue(put.has("cf".getBytes(), "price".getBytes(), "115".getBytes()));
    }
}
