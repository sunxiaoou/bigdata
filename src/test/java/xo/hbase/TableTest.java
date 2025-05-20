package xo.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class TableTest {
    private static final Logger LOG = LoggerFactory.getLogger(TableTest.class);

//    private static final String host = "hb_u";
//    private static final String host = "hb_cdh";
//    private static final String confPath = "src/main/resources/" + host;
//    private static final String zPrincipal = null;
//    private static final String principal = null;
//    private static final String keytab = null;
//    private static final boolean fallback = false;

    private static final String host = "hb_mrs";
    private static final String confPath = "src/main/resources/" + host;
    private static final String zPrincipal = "zookeeper/hadoop.hadoop.com";
    private static final String principal = "loader_hive1@HADOOP.COM";
    private static final String keytab = confPath + "/loader_hive1.keytab";
    private static final boolean fallback = true;

    private static HBase db;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
//        db = new HBase();
//        db = new HBase("node1", 2181, "/hbase");
//        db = new HBase("hadoop2", 2181, "/hbase");
        db = new HBase(confPath, zPrincipal, principal, keytab, fallback);
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        db.close();
    }

    @Test
    public void listNamespaces() throws IOException {
        LOG.info("namespaces({})", db.listNameSpaces());
    }

    @Test
    public void table() throws IOException {
//        String table = "manga:fruit";
        String table = "peTable";
        Pair<String, String> pair = HBase.tableName(table);
        LOG.info("table: {} {}", pair.getFirst(), pair.getSecond());
        String snapshot = HBase.tableSnapshot(table);
        LOG.info("snapshot({})", snapshot);
        LOG.info("table({})", HBase.snapshotTable(snapshot));
    }

    @Test
    public void columnFamilies2Json() throws IOException {
        String table = "manga:fruit";
        Map<String, Object> families = db.getColumnFamilies(table);
        LOG.info("table({})'s desc: {}", table, families);
        String json = JSON.toJSONString(families, SerializerFeature.PrettyFormat, SerializerFeature.MapSortField);
        String fileName = "tmp/json/" + table.replaceFirst(":", "-") + ".json";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(json);
            LOG.info("written column families to {}", fileName);
        } catch (IOException e) {
            LOG.error("Error writing to file: {}", fileName, e);
        }
    }

    @Test
    public void createTable() throws IOException {
        String table = "manga:fruit";
        Map<String, Object> cf = new HashMap<>();
        cf.put("blockCache", true);
        cf.put("blockSize", 65536);
        cf.put("bloomFilter", "ROW");
        cf.put("compression", "NONE");
        cf.put("dataBlockEncoding", "NONE");
        cf.put("inMemory", false);
        cf.put("keepDeleteCells", "FALSE");
        cf.put("minVersions", 0);
        cf.put("scope", 1);
        cf.put("ttl", 2147483647);
        cf.put("versions", 1);
        Map<String, Object> cfMap = new HashMap<>();
        cfMap.put("cf", cf);
        db.createTable(table, cfMap, false);
        LOG.info("created table({})", table);
    }

    @Test
    public void createTable2() throws IOException {
        String file = "tmp/json/fruit.json";
        db.createTable(file);
    }

    @Test
    public void listTables() throws IOException {
        List<String> tables = new ArrayList<>();
        for (String space: db.listNameSpaces()) {
            tables.addAll(db.listTables(space));
        }
        LOG.info("tables: {}", tables);
    }

    @Test
    public void listTables2() throws IOException {
        List<String> tables = db.listTables(Pattern.compile(".*"));
        LOG.info("tables: {}", tables);
    }

    @Test
    public void dropTable() throws IOException {
        String table = "manga:student_bak";
        db.dropTable(table);
    }

    @Test
    public void dropAll() throws IOException {
        List<String> tables = db.listTables(Pattern.compile(".*"));
        for (String table: tables) {
            db.dropTable(table);
        }
    }

    @Test
    public void countTableRows() throws IOException {
        String name = "peTable";
        LOG.info("row({})", db.countTableRows(name));
    }

    @Test
    public void countRows() throws Exception {
//        String name = "manga:fruit";
        String name = "peTable_250506";
        LOG.info("countRows({})", db.countRows(name));
    }

    @Test
    public void renameTable() throws IOException {
        String name = "manga:student";
        String newName = "manga:student2";
        db.renameTable(name, newName);
    }
}