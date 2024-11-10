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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class TableTest {
    private static final Logger LOG = LoggerFactory.getLogger(TableTest.class);

    private static final String host = "hb_h2";
//    private static final String host = "ubuntu";
    private static final String confPath = System.getProperty("user.dir") + "/src/main/resources/" + host;
    private static HBase db;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
//        db = new HBase();
//        db = new HBase("node1", 2181, "/hbase");
//        db = new HBase("hadoop2", 2181, "/hbase");
//        db = new HBase(host);
        db = new HBase(HBase.getPath(confPath));
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
            e.printStackTrace();
        }
    }

    @Test
    public void createTable() throws IOException {
        String file = "tmp/json/manga-fruit.json";
        String table = "manga:fruit2";
        String json = new String(Files.readAllBytes(Paths.get(file)), StandardCharsets.UTF_8);
        Map<String, Object> cfMap = JSON.parseObject(json, Map.class);
        db.createTable(table, cfMap);
        LOG.info("created table({}) from {}", table, file);
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
    public void renameTable() throws IOException {
        String name = "manga:student";
        String newName = "manga:student2";
        db.renameTable(name, newName);
    }
}