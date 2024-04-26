package xo.hbase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class TableTest {
    private static final Logger LOG = LoggerFactory.getLogger(TableTest.class);

    private static final String host = "hadoop2";
    private static HBase db;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
//        db = new HBase();
        db = new HBase(host, 2181, "/hbase");
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        db.close();
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
}
