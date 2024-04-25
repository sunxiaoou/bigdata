package xo.hbase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableTest {
    private static final Logger LOG = LoggerFactory.getLogger(TableTest.class);
    private static HBase db;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        db = new HBase();
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
}
