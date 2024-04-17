package xo.hbase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SnapshotTest2 {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotTest2.class);
    private static HBase db;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        String host = "hadoop2";
        db = new HBase(host, 2181, "/hbase");
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        db.close();
    }

    @Test
    public void cloneSnapshot() throws IOException {
        db.cloneSnapshot( "snap_student", "manga:student_bak");
    }
}
