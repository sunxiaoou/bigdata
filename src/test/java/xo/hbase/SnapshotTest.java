package xo.hbase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SnapshotTest {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotTest.class);
    private static HBase db;
    private static final String user = "sunxo";
    private static final String snapshot = "snap_student";
    private static final String copyTo = "hdfs://hadoop2:8020/hbase";

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        db = new HBase(user);
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        db.close();
    }

    @Test
    public void listSnapshots() throws IOException {
        LOG.info("snapshots: {}", db.listSnapshots());
    }

    @Test
    public void createSnapshot() throws IOException {
        db.createSnapshot("manga:student", "snap_student");
    }

    @Test
    public void cloneSnapshot() throws IOException {
        db.cloneSnapshot( "snap_student", "manga:student_bak");
    }

    @Test
    public void exportSnapshot() throws Exception {
        LOG.info("{}", db.exportSnapshot(snapshot, copyTo));
    }
}
