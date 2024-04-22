package xo.hbase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SnapshotTest {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotTest.class);
    private static final String user = "sunxo";
    private static final String tgtHost = "hadoop2";
    private static final String table = "manga:student";
    private static HBase src;
    private static HBase tgt;
    private static String snapshot;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        src = new HBase(user);
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        String dateStr = sdf.format(new Date());
        snapshot = table.replaceFirst(":", "-") + "-" + dateStr;

        tgt = new HBase(tgtHost, 2181, "/hbase");
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        tgt.close();
        src.close();
    }

    @Test
    public void listSnapshots() throws IOException {
        LOG.info("snapshots: {}", src.listSnapshots());
    }

    @Test
    public void createSnapshot() throws IOException {
        src.createSnapshot(table, snapshot);
    }

    @Test
    public void cloneSnapshot() throws IOException {
        src.cloneSnapshot(snapshot, snapshot.replaceFirst("-", ":"));
    }

    @Test
    public void exportSnapshot() throws Exception {
        String copyTo = String.format("hdfs://%s:8020/hbase", tgtHost);
        LOG.info("{}", src.exportSnapshot(snapshot, copyTo));
    }

    @Test
    public void cloneSnapshotTgt() throws IOException {
        tgt.cloneSnapshot(snapshot, snapshot.replaceFirst("-", ":"));
    }
}
