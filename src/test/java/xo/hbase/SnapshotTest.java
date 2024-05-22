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
//    private static final String user = "sunxo";
//    private static final String srcHost = "ubuntu";
    private static final String srcHost = "hadoop3";
    private static final String srcPath = System.getProperty("user.dir") + "/src/main/resources/" + srcHost;
    private static final String tgtHost = "hadoop2";
    private static final String table = "manga:fruit";
    private static HBase srcDb;
    private static HBase tgtDb;
    private static String snapshot;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
//        srcDb = new HBase(srcHost);
        srcDb = new HBase(HBase.getPath(srcPath));

        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        String dateStr = sdf.format(new Date());
        snapshot = table.replaceFirst(":", "-") + "_" + dateStr;

//        tgtDb = new HBase(tgtHost, 2181, "/hbase");
        tgtDb = new HBase(tgtHost);
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        tgtDb.close();
        srcDb.close();
    }

    @Test
    public void listSnapshots() throws IOException {
        LOG.info("snapshots: {}", srcDb.listSnapshots());
    }

    @Test
    public void deleteSnapshots() throws IOException {
        for (String snapshot: srcDb.listSnapshots()) {
            srcDb.deleteSnapshot(snapshot);
        }
    }

    @Test
    public void createSnapshot() throws IOException {
        srcDb.createSnapshot(table, snapshot);
    }

    @Test
    public void cloneSnapshot() throws IOException {
        srcDb.cloneSnapshot(snapshot, snapshot.replaceFirst("-", ":"));
    }

    @Test
    public void exportSnapshot() throws Exception {
//        String fs = tgtDb.getProperty("fs.defaultFS");
        HBase.changeUser(tgtDb.getUser());
        String copyTo = tgtDb.getProperty("hbase.rootdir");
        LOG.info("{}", srcDb.exportSnapshot(snapshot, copyTo));
    }

    @Test
    public void cloneSnapshotsTgt() throws IOException {
        for (String snapshot: tgtDb.listSnapshots()) {
            tgtDb.cloneSnapshot(snapshot, snapshot.replaceFirst("-", ":"));
        }
    }

    @Test
    public void deleteSnapshotsTgt() throws IOException {
        for (String snapshot: tgtDb.listSnapshots()) {
            tgtDb.deleteSnapshot(snapshot);
        }
    }
}
