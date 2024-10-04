package xo.hbase;

import org.apache.hadoop.hbase.util.Triple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ClusterTest {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterTest.class);

    private static final String host = "hadoop3";
//    private static final String host = "ubuntu";
    private static final String confPath = System.getProperty("user.dir") + "/src/main/resources/" + host;
    private static HBase db;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        db = new HBase(HBase.getPath(confPath));
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        db.close();
    }

    @Test
    public void getZookeeper() throws IOException {
        Triple<String, Integer, String> triple = db.getZookeeper();
        LOG.info("zookeeper[ips=({}),port=({}),zNode=({})]",
                triple.getFirst(), triple.getSecond(), triple.getThird());
    }

    @Test
    public void compare() throws IOException {
        Triple<String, Integer, String> triple = db.getZookeeper();
        Triple<String, Integer, String> t = new Triple<>(
                "10.1.125.203,10.1.125.204,10.1.125.205",
                2181,
                "/hbase");
        assertEquals(triple, t);
    }
}