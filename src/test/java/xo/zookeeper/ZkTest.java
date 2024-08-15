package xo.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class ZkTest {
    private static final Logger LOG = LoggerFactory.getLogger(ZkTest.class);
    private static ZkConnect zk;

    @BeforeClass
    public static void setupBeforeClass() throws IOException, InterruptedException {
        zk = new ZkConnect("localhost", 2181);
//        zk = new ZkConnect("localhost:2181");
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException, InterruptedException {
        zk.close();
    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {
        zk.createNode("test/uuid", UUID.randomUUID().toString().getBytes(), false);
    }
}
