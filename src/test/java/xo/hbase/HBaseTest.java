package xo.hbase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.*;

public class HBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTest.class);
    private static HBase db;
    private static final String peer = "rpcSvr";
    private static final String peerHost = "centos4";

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        String host = "ubuntu";
        db = new HBase(host, 2181, "/hbase");
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        db.close();
    }

    @Test
    public void listPeers() throws IOException {
        LOG.info("peers: {}", db.listPeers());
    }

    @Test
    public void addPeer() throws IOException {
        String key = String.format("%s:2181:/%s", peerHost, peer);
        db.addPeer(peer, key, Arrays.asList("peTable", "manga:fruit"));
        LOG.info("peers: {}", db.listPeers());
    }

    @Test
    public void addPeer2() throws IOException {
        String key = String.format("%s:2181:/%s", peerHost, peer);
        db.addPeer(peer, key, new HashSet<>(Arrays.asList("default", "manga")));
        LOG.info("peers: {}", db.listPeers());
    }

    @Test
    public void disablePeer() throws IOException {
        db.disablePeer(peer);
    }

    @Test
    public void enablePeer() throws IOException {
        db.enablePeer(peer);
    }

    @Test
    public void removePeer() throws IOException {
        db.removePeer(peer);
        LOG.info("peers: {}", db.listPeers());
    }

    @Test
    public void isPeerEnabled() throws IOException {
        LOG.info(db.isPeerEnabled(peer) ? "enabled" : "disabled");
    }
}