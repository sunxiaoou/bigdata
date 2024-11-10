package xo.hbase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class PeerTest {
    private static final Logger LOG = LoggerFactory.getLogger(PeerTest.class);
    private static HBase db;
    private static final String peer = "c2";
    private static final String peerHost = "centos2";

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        String host = "hb_u";
        db = new HBase(host, 2181, "/hbase");
//        db = new HBase();
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
        String key = String.format("%s:2181:/hbase", peerHost);
//        String key = String.format("%s:2181:/%s", peerHost, peer);
        db.addPeer(peer, key, Arrays.asList("bulk", "manga:fruit"), true);
        LOG.info("peers: {}", db.listPeers());
    }

    @Test
    public void addPeer2() throws IOException {
        String key = String.format("%s:2181:/%s", peerHost, peer);
        db.addPeer(peer, key, new HashSet<>(Arrays.asList("default", "manga")), true);
        LOG.info("peers: {}", db.listPeers());
    }

    @Test
    public void disablePeer() throws IOException {
        db.setPeerState(peer, false);
    }

    @Test
    public void enablePeer() throws IOException {
        db.setPeerState(peer, true);
    }

    @Test
    public void removePeer() throws IOException {
        db.removePeer(peer);
        LOG.info("peers: {}", db.listPeers());
    }

    @Test
    public void isPeerEnabled() throws IOException {
        LOG.info("peer({}) {}", peer, db.isPeerEnabled(peer) ? "enabled" : "disabled");
    }
}