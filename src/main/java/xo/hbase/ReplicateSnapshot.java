package xo.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class ReplicateSnapshot {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateSnapshot.class);

    private final ReplicateConfig config;
    private final HBase srcDb;
    private final ExportSnapshotClient exportSnapshotClient;
//    private final String peer;

    public ReplicateSnapshot() throws IOException, InterruptedException {
        this.config = ReplicateConfig.getInstance();
        this.srcDb = new HBase(config.getSourceHBaseConfPath(), null, null, null,
                false);
        this.exportSnapshotClient = new ExportSnapshotClient("localhost", 31415);
//        this.peer = config.getReplicateServerPeer();
    }

//    public void addPeer() throws IOException {
//        String clusterKey = String.format("%s:%d:%s",
//                config.getReplicateServerQuorumHost(),
//                config.getReplicateServerQuorumPort(),
//                config.getReplicateServerRpcSvrZNode());
//        if ("table".equals(config.getSourceHBaseMapType())) {
//            srcDb.addPeer(peer, clusterKey, new ArrayList<>(config.getSourceHBaseMapTables().keySet()), false);
//        } else if ("user".equals(config.getSourceHBaseMapType())) {
//            srcDb.addPeer(peer, clusterKey, config.getSourceHBaseMapNamespaces().keySet(), false);
//        } else {
//            srcDb.addPeer(peer, clusterKey, false);
//        }
//    }
//
//    public void enablePeer() throws IOException {
//        srcDb.setPeerState(peer, true);
//    }

    public void doSnapshot(String table, String copyFrom) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        String dateStr = sdf.format(new Date());
        String snapshot = table.replaceFirst(":", "-") + "_" + dateStr;
        srcDb.createSnapshot(table, snapshot);
        LOG.info("snapshot({}) from table({}) created", snapshot, table);
        LOG.info(exportSnapshotClient.exportSnapshotSync(
                new ExportRequest(table +"_" + dateStr, snapshot, copyFrom),
                300000).message);
    }

    public void doSnapshots() throws Exception {
        List<String> tables = new ArrayList<>();
        if ("table".equals(config.getSourceHBaseMapType())) {
            tables.addAll(config.getSourceHBaseMapTables().keySet());
        } else if ("user".equals(config.getSourceHBaseMapType())) {
            for (String space: config.getSourceHBaseMapNamespaces().keySet()) {
                tables.addAll(srcDb.listTables(space));
            }
        } else {
            tables = srcDb.listTables(Pattern.compile(".*"));
        }

        String copyFrom = srcDb.getProperty("hbase.rootdir");
        for (String table: tables) {
            doSnapshot(table, copyFrom);
        }
    }

    public void close() throws IOException {
        exportSnapshotClient.close();
        srcDb.close();
    }

    public static void main(String[] args) throws Exception {
        ReplicateSnapshot replicateSnapshot = new ReplicateSnapshot();
//        replicateSnapshot.addPeer();
        replicateSnapshot.doSnapshots();
//        replicateSnapshot.enablePeer();
        replicateSnapshot.close();
    }
}
