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
    private final HBase tgtDb;
//    private final String peer;

    public ReplicateSnapshot() throws IOException {
        this.config = ReplicateConfig.getInstance();
        this.srcDb = new HBase(config.getSourceHBaseConfPath(), null, null, false);
        this.tgtDb = new HBase(config.getTargetHBaseConfPath(), config.getTargetHBasePrincipal(),
                config.getTargetHBaseKeytab(), true);
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

    public void doSnapshot(String copyFrom, String copyTo, String table) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        String dateStr = sdf.format(new Date());
        String snapshot = table.replaceFirst(":", "-") + "_" + dateStr;
        srcDb.createSnapshot(table, snapshot);
        LOG.info("snapshot({}) from table({}) created", snapshot, table);

        tgtDb.distcpSnapshot(snapshot, copyFrom, copyTo);
        String table2 = table + "_" + dateStr;
        tgtDb.cloneSnapshot(snapshot, table2);
        LOG.info("snapshot({}) cloned to table({})", snapshot, table2);
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
//        HBase.changeUser(config.getTargetHadoopUser());

        String copyFrom = srcDb.getProperty("hbase.rootdir");
        String copyTo = tgtDb.getProperty("hbase.rootdir");
        for (String table: tables) {
            doSnapshot(copyFrom, copyTo, table);
        }
    }

    public void close() throws IOException {
        tgtDb.close();
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
