package xo.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ReplicateSnapshot {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateSnapshot.class);

    private final ReplicateConfig config;
    private final HBase srcDb;
    private final HBase tgtDb;

    public ReplicateSnapshot() throws IOException {
        this.config = ReplicateConfig.getInstance();
        this.srcDb = new HBase(
                config.getSourceHBaseQuorumHost(),
                config.getSourceHBaseQuorumPort(),
                config.getSourceHBaseQuorumPath());
        this.tgtDb = new HBase(
                config.getTargetHBaseQuorumHost(),
                config.getTargetHBaseQuorumPort(),
                config.getTargetHBaseQuorumPath());
    }

    private void addPeer() throws IOException {
        String rpcSvrZNode = config.getReplicateServerQuorumPath();
        String peer = rpcSvrZNode.substring(1);
        String clusterKey = String.format("%s:%d:%s",
                config.getReplicateServerQuorumHost(),
                config.getReplicateServerQuorumPort(),
                rpcSvrZNode);
        if ("table".equals(config.getSourceHBaseMapType())) {
            srcDb.addPeer(peer, clusterKey, new ArrayList<>(config.getSourceHBaseMapTables().keySet()), false);
        } else if ("user".equals(config.getSourceHBaseMapType())) {
            srcDb.addPeer(peer, clusterKey, config.getSourceHBaseMapNamespaces().keySet(), false);
        } else {
            srcDb.addPeer(peer, clusterKey, false);
        }
    }

    public int doSnapshot(String table, String copyTo) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        String dateStr = sdf.format(new Date());
        String snapshot = table.replaceFirst(":", "-") + "-" + dateStr;
        srcDb.createSnapshot(table, snapshot);
        LOG.debug("snapshot({}) from table({}) created", snapshot, table);

        int rc = srcDb.exportSnapshot(snapshot, copyTo);
        LOG.debug("export snapshot({}) to {} return {}", rc, copyTo, rc);

        if (rc == 0) {
            tgtDb.cloneSnapshot(snapshot, table);
            LOG.debug("snapshot({}) cloned to table({})", snapshot, table);
        }

        return rc;
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
            for (String space: srcDb.listNameSpaces()) {
                tables.addAll(srcDb.listTables(space));
            }
        }
        String copyTo = String.format("hdfs://%s:%d/hbase",
                config.getTargetHadoopHdfsHost(),
                config.getTargetHadoopHdfsPort());

        for (String table: tables) {
            int rc = doSnapshot(table, copyTo);
        }
    }

    public void close() throws IOException {
        tgtDb.close();
        srcDb.close();
    }

    public static void main(String[] args) throws Exception {
        ReplicateSnapshot replicateSnapshot = new ReplicateSnapshot();
        replicateSnapshot.addPeer();
        replicateSnapshot.doSnapshots();
        replicateSnapshot.close();
    }
}
