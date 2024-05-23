package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

// refer to org.apache.hadoop.hbase.client sample in https://hbase.apache.org/apidocs/index.html
public class HBase {
    private static final Logger LOG = LoggerFactory.getLogger(HBase.class);

    private final Configuration conf;
    private final Connection conn;
    private final Admin admin;

    static public Path getPath(String pathStr) {
        return new Path(pathStr);
    }

    static public void changeUser(String user) throws IOException {
        String current = UserGroupInformation.getCurrentUser().getShortUserName();
        if (!current.equals(user)) {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
            UserGroupInformation.setLoginUser(ugi);
            LOG.info("changed user from {} to {}", current, user);
        }
    }

    public HBase() throws IOException {
        conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public HBase(String host, int port, String znode) throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        conf.set("hbase.zookeeper.property.clientPort", "" + port);
        conf.set("zookeeper.znode.parent", znode);
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public HBase(String pathStr) throws IOException {
        conf = HBaseConfiguration.create();
        conf.addResource(pathStr + "/core-site.xml");
        conf.addResource(pathStr + "/hdfs-site.xml");
        conf.addResource(pathStr + "/mapred-site.xml");
        conf.addResource(pathStr + "/yarn-site.xml");
        conf.addResource(pathStr + "/hbase-site.xml");
//        conf.forEach(entry -> System.out.println(entry.getKey() + "=" + entry.getValue()));
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public HBase(Path path) throws IOException {
        conf = HBaseConfiguration.create();
        conf.addResource(new Path(path, "core-site.xml"));
        conf.addResource(new Path(path, "hdfs-site.xml"));
        conf.addResource(new Path(path, "mapred-site.xml"));
        conf.addResource(new Path(path, "yarn-site.xml"));
        conf.addResource(new Path(path, "hbase-site.xml"));
//        conf.forEach(entry -> System.out.println(entry.getKey() + "=" + entry.getValue()));
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public void close() throws IOException {
        admin.close();
        conn.close();
    }

    public String getProperty(String name) {
        return conf.get(name);
    }

    public String getUser() throws IOException {
        String root = conf.get("hbase.rootdir");
        if (root == null) {
            LOG.error("HBase root is not set in the configuration");
            return null;
        }
        FileSystem fs = FileSystem.get(conf);
        FileStatus fileStatus = fs.getFileStatus(new Path(root));
        return fileStatus.getOwner();
    }

    public List<String> listNameSpaces() throws IOException {
        List<String> spaces = new ArrayList<>();
        NamespaceDescriptor[] descriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor descriptor : descriptors) {
            String name = descriptor.getName();
            if (!"hbase".equals(name)) {
                spaces.add(name);
            }
        }
        return spaces;
    }

    public List<String> listTables(String space) throws IOException {
        List<String> tables = new ArrayList<>();
        List<TableDescriptor> descriptors = admin.listTableDescriptorsByNamespace(Bytes.toBytes(space));
        for (TableDescriptor descriptor: descriptors) {
            tables.add(descriptor.getTableName().getNameAsString());
        }
        return tables;
    }

    public List<String> listTables(Pattern pattern) throws IOException {
        List<String> tables = new ArrayList<>();
        List<TableDescriptor> descriptors = admin.listTableDescriptors(pattern);
        for (TableDescriptor descriptor: descriptors) {
            tables.add(descriptor.getTableName().getNameAsString());
        }
        return tables;
    }

    public void createTable(String space, String name, String family) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        if (! admin.tableExists(tableName)) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
                            .setBloomFilterType(BloomType.ROWCOL)
                            .setCompressionType(Compression.Algorithm.GZ).build())
                    .build();
            admin.createTable(tableDescriptor);
        }
    }

    public void dropTable(String table) throws IOException {
        TableName name = TableName.valueOf(table);
        try {
            admin.disableTable(name);
            admin.deleteTable(name);
        } catch (TableNotFoundException e) {
            LOG.info("table({}) doesn't exist", table);
        } catch (TableNotEnabledException e) {
            admin.deleteTable(name);
        }
    }

    public void dropTable(String space, String name) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    public void truncateTable(String name) throws IOException {
        TableName tableName = TableName.valueOf(name);
        if (admin.tableExists(tableName)) {
            if (admin.isTableEnabled(tableName)) {
                admin.disableTable(tableName);
            }
            admin.truncateTable(tableName, false);
        }
    }

    public void putCell(String space, String name, byte[] key, byte[] family, byte[] qualifier, byte[] value)
            throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        try (Table table = conn.getTable(tableName)) {
            Put put = new Put(key);
            put.addColumn(family, qualifier, value);
            table.put(put);
        }
    }

    public void putRow(String name, Pair<String, Map<String, Map<String, String>>> row)
            throws IOException {
        TableName tableName = TableName.valueOf(name);
        try (Table table = conn.getTable(tableName)) {
                String key = row.getFirst();
                Put put = new Put(Bytes.toBytes(key));
                Map<String, Map<String, String>> families = row.getSecond();
            for (Map.Entry<String, Map<String, String>> familyEntry : families.entrySet()) {
                String columnFamily = familyEntry.getKey();
                Map<String, String> qualifiers = familyEntry.getValue();
                for (Map.Entry<String, String> qualifierEntry : qualifiers.entrySet()) {
                    String qualifier = qualifierEntry.getKey();
                    String value = qualifierEntry.getValue();
                    LOG.debug("Row: {}, column family: {}, qualifier: {}, value: {}",
                            key, columnFamily, qualifier, value);
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                }
            }
            table.put(put);
        }
    }

    public void putRows(String name, Map<String, Map<String, Map<String, String>>> rows) throws IOException {
        TableName tableName = TableName.valueOf(name);
        try (Table table = conn.getTable(tableName)) {
            for (Map.Entry<String, Map<String, Map<String, String>>> rowEntry : rows.entrySet()) {
                String row = rowEntry.getKey();
                Put put = new Put(Bytes.toBytes(row));
                Map<String, Map<String, String>> families = rowEntry.getValue();
                for (Map.Entry<String, Map<String, String>> familyEntry : families.entrySet()) {
                    String columnFamily = familyEntry.getKey();
                    Map<String, String> qualifiers = familyEntry.getValue();
                    for (Map.Entry<String, String> qualifierEntry : qualifiers.entrySet()) {
                        String qualifier = qualifierEntry.getKey();
                        String value = qualifierEntry.getValue();
//                        System.out.println("Row: " + row + ", Column Family: " + columnFamily +
//                                ", Qualifier: " + qualifier + ", Value: " + value);
                        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                    }
                }
                table.put(put);
            }
        }
    }

    public void deleteCell(String space, String name, byte[] key, byte[] family) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        try (Table table = conn.getTable(tableName)) {
            Delete delete = new Delete(key);
            delete.addFamily(family);
            table.delete(delete);
        }
    }

    public void deleteRow(String name, String key) throws IOException {
        TableName tableName = TableName.valueOf(name);
        try (Table table = conn.getTable(tableName)) {
            table.delete(new Delete(Bytes.toBytes(key)));
        }
    }

    public Map<String, Map<String, Map<String, String>>> scanTable(String name) throws IOException {
        TableName tableName = TableName.valueOf(name);
        if (! admin.tableExists(tableName)) {
            return null;
        }
        Map<String, Map<String, Map<String, String>>> tableData = new HashMap<>();
        try (Table table = conn.getTable(tableName)) {
            try (ResultScanner scanner = table.getScanner(new Scan())) {
                for (Result result: scanner) {
                    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyMap = result.getNoVersionMap();
                    Map<String, Map<String, String>> row = new HashMap<>();
                    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> familyEntry: familyMap.entrySet()) {
                        NavigableMap<byte[], byte[]> qualifierMap = familyEntry.getValue();
                        Map<String, String> family = new TreeMap<>();
                        for (Map.Entry<byte[], byte[]> qualifierEntry: qualifierMap.entrySet()) {
                            family.put(Bytes.toString(qualifierEntry.getKey()),
                                    Bytes.toString(qualifierEntry.getValue()));
                        }
                        row.put(Bytes.toString(familyEntry.getKey()), family);
                    }
                    tableData.put(Bytes.toString(result.getRow()), row);
                }
            }
        }
        return tableData;
    }

    public boolean isTableEmpty(String name) throws IOException {
        TableName tableName = TableName.valueOf(name);
        if (!admin.tableExists(tableName)) {
            return false;
        }
        try (Table table = conn.getTable(tableName)) {
            try (ResultScanner scanner = table.getScanner(new Scan())) {
                Result result = scanner.next();
                scanner.close();
                table.close();
                return result == null;
            }
        }
    }

    public long countTableRows(String name) throws IOException {
        TableName tableName = TableName.valueOf(name);
        if (!admin.tableExists(tableName)) {
            return 0;
        }
        try (Table table = conn.getTable(tableName)) {
            try (ResultScanner scanner = table.getScanner(new Scan())) {
                long rowCount = 0;
                for (Result result : scanner) {
                    rowCount++;
                }
                scanner.close();
                table.close();
                return rowCount;
            }
        }
    }

    public String getCell(String space, String name, String pk, String cf, String col) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        try (Table table = conn.getTable(tableName)) {
            Get g = new Get(Bytes.toBytes(pk));
            Result r = table.get(g);
            byte[] value = r.getValue(Bytes.toBytes(cf), Bytes.toBytes(col));
            return Bytes.toString(value);
        }
    }

    public List<String> listPeers() throws IOException {
        List<String> peers = new ArrayList<>();
        List<ReplicationPeerDescription> descriptions = admin.listReplicationPeers();
        for (ReplicationPeerDescription descriptor : descriptions) {
            peers.add(descriptor.getPeerId());
        }
        LOG.debug("peers: {}", peers);
        return peers;
    }

    public void addPeer(String peerId, String clusterKey, String endpoint) throws IOException {
        ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
                .setClusterKey(clusterKey)
                .setReplicationEndpointImpl(endpoint)
                .build();
        admin.addReplicationPeer(peerId, peerConfig, false);
    }

    public void setPeerState(String peerId, boolean enabled) throws IOException {
        if (enabled) {
            try {
                admin.enableReplicationPeer(peerId);
            } catch (DoNotRetryIOException e) {
                LOG.info("peer({}) is already enabled", peerId);
            }
        } else {
            try {
                admin.disableReplicationPeer(peerId);
            } catch (DoNotRetryIOException e) {
                LOG.info("peer({}) is already disabled", peerId);
            }
        }
    }

    public void addPeer(String peerId, String clusterKey, boolean enabled) throws IOException {
        ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
                .setClusterKey(clusterKey)
                .setReplicateAllUserTables(true)
                .build();
        try {
            ReplicationPeerConfig original = admin.getReplicationPeerConfig(peerId);
            if (original.getTableCFsMap() == null && original.getNamespaces() == null) {
                LOG.warn("peer({}) already exists", peerId);
                setPeerState(peerId, enabled);
                return;
            }
            setPeerState(peerId, false);
            admin.removeReplicationPeer(peerId);
            LOG.info("peer({}) exists but is different, it needs to be replaced", peerId);
        } catch (ReplicationPeerNotFoundException e) {
            LOG.info("to create new peer({})", peerId);
        }
        admin.addReplicationPeer(peerId, peerConfig, enabled);
    }

    public void addPeer(String peerId, String clusterKey, List<String> tables, boolean enabled) throws IOException {
        Map<TableName, List<String>> map = new HashMap<>();
        for (String table: tables) {
            map.put(TableName.valueOf(table), new ArrayList<>());
        }
        ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
                .setClusterKey(clusterKey)
                .setReplicateAllUserTables(false)
                .setTableCFsMap(map)
                .build();
        try {
            Map<TableName, List<String>> original = admin.getReplicationPeerConfig(peerId).getTableCFsMap();
            if (original != null && original.keySet().equals(peerConfig.getTableCFsMap().keySet())) {
                LOG.warn("peer({}) already exists", peerId);
                setPeerState(peerId, enabled);
                return;
            }
            setPeerState(peerId, false);;
            admin.removeReplicationPeer(peerId);
            LOG.info("peer({}) exists but is different, it needs to be replaced", peerId);
        } catch (ReplicationPeerNotFoundException e) {
            LOG.info("to create new peer({})", peerId);
        }
        admin.addReplicationPeer(peerId, peerConfig, enabled);
    }

    public void addPeer(String peerId, String clusterKey, Set<String> spaces, boolean enabled) throws IOException {
        ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
                .setClusterKey(clusterKey)
                .setReplicateAllUserTables(false)
                .setNamespaces(spaces)
                .build();
        try {
            Set<String> original = admin.getReplicationPeerConfig(peerId).getNamespaces();
            if (original != null && original.equals(peerConfig.getNamespaces())) {
                LOG.warn("peer({}) already exists", peerId);
                setPeerState(peerId, enabled);
                return;
            }
            setPeerState(peerId, false);;
            admin.removeReplicationPeer(peerId);
            LOG.info("peer({}) exists but is different, it needs to be replaced", peerId);
        } catch (ReplicationPeerNotFoundException e) {
            LOG.info("to create new peer({})", peerId);
        }
        admin.addReplicationPeer(peerId, peerConfig, enabled);
    }

    public int peerState(String peerId) throws IOException {
        List<ReplicationPeerDescription> peers = admin.listReplicationPeers(Pattern.compile(peerId));
        if (peers.isEmpty() || !peerId.equals(peers.get(0).getPeerId())) {
            return -1;
        }
        return peers.get(0).isEnabled() ? 1 : 0;
    }

    public boolean isPeerEnabled(String peerId) throws IOException {
        List<ReplicationPeerDescription> peers = admin.listReplicationPeers(Pattern.compile(peerId));
        if (peers.isEmpty() || !peerId.equals(peers.get(0).getPeerId())) {
            throw new ReplicationPeerNotFoundException(peerId);
        }
        return peers.get(0).isEnabled();
    }

    public void removePeer(String peerId) throws IOException {
        admin.removeReplicationPeer(peerId);
    }

    public List<String> listSnapshots() throws IOException {
        List<String> snapshots = new ArrayList<>();
        List<SnapshotDescription> descriptions = admin.listSnapshots();
        for (SnapshotDescription descriptor : descriptions) {
            snapshots.add(descriptor.getName());
        }
        LOG.debug("peers: {}", snapshots);
        return snapshots;
    }

    public void createSnapshot(String tableName, String snapshotName) throws IOException {
        List<SnapshotDescription> descriptions = admin.listSnapshots(Pattern.compile(snapshotName));
        if (!descriptions.isEmpty()) {
            admin.deleteSnapshot(snapshotName);
            LOG.info("deleted old snapshot({})", snapshotName);
        }
        try {
            admin.snapshot(snapshotName, TableName.valueOf(tableName), new HashMap<>());
        } catch (SnapshotCreationException e) {
            LOG.info("table({}) doesnt existed", tableName);
        }
    }

    public void cloneSnapshot(String snapshotName, String tableName) throws IOException {
        List<SnapshotDescription> descriptions = admin.listSnapshots(Pattern.compile(snapshotName));
        if (descriptions.isEmpty()) {
            LOG.info("snapshot({}) doesn't exist", snapshotName);
        }
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            LOG.info("table({}) already exists", tableName);
            return;
        }
        admin.cloneSnapshot(snapshotName, table);
    }

    public void deleteSnapshot(String snapshotName) throws IOException {
        List<SnapshotDescription> descriptions = admin.listSnapshots(Pattern.compile(snapshotName));
        if (descriptions.isEmpty()) {
            LOG.info("snapshot({}) doesn't exist", snapshotName);
        }
        admin.deleteSnapshot(snapshotName);
    }

    public int exportSnapshot(String snapshot, String copyTo) throws Exception {
        List<String> opts = new ArrayList<>();
        opts.add("--snapshot");
        opts.add(snapshot);
        opts.add("--copy-to");
        opts.add(copyTo);
        opts.add("--overwrite");
        LOG.info("export --snapshot {} --copy-to {} --overwrite", snapshot, copyTo);
        int rc = ToolRunner.run(conf, new ExportSnapshot(), opts.toArray(new String[0]));
        LOG.info("exported rc({})", rc);
        return rc;
    }
}