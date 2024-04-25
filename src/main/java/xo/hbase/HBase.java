package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Triple;
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
    Configuration conf;
    Connection conn;
    Admin admin;

    public HBase() throws IOException {
        conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public HBase(String user) throws IOException {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        UserGroupInformation.setLoginUser(ugi);
        conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
        Configuration conf = HBaseConfiguration.create();
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

    public void close() throws IOException {
        admin.close();
        conn.close();
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
        for (TableDescriptor descriptor : descriptors) {
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

    public void dropTable(String space, String name) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    public void truncateTable(String space, String name) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
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

    public void putRow(String space, String name, Pair<String, Map<String, Map<String, String>>> row)
            throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
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

    public void putRows(String space, String name, Map<String, Map<String, Map<String, String>>> rows)
            throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
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

    public void deleteRow(String space, String name, String key) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        try (Table table = conn.getTable(tableName)) {
            table.delete(new Delete(Bytes.toBytes(key)));
        }
    }

    public Map<String, Map<String, Map<String, String>>> scanTable(String space, String name) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
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

    public boolean isTableEmpty(String space, String name) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
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

    public long countTableRows(String space, String name) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
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
        if (enabled && !isPeerEnabled(peerId)) {
            admin.enableReplicationPeer(peerId);
        } else if (!enabled && isPeerEnabled(peerId)) {
            admin.disableReplicationPeer(peerId);
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
            disablePeer(peerId);
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
            disablePeer(peerId);
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
            disablePeer(peerId);
            admin.removeReplicationPeer(peerId);
            LOG.info("peer({}) exists but is different, it needs to be replaced", peerId);
        } catch (ReplicationPeerNotFoundException e) {
            LOG.info("to create new peer({})", peerId);
        }
        admin.addReplicationPeer(peerId, peerConfig, enabled);
    }

    public void disablePeer(String peerId) throws IOException {
        try {
            admin.disableReplicationPeer(peerId);
        } catch (DoNotRetryIOException e) {
            LOG.info("peer({}) is already disabled", peerId);
        }
    }

    public void enablePeer(String peerId) throws IOException {
        admin.enableReplicationPeer(peerId);
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
        admin.snapshot(snapshotName, TableName.valueOf(tableName), new HashMap<>());
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
        return ToolRunner.run(conf, new ExportSnapshot(), opts.toArray(new String[0]));
    }

    /**
     * Convert triple of a fruit row to a pair
     * @param triple <row_number, name_value, price_value>
     * @return a Pair - (row_string, {"cf": {"name": name_string, "price": price_string}})
     */
    public static Pair<String, Map<String, Map<String, String>>> fruit(Triple<Integer, String, Float> triple) {
        Map<String, Map<String, String>> families = new HashMap<>();
        Map<String, String> family = new TreeMap<>();
        family.put("name", triple.getSecond());
        family.put("price", String.valueOf(triple.getThird()));
        families.put("cf", family);
        return new Pair<>(String.valueOf(triple.getFirst()), families);
    }

    /**
     * Convert a triple array to map
     * @return a Map - {row_string: {"cf": {"name": name_string, "price": price_string}}}
     */
    public static Map<String, Map<String, Map<String, String>>> fruits() {
        final Triple<Integer, String, Float>[] triples = new Triple[] {
                new Triple<>(101, "🍉", (float) 800.0),
                new Triple<>(102, "🍓", (float) 150.0),
                new Triple<>(103, "🍎", (float) 120.0),
                new Triple<>(104, "🍋", (float) 200.0),
                new Triple<>(105, "🍊", (float) 115.0),
                new Triple<>(106, "🍌", (float) 110.0)
        };
        Map<String, Map<String, Map<String, String>>> rows = new HashMap<>();
        for (Triple<Integer, String, Float> triple : triples) {
            Pair<String, Map<String, Map<String, String>>> pair = fruit(triple);
            rows.put(pair.getFirst(), pair.getSecond());
        }
        LOG.debug("fruits: {}", rows);
        return rows;
    }

    private static void run(String op, String host, String table) throws IOException {
        // use resources/hbase-site.xml as host is null
        HBase db = host == null ? new HBase(): new HBase(host, 2181, "/hbase");
        String space = "manga";
        String name = "fruit";
        if (table == null) {
            System.out.println(db.listNameSpaces());
            System.out.println(db.listTables("manga"));
        } else {
            if (table.contains(":")) {
                String[] ss = table.split(":");
                space = ss[0];
                name = ss[1];
            } else {
                space = null;
                name = table;
            }
        }
        switch (op) {
            case "put":
                if ("manga".equals(space) && "fruit".equals(name)) {
                    db.putRows(space, name, fruits());
                    System.out.println(name + " put");
                } else {
                    System.out.println("Can only put to \"manga:fruit\"");
                }
                return;
            case "add":
                if ("manga".equals(space) && "fruit".equals(name)) {
                    db.putRow(space, name, fruit(new Triple<>(107, "🍐", (float) 115)));
                    System.out.println(name + " add");
                } else {
                    System.out.println("Can only add to \"manga:fruit\"");
                }
                return;
            case "delete":
                if ("manga".equals(space) && "fruit".equals(name)) {
                    db.deleteRow(space, name, "107");
                    System.out.println(name + " deleted");
                } else {
                    System.out.println("Can only delete from \"manga:fruit\"");
                }
                return;
            case "count":
                System.out.println(String.format("%s has %d rows", name, db.countTableRows(space, name)));
                return;
            case "scan":
                if ("manga".equals(space) && "fruit".equals(name)) {
                    System.out.println(db.scanTable(space, name));
                    System.out.println(name + " scanned");
                } else {
                    System.out.println("Can only scan \"manga:fruit\"");
                }
                return;
            case "isEmpty":
                System.out.println(name + (db.isTableEmpty(space, name) ? " is empty" : " is not empty"));
                return;
            case "truncate":
                db.truncateTable(space, name);
                System.out.println(name + " truncated");
                return;
            default:
                System.out.println("Unknown op: " + op);
        }
        db.close();
    }

    public static void main(String[] args) throws IOException {
        if (args.length > 2) {
            run(args[0], args[1], args[2]);
        } else if (args.length > 1) {
            run(args[0], args[1], null);
        } else if (args.length > 0) {
            run(args[0], null, null);
        } else {
            System.out.println("Usage: HBase put|add|delete|count|scan|isEmpty|truncate host[,host2,...] table");
        }
    }
}