package xo.hbase;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hbase.mapreduce.RowCounter.createSubmittableJob;

// refer to org.apache.hadoop.hbase.client sample in https://hbase.apache.org/apidocs/index.html
public class HBase implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HBase.class);

    private final Configuration conf;
    private final Connection conn;
    private final Admin admin;

    static public Pair<String, String> tableName(String fullName) {
        String namespace, name;
        if (fullName.contains(":")) {
            String[] s = fullName.split(":");
            namespace = s[0];
            name = s[1];
        } else {
            namespace = "default";
            name = fullName;
        }
        return new Pair<>(namespace, name);
    }

    static public String tableSnapshot(String table) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        String dateStr = sdf.format(new Date());
        return table.replaceFirst(":", "-") + "_" + dateStr;
    }

    static public String snapshotTable(String snapshot) {
        return snapshot.substring(0, snapshot.length() - 7).replaceFirst("-", ":");
    }

    static public void changeUser(String user) throws IOException {
        String current = UserGroupInformation.getCurrentUser().getShortUserName();
        if (!current.equals(user)) {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
            UserGroupInformation.setLoginUser(ugi);
            LOG.info("changed user from {} to {}", current, user);
        }
    }

    /* Attempt to load the configuration file from the following locations in sequence
        $HBASE_CONF_DIR
        $HBASE_HOME/conf
        Other paths added through HBASE_CLASSPATH
     */
    public HBase() throws IOException {
        this.conf = HBaseConfiguration.create();
        this.conn = ConnectionFactory.createConnection(conf);
        this.admin = conn.getAdmin();
    }

    public HBase(String host, int port, String zNode) throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        conf.set("hbase.zookeeper.property.clientPort", "" + port);
        conf.set("zookeeper.znode.parent", zNode);
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    static Configuration loadConf(String pathStr) throws IOException {
        if (!Files.isDirectory(Paths.get(pathStr))) {
            throw new IOException(String.format("Path %s is not a directory", pathStr));
        }
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(pathStr, "core-site.xml"));
        conf.addResource(new Path(pathStr, "hdfs-site.xml"));
        conf.addResource(new Path(pathStr, "mapred-site.xml"));
        conf.addResource(new Path(pathStr, "yarn-site.xml"));
        conf.addResource(new Path(pathStr, "hbase-site.xml"));
//        conf.forEach(entry -> LOG.info(entry.getKey() + "=" + entry.getValue()));
        LOG.info("default file system: {}", conf.get("fs.defaultFS"));
        return conf;
    }

    static private String extractPrincipal(String zServerJaas) throws IOException {
        Pattern pattern = Pattern.compile("\\bprincipal\\s*=\\s*\"([^\"]+)\"");
        for (String line : Files.readAllLines(Paths.get(zServerJaas))) {
            Matcher matcher = pattern.matcher(line.trim());
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        return null;
    }

    static private void updateKeytabPathInJaas(String pathStr) throws IOException {
        String jaasFilePath = pathStr + "/zoo-client.jaas";
        java.nio.file.Path path = Paths.get(jaasFilePath);
        List<String> lines = Files.readAllLines(path);
        List<String> newLines = new ArrayList<>();
        Pattern pattern = Pattern.compile("(\\s*keyTab\\s*=\\s*\")(.+/)([^/\"\\s]+)(\".*)");
        boolean updated = false;

        for (String line : lines) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                String oldPath = matcher.group(2);
                oldPath = oldPath.endsWith("/") ? oldPath.substring(0, oldPath.length() - 1) : oldPath;
                if (!oldPath.equals(pathStr)) {
                    String newLine = matcher.group(1) + pathStr + "/" + matcher.group(3) + matcher.group(4);
                    newLines.add(newLine);
                    updated = true;
                } else {
                    newLines.add(line);
                }
            } else {
                newLines.add(line);
            }
        }
        if (updated) {
            Files.write(path, newLines, StandardCharsets.UTF_8);
            LOG.info("Updated keyTab path in {}", jaasFilePath);
        }
    }

    static Configuration loadConf(String pathStr, String zPrincipal, boolean fallback) throws IOException {
        Configuration conf = loadConf(pathStr);
        if (Files.isReadable(Paths.get(pathStr + "/krb5.conf"))) {
            System.setProperty("java.security.krb5.conf", pathStr + "/krb5.conf");
            LOG.info("java.security.krb5.conf: {}", System.getProperty("java.security.krb5.conf"));
        }
        if (zPrincipal == null || zPrincipal.isEmpty()) {
            zPrincipal = extractPrincipal(pathStr + "/zoo-server.jaas");
            if (zPrincipal == null) {
                throw new IOException("zookeeper principal is not set");
            }
            LOG.info("extracted zookeeper principal from zoo-server.jaas: {}", zPrincipal);
        }
        System.setProperty("zookeeper.server.principal", zPrincipal);
        updateKeytabPathInJaas(pathStr);
        System.setProperty("java.security.auth.login.config", pathStr + "/zoo-client.jaas");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        if (fallback) {
            conf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
        }
        conf.set("mapreduce.map.memory.mb", "1536");
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        return conf;
    }

    static private String getProviderName(Configuration conf) throws IOException {
        SaslClientAuthenticationProviders providers = SaslClientAuthenticationProviders.getInstance(conf);
        Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> provider =
                providers.selectProvider(conf.get("hbase.cluster.id", "default"), User.getCurrent());
        return provider.getFirst().getClass().getSimpleName();
    }

    static void login(Configuration conf, String principal, String keytab) throws IOException {
        UserGroupInformation.reset();
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        LOG.info("Logged in as '{}'", UserGroupInformation.getLoginUser());
        String provider = getProviderName(conf);
        if (!"GssSaslClientAuthenticationProvider".equals(provider)) {
            throw new RuntimeException("Unsupported authentication provider: " + provider);
        }
    }

    public HBase(String pathStr, String zPrincipal, boolean fallback)
            throws IOException {
        if (zPrincipal != null && !zPrincipal.isEmpty()) {
            conf = loadConf(pathStr, zPrincipal, fallback);
        } else {
            conf = loadConf(pathStr);
            System.setProperty("zookeeper.sasl.client", "false");
        }
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public HBase(String pathStr, String zPrincipal, String principal, String keytab, boolean fallback)
            throws IOException {
        if (principal != null && !principal.isEmpty()) {
            conf = loadConf(pathStr, zPrincipal, fallback);
            login(conf, principal, keytab);
        } else {
            conf = loadConf(pathStr);
            changeUser(getUser(conf));
            System.setProperty("zookeeper.sasl.client", "false");
        }
        LOG.info("Current user: {}", UserGroupInformation.getCurrentUser());
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public void close() throws IOException {
        admin.close();
        conn.close();
    }

    public boolean isClosed() {
        return conn.isClosed();
    }

    public String getProperty(String name) {
        return conf.get(name);
    }

    public Triple<String, Integer, String> getZookeeper() throws UnknownHostException {
        String hosts = conf.get("hbase.zookeeper.quorum");
        String[] hostArray = hosts.split(",");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostArray.length; i ++) {
            InetAddress inetAddress = InetAddress.getByName(hostArray[i]);
            String ip = inetAddress.getHostAddress();
            sb.append(ip);
            if (i < hostArray.length - 1) {
                sb.append(",");
            }
        }
        String ips = sb.toString();
        int port = Integer.parseInt(conf.get("hbase.zookeeper.property.clientPort"));
        String zNode = conf.get("zookeeper.znode.parent");
        return new Triple<>(ips, port, zNode);
    }

    static public String getUser(Configuration conf) throws IOException {
        String root = conf.get("hbase.rootdir");
        if (root == null) {
            LOG.error("HBase root is not set in the configuration");
            return null;
        }
        FileSystem fs = FileSystem.get(conf);
        FileStatus fileStatus = fs.getFileStatus(new Path(root));
        return fileStatus.getOwner();
    }

    public String getVersion() throws IOException {
        return admin.getClusterMetrics().getHBaseVersion();
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

    public void createNamespace(String name) throws IOException {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            LOG.info("namespace({}) already exists", name);
        }
    }

    public void dropNamespace(String name) throws IOException {
        try {
            admin.deleteNamespace(name);
        } catch (NamespaceNotFoundException e) {
            LOG.info("namespace({}) doesn't exist", name);
        }
    }

    public List<String> listNamespaceTables(String space) throws IOException {
        List<String> tables = new ArrayList<>();
        try {
            TableName[] tableNames = admin.listTableNamesByNamespace(space);
            for (TableName tableName: tableNames) {
                String name = tableName.getNameAsString();
                tables.add(name);
            }
        } catch (NamespaceNotFoundException e) {
            LOG.warn("namespace({}) doesn't exist", space);
        }
        return tables;
    }

    public List<String> listTables(String space) throws IOException {
        List<String> tables = new ArrayList<>();
        List<TableDescriptor> descriptors = admin.listTableDescriptorsByNamespace(Bytes.toBytes(space));
        for (TableDescriptor descriptor: descriptors) {
            String name = descriptor.getTableName().getNameAsString();
//            if (name.contains(":")) {
//                name = name.split(":")[1];
//            }
            tables.add(name);
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

    public Map<String, Object> getColumnFamilies(String name) throws IOException {
        TableName tableName = TableName.valueOf(name);
        TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
        assert name.equals(tableDescriptor.getTableName().getNameAsString());
        Map<String, Object> cfMap = new HashMap<>();
        for (ColumnFamilyDescriptor cf : tableDescriptor.getColumnFamilies()) {
            Map<String, Object> cfDetails = new HashMap<>();
            cfDetails.put("blockCache", cf.isBlockCacheEnabled());
            cfDetails.put("blockSize", cf.getBlocksize());
            cfDetails.put("bloomFilter", cf.getBloomFilterType());
            cfDetails.put("compression", cf.getCompressionType().name());
            cfDetails.put("dataBlockEncoding", cf.getDataBlockEncoding());
            cfDetails.put("inMemory", cf.isInMemory());
            cfDetails.put("keepDeleteCells", cf.getKeepDeletedCells());
            cfDetails.put("minVersions", cf.getMinVersions());
            cfDetails.put("scope", cf.getScope());
            cfDetails.put("ttl", cf.getTimeToLive());
            cfDetails.put("versions", cf.getMaxVersions());
            cfMap.put(Bytes.toString(cf.getName()), cfDetails);
        }
        return cfMap;
    }

    void createTable(String name, Map<String, Object> cfMap, boolean overwrite) throws IOException {
        TableName tableName = TableName.valueOf(name);
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        for (Map.Entry<String, Object> entry : cfMap.entrySet()) {
            String cfName = entry.getKey();
            Map<String, Object> cfDetails = (Map<String, Object>) entry.getValue();
            ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(cfName.getBytes());
            cfBuilder.setBlockCacheEnabled(((Boolean) cfDetails.get("blockCache")));
            cfBuilder.setBlocksize((Integer) cfDetails.get("blockSize"));
            cfBuilder.setBloomFilterType(BloomType.valueOf((String) cfDetails.get("bloomFilter")));
            cfBuilder.setCompressionType(Compression.Algorithm.valueOf((String) cfDetails.get("compression")));
            cfBuilder.setDataBlockEncoding(DataBlockEncoding.valueOf((String) cfDetails.get("dataBlockEncoding")));
            cfBuilder.setInMemory((Boolean) cfDetails.get("inMemory"));
            cfBuilder.setKeepDeletedCells(KeepDeletedCells.valueOf((String) cfDetails.get("keepDeleteCells")));
            cfBuilder.setMinVersions((Integer) cfDetails.get("minVersions"));
            cfBuilder.setScope((Integer) cfDetails.get("scope"));
            cfBuilder.setTimeToLive((Integer) cfDetails.get("ttl"));
            cfBuilder.setMaxVersions((Integer) cfDetails.get("versions"));
            tableDescriptorBuilder.setColumnFamily(cfBuilder.build());
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        if (admin.tableExists(tableName)) {
            if (!overwrite) {
                LOG.info("table({}) already exists", name);
                return;
            }
            LOG.info("remove existed table({})", name);
            dropTable(name);
        }
        admin.createTable(tableDescriptor);
    }

    public void createTable(String jsonFile) throws IOException {
        String jsonStr = new String(Files.readAllBytes(Paths.get(jsonFile)), StandardCharsets.UTF_8);
        Map<String, Object> jsonObj = JSON.parseObject(jsonStr, Map.class);
        createTable((String) jsonObj.get("tableName"),
                (Map<String, Object>) jsonObj.get("columnFamilies"),
                (boolean) jsonObj.get("overwrite"));
    }

    public void createTable(String name, String family) throws IOException {
        TableName tableName = TableName.valueOf(name);    // qualified name
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

    public void putCell(String name, byte[] key, byte[] family, byte[] qualifier, byte[] value)
            throws IOException {
        TableName tableName = TableName.valueOf(name);    // qualified name
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

    public void deleteCell(String name, byte[] key, byte[] family) throws IOException {
        TableName tableName = TableName.valueOf(name);    // qualified name
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

    public long countRows(String tableName) throws IOException, InterruptedException, ClassNotFoundException {
        // Optional: use local mode to avoid HDFS
//        conf.set("mapreduce.framework.name", "local");
//        conf.set("fs.defaultFS", "file:///");

        String[] args = new String[] { tableName };
        Job job = createSubmittableJob(conf, args);

        boolean success = job.waitForCompletion(true);
        if (!success) {
            throw new RuntimeException("RowCounter job failed");
        }

        // 用字符串名获取 Counter，避免访问不可见枚举类
        Counters counters = job.getCounters();
        return counters.findCounter(
                "org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters",
                "ROWS").getValue();
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

    public List<String> listSnapshots(String table) throws IOException {
        List<String> snapshots = new ArrayList<>();
        List<SnapshotDescription> descriptions = admin.listSnapshots();
        for (SnapshotDescription descriptor : descriptions) {
            if (table == null || descriptor.getTableName().getNameAsString().equals(table)) {
                snapshots.add(descriptor.getName());
            }
        }
        LOG.debug("snapshots: {}", snapshots);
        return snapshots;
    }

    public boolean snapshotExists(String snapshotName) throws IOException {
        List<SnapshotDescription> snapshots = admin.listSnapshots();
        return snapshots.stream().anyMatch(snapshot -> snapshot.getName().equals(snapshotName));
    }

   public void createSnapshot(String tableName, String snapshotName) throws IOException {
        TableName table = TableName.valueOf(tableName);
        if (!admin.tableExists(table)) {
            LOG.error("table({}) doesn't exist", tableName);
            return;
        }
        if (snapshotExists(snapshotName)) {
            admin.deleteSnapshot(snapshotName);
            LOG.warn("deleted old snapshot({})", snapshotName);
        }
        admin.snapshot(snapshotName, table, new HashMap<>());
    }

    public void cloneSnapshot(String snapshotName, String tableName) throws IOException {
        if (!snapshotExists(snapshotName)) {
            LOG.error("snapshot({}) doesn't exist", snapshotName);
            return;
        }
        Pair<String, String> pair = tableName(tableName);
        String namespace = pair.getFirst();
        if (!listNameSpaces().contains(namespace)) {
            LOG.warn("namespace({}) doesn't exist, creating...", namespace);
            createNamespace(namespace);
        }
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            if (admin.isTableEnabled(table)) {
                admin.disableTable(table);
            }
            admin.deleteTable(table);
            LOG.warn("deleted old table({})", table);
        }
        admin.cloneSnapshot(snapshotName, table);
    }

    public void deleteSnapshot(String snapshotName) throws IOException {
        if (!snapshotExists(snapshotName)) {
            LOG.error("snapshot({}) doesn't exist", snapshotName);
            return;
        }
        admin.deleteSnapshot(snapshotName);
    }

    static public void distcpSnapshot(Configuration conf, String snapshotName, String copyFrom, String copyTo)
            throws Exception {
        Path sourcePath = new Path(copyFrom + "/.hbase-snapshot/" + snapshotName);
        Path targetPath = new Path(copyTo + "/.hbase-snapshot/" + snapshotName);
        LOG.info("Copying snapshot from {} to {}", sourcePath, targetPath);

        List<Path> srcPaths = Collections.singletonList(sourcePath);
        DistCpOptions options = new DistCpOptions.Builder(srcPaths, targetPath)
                .withSyncFolder(true)
                .withDeleteMissing(true)
                .build();

        DistCp distCp = new DistCp(conf, options);
        Job job = distCp.execute();
        if (job.isSuccessful()) {
            LOG.info("DistCp job {} completed successfully", job.getJobID());
            LOG.info("Tracking URL: {}", job.getTrackingURL());
        } else {
            LOG.error("DistCp job {} failed", job.getJobID());
            LOG.error("Reason: {}", job.getStatus().getFailureInfo());
            throw new RuntimeException("DistCp failed");
        }
    }

    static public int exportSnapshot(Configuration conf, String snapshotName, String copyFrom, String copyTo)
            throws Exception {
        List<String> opts = new ArrayList<>();
        opts.add("--snapshot");
        opts.add(snapshotName);
        opts.add("--copy-from");
        opts.add(copyFrom);
        opts.add("--copy-to");
        opts.add(copyTo);
        opts.add("--overwrite");
        LOG.info("export --snapshot {} --copy-from {} --copy-to {} --overwrite", snapshotName, copyFrom, copyTo);
        int rc = ToolRunner.run(conf, new ExportSnapshot(), opts.toArray(new String[0]));
        LOG.info("exported rc({})", rc);
        return rc;
    }

    public int exportSnapshot(String snapshotName, String copyFrom, String copyTo) throws Exception {
        return exportSnapshot(conf, snapshotName, copyFrom, copyTo);
    }

    public void renameTable(String name, String newName) throws IOException {
        String snapshotName = name.replaceFirst(":", "-") + "_" + "snapshot";
        TableName table = TableName.valueOf(name);
        TableName newTable = TableName.valueOf(newName);
        if (!admin.tableExists(table)) {
            LOG.error("table({}) doesn't exist", name);
            return;
        }
        if (admin.isTableEnabled(table)) {
            admin.disableTable(table);
        }
        if (snapshotExists(snapshotName)) {
            admin.deleteSnapshot(snapshotName);
            LOG.warn("deleted old snapshot({})", snapshotName);
        }
        admin.snapshot(snapshotName, table, new HashMap<>());
        if (admin.tableExists(newTable)) {
            if (admin.isTableEnabled(newTable)) {
                admin.disableTable(newTable);
            }
            admin.deleteTable(newTable);
            LOG.warn("deleted old table({})", newTable);
        }
        admin.cloneSnapshot(snapshotName, newTable);
        admin.deleteSnapshot(snapshotName);
        admin.deleteTable(table);
    }
}