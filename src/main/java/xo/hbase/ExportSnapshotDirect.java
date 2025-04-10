package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

public class ExportSnapshotDirect {
    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotDirect.class);

    public static boolean snapshotExists(String snapshotName, Admin admin) throws IOException {
        List<SnapshotDescription> snapshots = admin.listSnapshots();
        return snapshots.stream().anyMatch(snapshot -> snapshot.getName().equals(snapshotName));
    }

    public static String getProviderName(Configuration conf) throws IOException {
        SaslClientAuthenticationProviders providers = SaslClientAuthenticationProviders.getInstance(conf);
        Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> provider =
                providers.selectProvider(conf.get("hbase.cluster.id", "default"), User.getCurrent());
        return provider.getFirst().getClass().getSimpleName();
    }

    public static String createSnapshot(String confDir, String snapshotName, TableName tableName) throws IOException {
        Configuration srcConf = HBaseConfiguration.create();
        srcConf.addResource(confDir + "/core-site.xml");
        srcConf.addResource(confDir + "/hdfs-site.xml");
        srcConf.addResource(confDir + "/hbase-site.xml");

        try (Connection srcConn = ConnectionFactory.createConnection(srcConf);
             Admin admin = srcConn.getAdmin()) {
            if (snapshotExists(snapshotName, admin)) {
                LOG.warn("Snapshot {} already exists in source cluster, deleting...", snapshotName);
                admin.deleteSnapshot(snapshotName);
            }
            admin.snapshot(snapshotName, tableName);
            LOG.info("Snapshot '{}' created for table '{}'", snapshotName, tableName);
        }
//        LOG.info("Authentication provider '{}'", getProviderName(srcConf));
        return srcConf.get("fs.defaultFS") + "/hbase";
    }

    public static Configuration login(String confDir, String principal, String keytab) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(confDir + "/core-site.xml");
        conf.addResource(confDir + "/hdfs-site.xml");
        conf.addResource(confDir + "/mapred-site.xml");
        conf.addResource(confDir + "/yarn-site.xml");
        conf.addResource(confDir + "/hbase-site.xml");

        conf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
        conf.setBoolean("dfs.client.use.legacy.blockreader", true);
        conf.setBoolean("dfs.client.read.shortcircuit", false);

        UserGroupInformation.reset();
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        LOG.info("Logged in as '{}' with '{}'", UserGroupInformation.getLoginUser(), getProviderName(conf));
        return conf;
    }

    public static int exportSnapshot(Configuration conf, String snapshotName, String copyFrom)
            throws Exception {
        String[] exportArgs = new String[]{
                "--snapshot", snapshotName,
                "--copy-from", copyFrom,
                "--copy-to", conf.get("fs.defaultFS") + "/hbase",
                "--overwrite"
        };

        int result = ToolRunner.run(conf, new ExportSnapshot(), exportArgs);
        if (result != 0) {
            LOG.error("ExportSnapshot failed with rc={}", result);
            throw new IOException("ExportSnapshot failed with rc=" + result);
        }
        return result;
    }

    public static void cloneSnapshot(Configuration conf, String snapshotName, TableName tableName)
            throws IOException, InterruptedException {
        UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Void>) () -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {

                if (admin.tableExists(tableName)) {
                    LOG.warn("Table {} already exists in target cluster, deleting...", tableName);
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }
                admin.cloneSnapshot(snapshotName, tableName);
                LOG.info("Cloned snapshot '{}' to table '{}'", snapshotName, tableName);
            }
            return null;
        });
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Usage: ExportSnapshotDirect <snapshotName> <tableName> <sourceConfDir> <targetConfDir> <principal> <keytab>");
            System.exit(1);
        }

        String snapshotName = args[0];
        TableName tableName = TableName.valueOf(args[1]);
        String sourceConfDir = args[2];
        String targetConfDir = args[3];
        String principal = args[4];
        String keytab = args[5];

        Configuration tgtConf = login(targetConfDir, principal, keytab);
        String copyFrom = createSnapshot(sourceConfDir, snapshotName, tableName);
        if (0 == exportSnapshot(tgtConf, snapshotName, copyFrom)) {
            cloneSnapshot(tgtConf, snapshotName, tableName);
        }
    }
}