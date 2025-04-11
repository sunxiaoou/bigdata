package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
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

    public static Configuration login(String confDir, String principal, String keytab) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(confDir + "/core-site.xml");
        conf.addResource(confDir + "/hdfs-site.xml");
        conf.addResource(confDir + "/mapred-site.xml");
        conf.addResource(confDir + "/yarn-site.xml");
        conf.addResource(confDir + "/hbase-site.xml");

        conf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
        conf.set("mapreduce.map.memory.mb", "1536");
        conf.set("mapred.child.java.opts", "-Xmx1024m");

        UserGroupInformation.reset();
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        LOG.info("Logged in as '{}'", UserGroupInformation.getLoginUser());
        String provider = getProviderName(conf);
        if (!"GssSaslClientAuthenticationProvider".equals(provider)) {
            throw new RuntimeException("Unsupported authentication provider: " + provider);
        }
        return conf;
    }

    public static String createSnapshot(String confDir, String snapshotName, TableName tableName) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(confDir + "/core-site.xml");
        conf.addResource(confDir + "/hdfs-site.xml");
        conf.addResource(confDir + "/hbase-site.xml");

        try (Connection srcConn = ConnectionFactory.createConnection(conf);
             Admin admin = srcConn.getAdmin()) {
            if (snapshotExists(snapshotName, admin)) {
                LOG.warn("Snapshot {} already exists in source cluster, deleting...", snapshotName);
                admin.deleteSnapshot(snapshotName);
            }
            admin.snapshot(snapshotName, tableName);
            LOG.info("Snapshot '{}' created for table '{}'", snapshotName, tableName);
        }
        return conf.get("hbase.rootdir");
    }

    public static void distcpSnapshot(Configuration conf, String snapshotName, String copyFrom)
            throws Exception {
        String copyTo = conf.get("hbase.rootdir");
        Path sourcePath = new Path(copyFrom + "/.hbase-snapshot/" + snapshotName);
        Path targetPath = new Path(copyTo + "/.hbase-snapshot/" + snapshotName);
        LOG.info("Copying snapshot from {} to {}", sourcePath, targetPath);

        List<Path> srcPaths = Collections.singletonList(sourcePath);
        DistCpOptions options = new DistCpOptions(srcPaths, targetPath);
        options.setSyncFolder(true);
        options.setDeleteMissing(true);

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
        distcpSnapshot(tgtConf, snapshotName, copyFrom);
        cloneSnapshot(tgtConf, snapshotName, tableName);
    }
}