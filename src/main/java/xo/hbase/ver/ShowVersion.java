package xo.hbase.ver;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ShowVersion {
    private static final Logger LOG = LoggerFactory.getLogger(ShowVersion.class);

    static class HBase implements AutoCloseable {
        private final Configuration conf;
        private final Connection conn;
        private final Admin admin;

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
            LOG.info("default file system: {}", conf.get("fs.defaultFS"));
            return conf;
        }

        static Configuration loadConf(String pathStr, String zPrincipal, boolean fallback) throws IOException {
            Configuration conf = loadConf(pathStr);
            if (Files.isReadable(Paths.get(pathStr + "/krb5.conf"))) {
                System.setProperty("java.security.krb5.conf", pathStr + "/krb5.conf");
                LOG.info("java.security.krb5.conf: {}", System.getProperty("java.security.krb5.conf"));
            }
            if (zPrincipal != null && !zPrincipal.isEmpty()) {
                System.setProperty("zookeeper.server.principal", zPrincipal);
            }
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

        static public void changeUser(String user) throws IOException {
            String current = UserGroupInformation.getCurrentUser().getShortUserName();
            if (!current.equals(user)) {
                UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
                UserGroupInformation.setLoginUser(ugi);
                LOG.info("changed user from {} to {}", current, user);
            }
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

        public String getVersion() throws IOException {
            return admin.getClusterMetrics().getHBaseVersion();
        }

        public void close() throws IOException {
            admin.close();
            conn.close();
        }
    }

    public static void main(String[] args) throws IOException {
//        -h doc/HBase/hbk_c5
//        -z zookeeper/centos5@EXAMPLE.COM
//        -p hbase/centos5@EXAMPLE.COM
//        -k hadoop.keytab

        Options options = new Options();
        options.addOption(Option.builder("h")
                .longOpt("confPath")
                .hasArg()
                .required()
                .desc("HBase configuration path").build());
        options.addOption(Option.builder("z")
                .longOpt("zPrincipal")
                .hasArg()
                .desc("Zookeeper Principal name").build());
        options.addOption(Option.builder("p")
                .longOpt("principal")
                .hasArg()
                .required()
                .desc("Principal name").build());
        options.addOption(Option.builder("k")
                .longOpt("keytab")
                .hasArg()
                .required()
                .desc("keytab path").build());
        options.addOption(Option.builder("?")
                .longOpt("help")
                .desc("Show help").build());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            formatter.printHelp("ShowVersion", options);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("help")) {
            formatter.printHelp("ShowVersion", options);
            System.exit(0);
        }

        String confPath = cmd.getOptionValue("confPath");
        String zPrincipal = cmd.getOptionValue("zPrincipal");
        String principal = cmd.getOptionValue("principal");
        String keytab = cmd.getOptionValue("keytab");

        try (HBase db = new HBase(confPath, zPrincipal, principal, confPath + "/" + keytab, true)) {
            LOG.info("version({})", db.getVersion());
            System.out.println("HBase version: " + db.getVersion());
        } catch (IOException e) {
            LOG.error("Error getting HBase version", e);
        }
    }
}
