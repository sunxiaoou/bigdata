package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Snapshot {
    private static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);
    private static String dbStr;
    private static String dbStr2;
    private static String zPrincipal = null;
    private static String principal = null;
    private static String keytab = null;

    private static String table;
    private static String action;

    private static void usage() {
        System.out.println("Usage: snapshot [options]");
        System.out.println("Options:");
        System.out.println("  --action <action>         Action to perform. One of:");
        System.out.println("                                - list");
        System.out.println("                                - create");
        System.out.println("                                - delete");
        System.out.println("                                - deleteAll");
        System.out.println("                                - export");
        System.out.println("                                - clone");
        System.out.println("  --db <srcDB>          HBase database.");
        System.out.println("  --db2 <tgtDB>         Target HBase database (required for export action).");
        System.out.println("  --zPrinciple <zPrinciple>   Zookeeper principal. (on target side in export action)");
        System.out.println("  --principle <principle>   HBase principal. (on target side in export action)");
        System.out.println("  --keytab <keytab>         HBase keytab. (on target side in export action)");
        System.out.println("  --table <table>       Table name (not required for list|deleteAll action).");
        System.out.println("  --help                Show this help message.");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  List snapshots:       snapshot --action list --db dbStr");
        System.out.println("  Create a snapshot:    snapshot --action create --db dbStr --table table");
        System.out.println("  Delete a snapshot:    snapshot --action delete --db dbStr --table table");
        System.out.println("  Delete all snapshots:    snapshot --action deleteAll --db dbStr");
        System.out.println("  Export a snapshot:    snapshot --action export --db srcDb --db2 tgtDb --table table");
        System.out.println("  Clone a snapshot:     snapshot --action clone --db dbStr --table table");
    }

    private static void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--db":
                    dbStr = args[++i];
                    break;
                case "--db2":
                    dbStr2 = args[++i];
                    break;
                case "--zPrincipal":
                    zPrincipal = args[++i];
                    break;
                case "--principal":
                    principal = args[++i];
                    break;
                case "--keytab":
                    keytab = args[++i];
                    break;
                case "--table":
                    table = args[++i];
                    break;
                case "--action":
                    action = args[++i];
                    break;
                default:
                    LOG.error("Unknown argument: " + args[i]);
                    usage();
                    System.exit(1);
            }
        }

        if (dbStr == null || action == null) {
            LOG.error("db and action is required.");
            usage();
            System.exit(1);
        }
    }

    private static void performAction() {
//        HBase db = null;
        String snapshot = null;
        if (table != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
            String dateStr = sdf.format(new Date());
            snapshot = table.replaceFirst(":", "-") + "_" + dateStr;
        }

        try {
            if ("distcp".equals(action) || "export".equals(action)) {
                String copyFrom = HBase.loadConf(dbStr).get("hbase.rootdir");
//                db = new HBase(dbStr2, zPrincipal, principal, keytab, true);
                Configuration conf;
                if (principal != null && keytab != null) {
                    conf = HBase.loadConf(dbStr2, zPrincipal, true);
                    HBase.login(conf, principal, keytab);
                } else {
                    conf = HBase.loadConf(dbStr2);
                    HBase.changeUser(HBase.getUser(conf));
                }
                String copyTo = conf.get("hbase.rootdir");

                if ("distcp".equals(action)) {
                    HBase.distcpSnapshot(conf, snapshot, copyFrom, copyTo);
                    LOG.info("Snapshot {} distcp to {} successfully.", snapshot, copyTo);
                } else {
                    if (HBase.exportSnapshot(conf, snapshot, copyFrom, copyTo) == 0) {
                        LOG.info("Snapshot {} exported successfully.", snapshot);
                    } else {
                        LOG.warn("Failed to export snapshot {}.", snapshot);
                    }
                }
                return;
            }

            HBase db = new HBase(dbStr, zPrincipal, principal, keytab, false);
            switch (action) {
                case "list":
                    LOG.info("Snapshots: {}", db.listSnapshots(table));
                    break;
                case "create":
                    assert snapshot != null;
                    db.createSnapshot(table, snapshot);
                    LOG.info("Snapshot {} created successfully for table {}.", snapshot, table);
                    break;
                case "delete":
                    db.deleteSnapshot(snapshot);
                    LOG.info("Snapshot {} deleted successfully.", snapshot);
                    break;
                case "deleteAll":
                    for (String s: db.listSnapshots(table)) {
                        db.deleteSnapshot(s);
                        LOG.info("Snapshot(s) {} deleted successfully.", s);
                    }
                    break;
                case "clone":
                    db.cloneSnapshot(snapshot, table);
                    LOG.info("Snapshot {} cloned to table {}.", snapshot, table);
                    break;
                default:
                    LOG.warn("Invalid action: {}", action);
            }
            db.close();
        } catch (Exception e) {
            LOG.error("Error closing databases:", e);
        }
    }

    public static void main(String[] args) {
        parseArgs(args);
        performAction();
    }
}
