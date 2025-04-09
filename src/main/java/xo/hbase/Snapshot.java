package xo.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Snapshot {
    private static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);
    private static String dbStr;
    private static String dbStr2;
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
        HBase db;
        HBase db2 = null;
        String snapshot = null;
        try {
            if ("export".equals(action)) {
                db = new HBase(dbStr, null, null);
            } else {
                db = new HBase(dbStr, principal, keytab);
            }
            if (dbStr2 != null) {
                db2 = new HBase(dbStr2, principal, keytab);
            }
            if (table != null) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
                String dateStr = sdf.format(new Date());
                snapshot = table.replaceFirst(":", "-") + "_" + dateStr;
            }

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
                        LOG.info("Snapshot {} deleted successfully.", s);
                    }
                    break;
                case "export":
                    assert db2 != null;
                    String copyFrom = db.getProperty("hbase.rootdir");
                    String copyTo = db2.getProperty("hbase.rootdir");
                    String auth = db2.getProperty("hbase.security.authentication");
                    if (auth != null && auth.equals("kerberos")) {
                        db2.setFallback(true);
                    } else {
                        HBase.changeUser(db2.getUser());
                    }
                    if (db2.exportSnapshot(snapshot, copyFrom, copyTo) == 0) {
                        LOG.info("Snapshot {} exported successfully.", snapshot);
                    } else {
                        LOG.warn("Failed to export snapshot {}.", snapshot);
                    }
                    break;
                case "clone":
                    assert snapshot != null;
                    db.cloneSnapshot(snapshot, table);
                    LOG.info("Snapshot {} cloned successfully to table {}.", snapshot, table);
                    break;
                default:
                    LOG.warn("Invalid action: {}", action);
            }

            db.close();
            if (db2 != null) {
                db2.close();
            }
        } catch (Exception e) {
            LOG.error("Error closing databases:", e);
        }
    }

    public static void main(String[] args) {
        parseArgs(args);
        performAction();
    }
}
