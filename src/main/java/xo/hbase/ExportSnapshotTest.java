package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportSnapshotDirect {
    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotDirect.class);

    public static void main(String[] args) throws Exception {
        String snapshot = "manga-fruit_snapshot";
        String table = "manga:fruit";
        String srcPath = "hb_u";
        String tgtPath = "hb_mrs";
        String zPrincipal = "zookeeper/hadoop.hadoop.com";
        String principal = "loader_hive1@HADOOP.COM";
        String keytab = "hb_mrs/loader_hive1.keytab";

        Configuration conf = HBase.loadConf(tgtPath, zPrincipal, false);
        HBase.login(conf, principal, keytab);

        HBase srcDb = new HBase(srcPath, null, null, null, false);
        String copyFrom = srcDb.getProperty("hbase.rootdir");
        srcDb.createSnapshot(table, snapshot);
        srcDb.close();
        conf = HBase.loadConf(tgtPath, zPrincipal, true);
        String copyTo = conf.get("hbase.rootdir");
        HBase.exportSnapshot(conf, snapshot, copyFrom, copyTo);
        HBase tgtDb = new HBase(tgtPath, zPrincipal, false);
        tgtDb.cloneSnapshot(snapshot, table);
        tgtDb.close();
    }
}