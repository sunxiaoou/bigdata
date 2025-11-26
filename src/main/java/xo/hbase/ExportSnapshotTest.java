package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class ExportSnapshotTest {
    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotTest.class);

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

//        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
//        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
//                    HBase srcDb = new HBase(srcPath, null, null, null, false);
//                    String copyFrom = srcDb.getProperty("hbase.rootdir");
//                    srcDb.createSnapshot(table, snapshot);
//                    srcDb.close();
//                    return null;
//                });
//        String copyFrom = "hdfs://ubuntu:8020/hbase";
//        conf = HBase.loadConf(tgtPath, zPrincipal, true);
//        String copyTo = conf.get("hbase.rootdir");
//        HBase.exportSnapshot(conf, snapshot, copyFrom, copyTo);
        Thread.sleep(2000);

        Thread thread = new Thread(() -> {
            try {
                HBase tgtDb = new HBase(tgtPath, zPrincipal, false);
                tgtDb.cloneSnapshot(snapshot, table);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        thread.join();
    }
}