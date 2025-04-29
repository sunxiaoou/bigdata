package xo.hbase;

import org.junit.Test;

public class SnapshotTest {
    @Test
    public void listSnapshots() {
        String[] args = {
                "--action", "list",
                "--db", "hb_u"};
        Snapshot.main(args);
    }

    @Test
    public void listTableSnapshots() {
        String[] args = {
                "--action", "list",
                "--db", "hb_c3",
                "--zPrincipal", "zookeeper/centos3@EXAMPLE.COM",
                "--principal", "hbase/centos3@EXAMPLE.COM",
                "--keytab", "hb_c3/hadoop.keytab",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void listTableSnapshots2() {
        String[] args = {
                "--action", "list",
                "--db", "hb_mrs",
                "--zPrincipal", "zookeeper/hadoop.hadoop.com",
                "--principal", "loader_hive1@HADOOP.COM",
                "--keytab", "hb_mrs/loader_hive1.keytab",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void createSnapshot() {
        String[] args = {
                "--action", "create",
                "--db", "hb_u",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void deleteSnapshot() {
        String[] args = {
                "--action", "delete",
                "--db", "hb_u",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void deleteAllSnapshots() {
        String[] args = {
                "--action", "deleteAll",
                "--db", "hb_c2"};
        Snapshot.main(args);
    }

    @Test
    public void deleteAllSnapshots2() {
        String[] args = {
                "--action", "deleteAll",
                "--zPrincipal", "zookeeper/centos3@EXAMPLE.COM",
                "--principal", "hbase/centos3@EXAMPLE.COM",
                "--keytab", "hb_c3/hadoop.keytab",
                "--db", "hb_c3"};
        Snapshot.main(args);
    }

    @Test
    public void distcpSnapshot() {
        String[] args = {
                "--action", "distcp",
                "--db", "hb_u",
                "--db2", "hb_c2",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void distcpSnapshot2() {
        String[] args = {
                "--action", "distcp",
                "--db", "hb_u",
                "--db2", "hb_c3",
                "--zPrincipal", "zookeeper/centos3@EXAMPLE.COM",
                "--principal", "hbase/centos3@EXAMPLE.COM",
                "--keytab", "hb_c3/hadoop.keytab",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void distcpSnapshot3() {
        String[] args = {
                "--action", "distcp",
                "--db", "hb_u",
                "--db2", "hb_mrs",
                "--zPrincipal", "zookeeper/hadoop.hadoop.com",
                "--principal", "loader_hive1@HADOOP.COM",
                "--keytab", "hb_mrs/loader_hive1.keytab",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void exportSnapshot() {
        String[] args = {
                "--action", "export",
                "--db", "hb_u",
                "--db2", "hb_c2",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void exportSnapshot2() {
        String[] args = {
                "--action", "export",
                "--db", "hb_u",
                "--db2", "hb_mrs",
                "--zPrincipal", "zookeeper/hadoop.hadoop.com",
                "--principal", "loader_hive1@HADOOP.COM",
                "--keytab", "hb_mrs/loader_hive1.keytab",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void cloneSnapshot() {
        String[] args = {
                "--action", "clone",
                "--db", "hb_c2",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void cloneSnapshot2() {
        String[] args = {
                "--action", "clone",
                "--db", "hb_c3",
                "--zPrincipal", "zookeeper/centos3@EXAMPLE.COM",
                "--principal", "hbase/centos3@EXAMPLE.COM",
                "--keytab", "hb_c3/hadoop.keytab",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }

    @Test
    public void cloneSnapshot3() {
        String[] args = {
                "--action", "clone",
                "--db", "hb_mrs",
                "--zPrincipal", "zookeeper/hadoop.hadoop.com",
                "--principal", "loader_hive1@HADOOP.COM",
                "--keytab", "hb_mrs/loader_hive1.keytab",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }
}