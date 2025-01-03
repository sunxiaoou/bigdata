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
                "--db", "hb_c2",
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
                "--db", "hb_u"};
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
    public void cloneSnapshot() {
        String[] args = {
                "--action", "clone",
                "--db", "hb_c2",
                "--table", "manga:fruit"};
        Snapshot.main(args);
    }
}