package xo.hbase.bulkdatagenerator;

import org.junit.Test;

public class BulkDataGeneratorTest {
    private static final String confPath = "hb_u";
    private static final String tableName = "bulk";

    @Test
    public void createBulk() throws Exception {
        String[] args = {
                confPath,
                "-t", tableName,
                "-d",
                "-mc", "10",
                "-r", "100",
                "-sc", "10"};
        BulkDataGeneratorTool.main(args);
    }

    @Test
    public void appendBulk() throws Exception {
        String[] args = {
                confPath,
                "-t", tableName,
                "-mc", "10",
                "-r", "100",
                "-sc", "10"};
        BulkDataGeneratorTool.main(args);
    }
}