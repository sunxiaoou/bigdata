package xo.mapreduce;

import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.hdfs.Hdfs;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class MRTest {
    private static final Logger LOG = LoggerFactory.getLogger(MRTest.class);

    private static final String host = "ubuntu";
    private static final String user = "sunxo";
    private static Hdfs hdfs;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        hdfs = new Hdfs(host, user);
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        hdfs.close();
    }

    @Test
    public void wordCount() throws Exception {
        String input = "wordcount/input";
        String output = "wordcount/output";
        hdfs.delFile(output);
        int rc = ToolRunner.run(hdfs.getConf(), new WordCount(), new String[]{input, output});
        assertEquals(rc, 0);
        LOG.info(hdfs.readFile(output + "/part-r-00000"));
    }
}