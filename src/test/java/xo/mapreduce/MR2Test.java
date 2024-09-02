package xo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class MR2Test {
    private static final Logger LOG = LoggerFactory.getLogger(MR2Test.class);

    @Test
    public void wordCount() throws Exception {
        String base = System.getProperty("user.dir");
        String input = base + "/mapreduce/wordcount/input";
        String output = base + "/mapreduce/wordcount/output";
        WordCount.delDir(output);
        Configuration conf = new Configuration();
        int rc = ToolRunner.run(conf, new WordCount(), new String[]{"file:///" + input, "file:///" + output});
        assertEquals(0, rc);
        LOG.info(WordCount.readFile(output + "/part-r-00000"));
    }
}