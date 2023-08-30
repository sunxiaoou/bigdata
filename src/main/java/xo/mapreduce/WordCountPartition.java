package xo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static xo.mapreduce.WordCount.delDir;
import static xo.mapreduce.WordCount.readFile;

public class WordCountPartition extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountPartition.class);

    static class MyPartitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text text, LongWritable longWritable, int i) {
            if (text.getLength() <= 5) {
                return 0;
            }
            return 1;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "WordCountPartition");
        job.setJarByClass(WordCountPartition.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(WordCount.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setPartitionerClass(MyPartitioner.class);

        job.setReducerClass(WordCount.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(2);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0: 1;
    }

    private static int mr() throws Exception {
        String base = System.getProperty("user.dir");
        String input = base + "/mapreduce/wordcount/input";
        String output = base + "/mapreduce/wordcount/partition";
        delDir(output);
        Configuration conf = new Configuration();
        int rc = ToolRunner.run(conf, new WordCountPartition(), new String[]{"file:///" + input, "file:///" + output});
        LOG.info(readFile(output + "/part-r-00000"));
        LOG.info(readFile(output + "/part-r-00001"));
        return rc;
    }

    public static void main(String[] args) throws Exception {
        LOG.info(String.valueOf(mr()));
    }
}
