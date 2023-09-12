package xo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static xo.mapreduce.WordCount.delDir;
import static xo.mapreduce.WordCount.readFile;

public class WordCountCombiner extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountCombiner.class);

    static class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        // Same as WordCount.MyReducer.reduce()
        // then "Reduce input" == "Reduce output" in Map-Reduce Framework later
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value: values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "WordCountCombiner");
        job.setJarByClass(WordCountCombiner.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(WordCount.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setCombinerClass(MyCombiner.class);

        job.setReducerClass(WordCount.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0: 1;
    }

    private static int mr() throws Exception {
        String base = System.getProperty("user.dir");
        String input = base + "/mapreduce/wordcount/input";
        String output = base + "/mapreduce/wordcount/combiner";
        delDir(output);
        Configuration conf = new Configuration();
        int rc = ToolRunner.run(conf, new WordCountCombiner(), new String[]{"file:///" + input, "file:///" + output});
        LOG.info(readFile(output + "/part-r-00000"));
        return rc;
    }

    public static void main(String[] args) throws Exception {
        LOG.info(String.valueOf(mr()));
    }
}
