package xo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCount extends Configured implements Tool {
    final static String input = "wordcount/input";
    final static String output = "wordcount/output";

    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text text = new Text();
            LongWritable longWritable = new LongWritable();
            String[] words = value.toString().split(", ");
            for (String word: words) {
                text.set(word);
                longWritable.set(1);
                context.write(text, longWritable);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value: values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "WordCount");
        job.setJarByClass(WordCount.class);

        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("file:///" + base + "/input"));
        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job, new Path("file:///" + base + "/output"));
        TextOutputFormat.setOutputPath(job, new Path(output));

        boolean b = job.waitForCompletion(true);
        return b ? 0: 1;
    }

    // $ mvn clean package
    // $ hadoop jar target/bigdata-1.0-SNAPSHOT.jar xo.mapreduce.WordCount
    public static void main(String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("sunxo");
        UserGroupInformation.setLoginUser(ugi);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ubuntu:8020");
        conf.set("yarn.resourcemanager.hostname", "ubuntu");

        int rc = ToolRunner.run(conf, new WordCount(), args);
        System.exit(rc);
    }
}
