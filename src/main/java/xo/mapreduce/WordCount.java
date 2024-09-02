package xo.mapreduce;

import org.apache.hadoop.mapreduce.Counter;
import xo.hdfs.Hdfs;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class WordCount extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    static public class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
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
            Counter counter = context.getCounter("MR_COUNTER", "myMapCounter");
            counter.increment(1L);      // count line of input file
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "WordCount");
//        job.setJarByClass(WordCount.class);
        job.setJar(System.getProperty("user.dir") + "/target/bigdata-1.0-SNAPSHOT.jar"); // for intellij idea

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0: 1;
    }

    static void delDir(String dir) throws IOException {
        java.nio.file.Path path = Paths.get(dir);
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted((p1, p2) -> -p1.compareTo(p2)) // Delete from bottom to top
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }

    static String readFile(String file) throws IOException {
        java.nio.file.Path path = Paths.get(file);
        byte[] fileBytes = Files.readAllBytes(path);
        return new String(fileBytes);
    }

    private static int mr(String host, int port, String user) throws Exception {
        String input = "wordcount/input";
        String output = "wordcount/output";

        Hdfs hdfs = new Hdfs(host, user);
        hdfs.delFile(output);
        int rc = ToolRunner.run(hdfs.getConf(), new WordCount(), new String[]{input, output});
        LOG.info(hdfs.readFile(output + "/part-r-00000"));
        hdfs.close();
        return rc;
    }

    public static void main(String[] args) throws Exception {
        LOG.info(String.valueOf(mr("ubuntu", 8020, "sunxo")));
    }
}