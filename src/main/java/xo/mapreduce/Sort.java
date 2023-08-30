package xo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class Sort extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(Sort.class);

    static class MyMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(", ");
            SortBean bean = new SortBean();
            bean.setWord(split[0]);
            bean.setNum(Integer.parseInt(split[1]));
            context.write(bean, NullWritable.get());
        }
    }

    static class SortBean implements WritableComparable<SortBean> {
        private String word;
        private int num;

        public void setWord(String word) {
            this.word = word;
        }

        public void setNum(int num) {
            this.num = num;
        }

        public String getWord() {
            return word;
        }

        public int getNum() {
            return num;
        }

        @Override
        public String toString() {
            return word + ", " + num;
        }

        @Override
        public int compareTo(SortBean bean) {
            int result = this.word.compareTo(bean.word);
            if (result == 0) {
                result = this.num - bean.num;
            }
            return result;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(word);
            out.writeInt(num);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.word = in.readUTF();
            this.num = in.readInt();
        }
    }

    static class MyReducer extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {
        @Override
        protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "Sort");
        job.setJarByClass(Sort.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);

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

    private static int mr() throws Exception {
        String base = System.getProperty("user.dir");
        String input = base + "/mapreduce/sort/input";
        String output = base + "/mapreduce/sort/output";
        delDir(output);
        Configuration conf = new Configuration();
        int rc = ToolRunner.run(conf, new Sort(), new String[]{"file:///" + input, "file:///" + output});
        LOG.info(readFile(output + "/part-r-00000"));
        return rc;
    }

    public static void main(String[] args) throws Exception {
        LOG.info(String.valueOf(mr()));
    }
}
