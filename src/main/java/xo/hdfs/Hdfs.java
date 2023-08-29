package xo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Hdfs {
    private static final Logger LOG = LoggerFactory.getLogger(Hdfs.class);
    FileSystem fileSystem;

    public Hdfs(Configuration conf) throws IOException {
        fileSystem = FileSystem.get(conf);
    }

    public Hdfs(String host, int port) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", String.format("hdfs://%s:%d", host, port));
        fileSystem = FileSystem.get(conf);
    }

    public Hdfs(String host, int port, String user) throws IOException {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        UserGroupInformation.setLoginUser(ugi);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", String.format("hdfs://%s:%d", host, port));
        fileSystem = FileSystem.get(conf);
    }

    public void close() throws IOException {
        fileSystem.close();
    }

    public void createFile(String filePath, String text) throws IOException {
        java.nio.file.Path path = java.nio.file.Paths.get(filePath);
        Path dir = new Path(path.getParent().toString());
        fileSystem.mkdirs(dir);
        OutputStream os = fileSystem.create(new Path(dir, path.getFileName().toString()));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os));
        writer.write(text);
        writer.close();
        os.close();
    }

    // hdfs dfs -text filePath
    public String readFile(String filePath) throws IOException {
        InputStream in = fileSystem.open(new Path(filePath));
        byte[] buffer = new byte[256];
        int bytesRead = in.read(buffer);
        return new String(buffer, 0, bytesRead);
    }

    // hdfs dfs -rm -r -f filePath
    public boolean delFile(String filePath) throws IOException {
        return fileSystem.delete(new Path(filePath), true);
    }

    private static void wordCount(Hdfs fs) throws IOException {
        String text = "apache, http, hadoop, hadoop, sqoop, hue, mapreduce, sqoop, oozie, hbase, http";
        String path = "wordcount/input/wc.txt";
        fs.createFile(path, text);
        LOG.info("read: " + fs.readFile(path));
    }

    public static void main(String[] args) throws IOException {
        Hdfs fs = new Hdfs("192.168.55.250", 8020, "sunxo");
//        fs.createFile("/tmp/output/hello.txt", "Hello Hadoop");
//        LOG.info(fs.readFile("/tmp/output/hello.txt"));
//        LOG.debug("delete - {}", fs.delFile("/tmp/output/myfile.txt"));
//        wordCount(fs);
//        LOG.info(String.valueOf(fs.delFile("wordcount/output")));
        LOG.info(fs.readFile("wordcount/output/part-r-00000"));
        fs.close();
    }
}
