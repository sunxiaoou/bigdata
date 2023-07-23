package xo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Hdfs {
    private static final Logger LOG = LoggerFactory.getLogger(Hdfs.class);
    FileSystem fileSystem;

    public Hdfs(String host, int port) throws IOException {
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

    public String readFile(String filePath) throws IOException {
        InputStream in = fileSystem.open(new Path(filePath));
        byte[] buffer = new byte[256];
        int bytesRead = in.read(buffer);
        return new String(buffer, 0, bytesRead);
    }

    public boolean delFile(String filePath) throws IOException {
        return fileSystem.delete(new Path(filePath), false);
    }

    public static void main(String[] args) throws IOException {
        Hdfs fs = new Hdfs("192.168.55.250", 8020);
        fs.createFile("/tmp/output/hello.txt", "Hello Hadoop");
        LOG.info(fs.readFile("/tmp/output/hello.txt"));
        LOG.debug("delete - {}", fs.delFile("/tmp/output/myfile.txt"));
        fs.close();
    }
}
