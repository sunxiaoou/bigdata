package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MyVerifyReplication extends VerifyReplication {

    /**
     * To verify replication serialization from HBase clusterA to RPCServer, we put WAL Entries to
     * another clusterB via Kafka, suppose both RPCServer and clusterB are on serverB, however the
     * peer cluster key of RPCServer looks like "serverB:2181:/myPeer", it should be changed to
     * "serverB:2181:/hbase" to point to clusterB so we can do verification
     */
    @Override
    public Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
        String root = conf.get("hbase.rootdir");
        assert root != null;
        FileSystem fs = FileSystem.get(conf);
        FileStatus fileStatus = fs.getFileStatus(new Path(root));
        HBase.changeUser(fileStatus.getOwner());

        Job job = super.createSubmittableJob(conf, args);
        if (job != null) {
            String peer = job.getConfiguration().get("verifyrep.peerQuorumAddress");
            String[] s = peer.split(":");
            job.getConfiguration().set("verifyrep.peerQuorumAddress", String.format("%s:%s:/hbase", s[0], s[1]));
        }
        return job;
    }

    public static Configuration createConfig(String path) {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(path + "/core-site.xml");
        conf.addResource(path + "/hdfs-site.xml");
        conf.addResource(path + "/mapred-site.xml");
        conf.addResource(path + "/yarn-site.xml");
        conf.addResource(path + "/hbase-site.xml");
        return conf;
    }

    // java -cp ".:lib/*:target/bigdata-1.0-SNAPSHOT.jar" xo.hbase.MyVerifyReplication hadoop3 hb_h2 manga:fruit
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: MyVerifyReplication confPath peerName tableName");
            System.exit(1);
        }
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        int res = ToolRunner.run(createConfig(args[0]), new MyVerifyReplication(), newArgs);
        System.out.println("MyVerifyReplication return " + res);
        System.exit(res);
    }
}