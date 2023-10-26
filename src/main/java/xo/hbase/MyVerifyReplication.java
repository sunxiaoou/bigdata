package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MyVerifyReplication extends VerifyReplication {

    @Override
    public Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
        Job job = super.createSubmittableJob(conf, args);
        if (job != null) {
            String peer = job.getConfiguration().get("verifyrep.peerQuorumAddress");
            String[] s = peer.split(":");
            job.getConfiguration().set("verifyrep.peerQuorumAddress", String.format("%s:%s:/hbase", s[0], s[1]));
        }
        return job;
    }

    // java -cp ".:lib/*:target/bigdata-1.0-SNAPSHOT.jar" xo.hbase.MyVerifyReplication macos manga:fruit
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(HBaseConfiguration.create(), new MyVerifyReplication(), args);
        System.exit(res);
    }
}