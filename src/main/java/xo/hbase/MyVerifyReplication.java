package xo.hbase;

import org.apache.hadoop.conf.Configuration;
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