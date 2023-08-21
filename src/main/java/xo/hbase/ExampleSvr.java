package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;


public class ExampleSvr {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleSvr.class);

    private static final Configuration CONF = HBaseConfiguration.create();
    private static final BlockingService SERVICE =
            ExampleProto.RowCountService.newReflectiveBlockingService(new ExampleService());

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration(CONF);
        NettyRpcServer rpcServer = new NettyRpcServer(null,
                "rpcSvr",
                Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
                new InetSocketAddress("localhost", 8813),
                conf,
                new FifoRpcScheduler(conf, 1),
                true
        );

        rpcServer.start();
        LOG.info("RPC server started on: " + rpcServer.getListenerAddress());

        // Keep the server running
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            rpcServer.stop();
            LOG.info("RPC server stopped.");
        }
    }
}
