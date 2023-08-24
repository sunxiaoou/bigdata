package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


public class ReplicateSvr {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateSvr.class);

    NettyRpcServer rpcServer;

    public ReplicateSvr(String host, int port) throws IOException {
        Configuration conf = new Configuration(HBaseConfiguration.create());
        BlockingService service =
                AdminProtos.AdminService.newReflectiveBlockingService(new ReplicateService());
        this.rpcServer = new NettyRpcServer(null,
                "rpcSvr",
                Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
                new InetSocketAddress(host, port),
                conf,
                new FifoRpcScheduler(conf, 1),
                true
        );
    }

    private String register(String zkHost, int zkPort, String zkPeer, String host, int port)
            throws IOException, KeeperException, InterruptedException {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        int sessionTimeout = 90000;
        CountDownLatch connSignal = new CountDownLatch(0);
        ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connSignal.countDown();
                }
            }
        });

        String path = zkPeer;
        if (zk.exists(path, false) == null) {
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zk.exists(path + "/hbaseid", false) == null) {
            String uuid = UUID.randomUUID().toString();
            zk.create(path + "/hbaseid", uuid.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        path += "/rs";
        if (zk.exists(path, false) == null) {
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        path += String.format("/%s,%d,%d", host, port, System.currentTimeMillis());

        return zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    private void run() {
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

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        String host = "macos";
        int port = 8813;
        String zkHost = "macos";
        int zkPort = 2181;
        String zkPeer = "/myPeer";

        ReplicateSvr svr = new ReplicateSvr("0", port);
        LOG.info(svr.register(zkHost, zkPort, zkPeer, host, port));
        svr.run();
    }
}
