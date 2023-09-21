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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


public class ReplicateSvr {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateSvr.class);

    private static final String PROPERTIES_FILE = "replicate_svr.properties";
    private static final String REPLICATE_SERVER_NAME = "replicate.server.name";
    private static final String REPLICATE_SERVER_HOST = "replicate.server.host";
    private static final String REPLICATE_SERVER_PORT = "replicate.server.port";
    private static final String REPLICATE_SERVER_QUORUM_HOST = "replicate.server.quorum.host";
    private static final String REPLICATE_SERVER_QUORUM_PORT = "replicate.server.quorum.port";
    private static final String REPLICATE_SERVER_QUORUM_PATH = "replicate.server.quorum.path";

    private final Properties properties;
    private final NettyRpcServer rpcServer;

    public ReplicateSvr() throws IOException {
        this.properties = PropertyTool.loadProperties(PROPERTIES_FILE);
        Configuration conf = new Configuration(HBaseConfiguration.create());
        BlockingService service = null;
        try {
            service = AdminProtos.AdminService.newReflectiveBlockingService(new ReplicateService(properties));
        } catch (Exception e) {
            LOG.error("Unable to initialize dataSink. Make sure the sink implementation is in classpath"
                    + e.getMessage(), e);
            e.printStackTrace();
        }
        this.rpcServer = new NettyRpcServer(null,
                properties.getProperty(REPLICATE_SERVER_NAME),
                Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
                new InetSocketAddress("0", Integer.parseInt(properties.getProperty(REPLICATE_SERVER_PORT))),
                conf,
                new FifoRpcScheduler(conf, 1),
                true
        );
    }

    private String register()
            throws IOException, KeeperException, InterruptedException {
        String connectString = String.format("%s:%d",
                properties.getProperty(REPLICATE_SERVER_QUORUM_HOST),
                Integer.parseInt(properties.getProperty(REPLICATE_SERVER_QUORUM_PORT)));
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

        String path = properties.getProperty(REPLICATE_SERVER_QUORUM_PATH);
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
        path += String.format("/%s,%d,%d",
                properties.getProperty(REPLICATE_SERVER_HOST),
                Integer.parseInt(properties.getProperty(REPLICATE_SERVER_PORT)),
                System.currentTimeMillis());
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

    /**
     * hbase> add_peer 'macos', CLUSTER_KEY => "macos:2181:/myPeer"
     */
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ReplicateSvr svr = new ReplicateSvr();
        LOG.info(svr.register());
        svr.run();
    }
}
