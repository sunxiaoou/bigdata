package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.util.StringUtils;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


public class ReplicateSvr {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateSvr.class);
    private static final String TABLE_MAP_DELIMITER = ":";

    private final ReplicateConfig config;
    private final NettyRpcServer rpcServer;
    private final int port;

    public ReplicateSvr() throws IOException {
        this.config = ReplicateConfig.getInstance();
        Configuration conf = new Configuration(HBaseConfiguration.create());
        BlockingService service = null;
        try {
            service = AdminProtos.AdminService.newReflectiveBlockingService(new ReplicateService(config));
        } catch (Exception e) {
            LOG.error("Unable to initialize dataSink. Make sure the sink implementation is in classpath"
                    + e.getMessage(), e);
            e.printStackTrace();
        }
        this.rpcServer = new NettyRpcServer(null,
                config.getReplicateServerName(),
                Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
                new InetSocketAddress(0),
                conf,
                new FifoRpcScheduler(conf, 1),
                true
        );
        this.port = rpcServer.getListenerAddress().getPort();
    }

    private String register()
            throws IOException, KeeperException, InterruptedException {
        String connectString = String.format("%s:%d",
                config.getReplicateServerHost(),
                config.getReplicateServerQuorumPort());
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

        String path = config.getReplicateServerQuorumPath();
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
                config.getReplicateServerHost(),
                port,
                System.currentTimeMillis());
        return zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    private void addPeer() throws IOException {
        HBase db = new HBase(
                config.getSourceHbaseQuorumHost(),
                config.getSourceHbaseQuorumPort(),
                config.getSourceHbaseQuorumPath());
        String peer = config.getReplicateServerHost();
        int state = db.peerState(peer);
        if (state < 0) {
            String key = String.format("%s:%d:%s",
                    peer,
                    config.getReplicateServerQuorumPort(),
                    config.getReplicateServerQuorumPath());
            db.addPeer(peer, key, new ArrayList<>(config.getSinkKafkaTopicTableMap().keySet()));
            db.enablePeer(peer);
        } else if (state == 0) {
            db.enablePeer(peer);
        }
        db.close();
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
        svr.addPeer();
        svr.run();
    }
}
