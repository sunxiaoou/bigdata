package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.ipc.*;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.zookeeper.ZkConnect;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class ReplicateSvr {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateSvr.class);

    private final ReplicateConfig config;
    private final BlockingQueue<Pair<List<AdminProtos.WALEntry>, CellScanner>> queue;
    private final NettyRpcServer rpcServer;
    private final AbstractSink sink;
    private final int port;
    private String ephemeral;
    private ZkConnect zkConnect;

    public ReplicateSvr() throws IOException {
        this.config = ReplicateConfig.getInstance();
        this.queue = new LinkedBlockingQueue<>();
        Configuration conf = new Configuration(HBaseConfiguration.create());
        BlockingService service = AdminProtos.AdminService.newReflectiveBlockingService(new ReplicateService(queue));
        this.rpcServer = new NettyRpcServer(null,
                config.getReplicateServerName(),
                Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
                new InetSocketAddress(0),
                conf,
                new FifoRpcScheduler(conf, 1),
                true
        );
        this.port = rpcServer.getListenerAddress().getPort();
        this.sink = new SinkFactory.Builder().withConfiguration(config).build();
    }

    private void register() throws IOException, KeeperException, InterruptedException {
        String connectString = String.format("%s:%d",
                config.getReplicateServerHost(),
                config.getReplicateServerQuorumPort());
        this.zkConnect = new ZkConnect(connectString);
        String root = config.getReplicateServerQuorumPath();
        zkConnect.createNode(root + "/hbaseid", UUID.randomUUID().toString().getBytes(), false);
        String node = String.format("%s/rs/%s,%d,%d",
                root,
                config.getReplicateServerHost(),
                port,
                System.currentTimeMillis());
        this.ephemeral = zkConnect.createNode(node, null, true);
        LOG.info("register RPC server ephemeral({}) to zK({})", ephemeral, connectString);
    }

    private void addPeer() throws IOException {
        HBase db = new HBase(
                config.getSourceHbaseQuorumHost(),
                config.getSourceHbaseQuorumPort(),
                config.getSourceHbaseQuorumPath());
        String rpcSvrZNode = config.getReplicateServerQuorumPath();
        String peer = rpcSvrZNode.substring(1);
        String key = String.format("%s:%d:%s",
                config.getReplicateServerQuorumHost(),
                config.getReplicateServerQuorumPort(),
                rpcSvrZNode);
        if ("table".equals(config.getSourceHbaseMapType())) {
            db.addPeer(peer, key, new ArrayList<>(config.getSourceHbaseMapTables().keySet()));
        } else if ("user".equals(config.getSourceHbaseMapType())) {
            db.addPeer(peer, key, config.getSourceHbaseMapNamespaces().keySet());
        } else {
            db.addPeer(peer, key);
        }
        db.close();
        LOG.info("peer({}) is ready on HBase({})", peer, key);
    }

    private void run() throws InterruptedException, IOException, KeeperException {
        register();
        addPeer();
        rpcServer.start();
        LOG.info("RPC server started on: " + rpcServer.getListenerAddress());

        // Keep the server running
        try {
            while (true) {
                Pair<List<AdminProtos.WALEntry>, CellScanner> pair = queue.take();
                List<AdminProtos.WALEntry> entryProtos = pair.getFirst();
                CellScanner cellScanner = pair.getSecond();
                if (sink != null) {
                    sink.put(entryProtos, cellScanner);
                } else {
                    for (WAL.Entry entry : AbstractSink.merge(entryProtos, cellScanner)) {
                        LOG.info(entry.toString());
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            rpcServer.stop();
            LOG.info("RPC server stopped.");
            zkConnect.close();
            LOG.info("unregistered RPC server ephemeral({})", ephemeral);
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ReplicateSvr svr = new ReplicateSvr();
        svr.run();
    }
}