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
    private final BlockingQueue<Pair<List<AdminProtos.WALEntry>, List<Cell>>> queue;
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
        String rpcSvrZNode = config.getReplicateServerRpcSvrZNode();
        zkConnect.createNode(rpcSvrZNode + "/hbaseid", UUID.randomUUID().toString().getBytes(), false);
        String node = String.format("%s/rs/%s,%d,%d",
                rpcSvrZNode,
                config.getReplicateServerHost(),
                port,
                System.currentTimeMillis());
        this.ephemeral = zkConnect.createNode(node, null, true);
        LOG.info("register RPC server ephemeral({}) to zK({})", ephemeral, connectString);
    }

    private void addPeer() throws IOException {
        HBase db = new HBase(
                config.getSourceHBaseQuorumHost(),
                config.getSourceHBaseQuorumPort(),
                config.getSourceHBaseQuorumPath());
        String rpcSvrZNode = config.getReplicateServerRpcSvrZNode();
        String peer = config.getReplicateServerPeer();
        String key = String.format("%s:%d:%s",
                config.getReplicateServerQuorumHost(),
                config.getReplicateServerQuorumPort(),
                rpcSvrZNode);
        if ("table".equals(config.getSourceHBaseMapType())) {
            db.addPeer(peer, key, new ArrayList<>(config.getSourceHBaseMapTables().keySet()), true);
        } else if ("user".equals(config.getSourceHBaseMapType())) {
            db.addPeer(peer, key, config.getSourceHBaseMapNamespaces().keySet(), true);
        } else {
            db.addPeer(peer, key, true);
        }
        db.close();
        LOG.info("peer({}) is ready on HBase({})", peer, key);
    }

    private void processCache(List<Pair<List<AdminProtos.WALEntry>, List<Cell>>> cache) {
        Iterator<Pair<List<AdminProtos.WALEntry>, List<Cell>>> iterator = cache.iterator();
        while (iterator.hasNext()) {
            Pair<List<AdminProtos.WALEntry>, List<Cell>> pair = iterator.next();
            List<AdminProtos.WALEntry> entryProtos = pair.getFirst();
            List<Cell> cells = pair.getSecond();
            CellScanner scanner = CellUtil.createCellScanner(cells.iterator());
            if (sink != null) {
                if (!sink.put(entryProtos, scanner)) {
                    break;
                }
                iterator.remove();
            } else {
                for (WAL.Entry entry : AbstractSink.merge(entryProtos, scanner)) {
                    LOG.info(entry.toString());
                }
            }
        }

        long count = cache.size();
        if (count > 0) {
            LOG.warn("There are {} item(s) in cache", count);
        }
    }

    private void run() throws InterruptedException, IOException, KeeperException {
        register();
        addPeer();
        rpcServer.start();
        LOG.info("RPC server started on: " + rpcServer.getListenerAddress());

        List<Pair<List<AdminProtos.WALEntry>, List<Cell>>> cache = new ArrayList<>();
        try {
            while (true) {
                cache.add(queue.take());
                processCache(cache);
            }
        } catch (InterruptedException e) {
            LOG.error("Failed to take entries from queue - {}", e.getMessage());
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