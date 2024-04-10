package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.ipc.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.zookeeper.ZkConnect;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


class Service implements AdminProtos.AdminService.BlockingInterface {
    private static final Logger LOG = LoggerFactory.getLogger(Service.class);

    private final BlockingQueue<Pair<List<AdminProtos.WALEntry>, CellScanner>> queue;

    public Service(BlockingQueue<Pair<List<AdminProtos.WALEntry>, CellScanner>> queue) {
        this.queue = queue;
    }

    public static AdminProtos.AdminService.BlockingInterface newBlockingStub(RpcClient client, InetSocketAddress addr)
            throws IOException {
        return newBlockingStub(client, addr, User.getCurrent());
    }

    public static AdminProtos.AdminService.BlockingInterface newBlockingStub(RpcClient client, InetSocketAddress addr, User user)
            throws IOException {
        BlockingRpcChannel channel = client.createBlockingRpcChannel(
                ServerName.valueOf(addr.getHostName(), addr.getPort(), System.currentTimeMillis()),
                user, 0);
        return AdminProtos.AdminService.newBlockingStub(channel);
    }

    @Override
    public AdminProtos.GetRegionInfoResponse getRegionInfo(RpcController controller, AdminProtos.GetRegionInfoRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.GetStoreFileResponse getStoreFile(RpcController controller, AdminProtos.GetStoreFileRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.GetOnlineRegionResponse getOnlineRegion(RpcController controller, AdminProtos.GetOnlineRegionRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.OpenRegionResponse openRegion(RpcController controller, AdminProtos.OpenRegionRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.WarmupRegionResponse warmupRegion(RpcController controller, AdminProtos.WarmupRegionRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.CloseRegionResponse closeRegion(RpcController controller, AdminProtos.CloseRegionRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.FlushRegionResponse flushRegion(RpcController controller, AdminProtos.FlushRegionRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.CompactionSwitchResponse compactionSwitch(RpcController controller, AdminProtos.CompactionSwitchRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.CompactRegionResponse compactRegion(RpcController controller, AdminProtos.CompactRegionRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(RpcController controller, AdminProtos.ReplicateWALEntryRequest request) {
        String clusterId = request.getReplicationClusterId();
        LOG.info(clusterId);
        String sourceBaseNamespaceDirPath = request.getSourceBaseNamespaceDirPath();
        LOG.info(sourceBaseNamespaceDirPath);
        String sourceHFileArchiveDirPath = request.getSourceHFileArchiveDirPath();
        LOG.info(sourceHFileArchiveDirPath);
        List<AdminProtos.WALEntry> entryProtos = request.getEntryList();
        LOG.debug("entryProtos: " + entryProtos.toString());
        CellScanner cellScanner = ((HBaseRpcController) controller).cellScanner();
        ((HBaseRpcController) controller).setCellScanner(null);
        try {
            queue.put(new Pair<>(entryProtos, cellScanner));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        AdminProtos.ReplicateWALEntryResponse.Builder responseBuilder = AdminProtos.ReplicateWALEntryResponse.newBuilder();
        // Add any response data to the response builder
        return responseBuilder.build();
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replay(RpcController controller, AdminProtos.ReplicateWALEntryRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.RollWALWriterResponse rollWALWriter(RpcController controller, AdminProtos.RollWALWriterRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.GetServerInfoResponse getServerInfo(RpcController controller, AdminProtos.GetServerInfoRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.StopServerResponse stopServer(RpcController controller, AdminProtos.StopServerRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller, AdminProtos.UpdateFavoredNodesRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.UpdateConfigurationResponse updateConfiguration(RpcController controller, AdminProtos.UpdateConfigurationRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.GetRegionLoadResponse getRegionLoad(RpcController controller, AdminProtos.GetRegionLoadRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.ClearCompactionQueuesResponse clearCompactionQueues(RpcController controller, AdminProtos.ClearCompactionQueuesRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.ClearRegionBlockCacheResponse clearRegionBlockCache(RpcController controller, AdminProtos.ClearRegionBlockCacheRequest request) throws ServiceException {
        return null;
    }

    @Override
    public QuotaProtos.GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(RpcController controller, QuotaProtos.GetSpaceQuotaSnapshotsRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.ExecuteProceduresResponse executeProcedures(RpcController controller, AdminProtos.ExecuteProceduresRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.SlowLogResponses getSlowLogResponses(RpcController controller, AdminProtos.SlowLogResponseRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.SlowLogResponses getLargeLogResponses(RpcController controller, AdminProtos.SlowLogResponseRequest request) throws ServiceException {
        return null;
    }

    @Override
    public AdminProtos.ClearSlowLogResponses clearSlowLogsResponses(RpcController controller, AdminProtos.ClearSlowLogResponseRequest request) throws ServiceException {
        return null;
    }

    @Override
    public HBaseProtos.LogEntry getLogEntries(RpcController controller, HBaseProtos.LogRequest request) throws ServiceException {
        return null;
    }
}

public class ReplicateSvr2 {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateSvr2.class);

    private final ReplicateConfig config;
    private final BlockingQueue<Pair<List<AdminProtos.WALEntry>, CellScanner>> queue;
    private final NettyRpcServer rpcServer;
    private final int port;
    private String ephemeral;
    private ZkConnect zkConnect;

    public ReplicateSvr2() throws IOException {
        this.config = ReplicateConfig.getInstance();
        this.queue = new LinkedBlockingQueue<>();
        Configuration conf = new Configuration(HBaseConfiguration.create());
        BlockingService service = AdminProtos.AdminService.newReflectiveBlockingService(new Service(queue));
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

    protected List<WAL.Entry> getEntries(List<AdminProtos.WALEntry> entryProtos, CellScanner scanner) {
        List<WAL.Entry> list = new ArrayList<>();
        for (AdminProtos.WALEntry entryProto: entryProtos) {
            WALProtos.WALKey keyProto = entryProto.getKey();
            HBaseProtos.UUID id = keyProto.getClusterIdsList().get(0);
            long sequence = keyProto.getLogSequenceNumber();
            WALKeyImpl key = new WALKeyImpl(
                    keyProto.getEncodedRegionName().toByteArray(),
                    TableName.valueOf(keyProto.getTableName().toByteArray()),
                    sequence,
                    keyProto.getWriteTime(),
                    new UUID(id.getMostSigBits(), id.getLeastSigBits()));
            int count = entryProto.getAssociatedCellCount();
            WALEdit edit = new WALEdit(count, false);
            for (int i = 0; i < count; i ++) {
                try {
                    if (!scanner.advance())
                        break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                edit.add(scanner.current());
            }
            list.add(new WAL.Entry(key, edit));
        }
        return list;
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
                for(WAL.Entry entry : getEntries(pair.getFirst(), pair.getSecond())) {
                    LOG.info(entry.toString());
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
        ReplicateSvr2 svr = new ReplicateSvr2();
        svr.run();
    }
}
