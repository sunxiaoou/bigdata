package xo.hbase;

import xo.protobuf.EntryProto;
import xo.protobuf.ProtoBuf;
import xo.protobuf.ProtoBufFile;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;


public class ReplicateService implements AdminService.BlockingInterface {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateService.class);
    private static final String CLIENT_NAME = "ReplicateClt";

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
    public AdminProtos.GetRegionInfoResponse getRegionInfo(RpcController controller, AdminProtos.GetRegionInfoRequest request) {
        return null;
    }

    @Override
    public AdminProtos.GetStoreFileResponse getStoreFile(RpcController controller, AdminProtos.GetStoreFileRequest request) {
        return null;
    }

    @Override
    public AdminProtos.GetOnlineRegionResponse getOnlineRegion(RpcController controller, AdminProtos.GetOnlineRegionRequest request) {
        return null;
    }

    @Override
    public AdminProtos.OpenRegionResponse openRegion(RpcController controller, AdminProtos.OpenRegionRequest request) {
        return null;
    }

    @Override
    public AdminProtos.WarmupRegionResponse warmupRegion(RpcController controller, AdminProtos.WarmupRegionRequest request) {
        return null;
    }

    @Override
    public AdminProtos.CloseRegionResponse closeRegion(RpcController controller, AdminProtos.CloseRegionRequest request) {
        return null;
    }

    @Override
    public AdminProtos.FlushRegionResponse flushRegion(RpcController controller, AdminProtos.FlushRegionRequest request) {
        return null;
    }

    @Override
    public AdminProtos.CompactionSwitchResponse compactionSwitch(RpcController controller, AdminProtos.CompactionSwitchRequest request) {
        return null;
    }

    @Override
    public AdminProtos.CompactRegionResponse compactRegion(RpcController controller, AdminProtos.CompactRegionRequest request) {
        return null;
    }

    private void logCells(CellScanner scanner) {
        List<Cell> cells = new ArrayList<>();
        while (true) {
            try {
                if (!scanner.advance())
                    break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            cells.add(scanner.current());
        }
        LOG.info("cells: " + cells.toString());
    }

    private List<WAL.Entry> merge(List<WALEntry> entries, CellScanner scanner) {
        List<WAL.Entry> list = new ArrayList<>();
        for (WALEntry entry: entries) {
            WALProtos.WALKey walKey = entry.getKey();
            WALKeyImpl key = new WALKeyImpl(
                    walKey.getEncodedRegionName().toByteArray(),
                    TableName.valueOf(walKey.getTableName().toByteArray()),
                    walKey.getLogSequenceNumber(),
                    walKey.getWriteTime(),
                    null);
            int count = entry.getAssociatedCellCount();
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

    private void writeLog(List<WAL.Entry> entries) {
        String path = "target/entry.dat";
        ProtoBufFile.deleteFile(path);
        for (WAL.Entry entry: entries) {
            EntryProto.Entry entryProto = ProtoBuf.entry2Proto(entry);
            ProtoBufFile.append(path, entryProto);
        }
    }

    @Override
    public ReplicateWALEntryResponse replicateWALEntry(RpcController controller, ReplicateWALEntryRequest request) {
        String clusterId = request.getReplicationClusterId();
        LOG.info(clusterId);
        String sourceBaseNamespaceDirPath = request.getSourceBaseNamespaceDirPath();
        LOG.info(sourceBaseNamespaceDirPath);
        String sourceHFileArchiveDirPath = request.getSourceHFileArchiveDirPath();
        LOG.info(sourceHFileArchiveDirPath);
        List<WALEntry> entries = request.getEntryList();
        LOG.info("entries: " + entries.toString());
        CellScanner cellScanner = ((HBaseRpcController) controller).cellScanner();
        ((HBaseRpcController) controller).setCellScanner(null);
        if (CLIENT_NAME.equals(clusterId)) {
//            logCells(cellScanner);  // alternative with replication as cellScanner can only use once
            writeLog(merge(entries, cellScanner));
        } else {
            try {
                ReplicationSink sink = new ReplicationSink(HBaseConfiguration.create(), null);
                // replicate to HBase cluster described in resources/hbase-site.xml
                sink.replicateEntries(
                        entries,
                        cellScanner,
                        clusterId,
                        sourceBaseNamespaceDirPath,
                        sourceHFileArchiveDirPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ReplicateWALEntryResponse.Builder responseBuilder = ReplicateWALEntryResponse.newBuilder();
        // Add any response data to the response builder
        return responseBuilder.build();
    }

    @Override
    public ReplicateWALEntryResponse replay(RpcController controller, ReplicateWALEntryRequest request) {
        return null;
    }

    @Override
    public AdminProtos.ClearRegionBlockCacheResponse clearRegionBlockCache(RpcController controller, AdminProtos.ClearRegionBlockCacheRequest request) {
        return null;
    }

    @Override
    public QuotaProtos.GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(RpcController controller, QuotaProtos.GetSpaceQuotaSnapshotsRequest request) {
        return null;
    }

    @Override
    public AdminProtos.ExecuteProceduresResponse executeProcedures(RpcController controller, AdminProtos.ExecuteProceduresRequest request) {
        return null;
    }

    @Override
    public AdminProtos.SlowLogResponses getSlowLogResponses(RpcController controller, AdminProtos.SlowLogResponseRequest request) {
        return null;
    }

    @Override
    public AdminProtos.SlowLogResponses getLargeLogResponses(RpcController controller, AdminProtos.SlowLogResponseRequest request) {
        return null;
    }

    @Override
    public AdminProtos.ClearSlowLogResponses clearSlowLogsResponses(RpcController controller, AdminProtos.ClearSlowLogResponseRequest request) {
        return null;
    }

    @Override
    public HBaseProtos.LogEntry getLogEntries(RpcController controller, HBaseProtos.LogRequest request) {
        return null;
    }

    @Override
    public AdminProtos.StopServerResponse stopServer(RpcController controller, AdminProtos.StopServerRequest request) {
        return null;
    }

    @Override
    public AdminProtos.UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller, AdminProtos.UpdateFavoredNodesRequest request) {
        return null;
    }

    @Override
    public AdminProtos.UpdateConfigurationResponse updateConfiguration(RpcController controller, AdminProtos.UpdateConfigurationRequest request) {
        return null;
    }

    @Override
    public AdminProtos.GetRegionLoadResponse getRegionLoad(RpcController controller, AdminProtos.GetRegionLoadRequest request) {
        return null;
    }

    @Override
    public AdminProtos.ClearCompactionQueuesResponse clearCompactionQueues(RpcController controller, AdminProtos.ClearCompactionQueuesRequest request) {
        return null;
    }

    @Override
    public AdminProtos.RollWALWriterResponse rollWALWriter(RpcController controller, AdminProtos.RollWALWriterRequest request) {
        return null;
    }

    @Override
    public AdminProtos.GetServerInfoResponse getServerInfo(RpcController controller, AdminProtos.GetServerInfoRequest request) {
        return null;
    }
}


