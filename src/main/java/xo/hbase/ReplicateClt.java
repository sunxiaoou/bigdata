package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcClientConfigHelper;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;


public class ReplicateClt {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateClt.class);
    private static final String CLIENT_NAME = "ReplicateClt";

    AdminProtos.AdminService.BlockingInterface admin;

    public ReplicateClt(String host, int port) throws IOException {
        Configuration conf = new Configuration();
        NioEventLoopGroup nio = new NioEventLoopGroup();
        NettyRpcClientConfigHelper.setEventLoopConfig(conf, nio, NioSocketChannel.class);
        AbstractRpcClient<?> client =
                new NettyRpcClient(conf, HConstants.CLUSTER_ID_DEFAULT, null, null);
        admin = ReplicateService.newBlockingStub(client, new InetSocketAddress(host, port));
    }

    static private WALEdit createEdit(Map<String, Map<String, Map<String, String>>> map) {
        WALEdit edit = new WALEdit(map.size(), false);
        for (Map.Entry<String, Map<String, Map<String, String>>> rowEntry : map.entrySet()) {
            String row = rowEntry.getKey();
            Map<String, Map<String, String>> families = rowEntry.getValue();
            for (Map.Entry<String, Map<String, String>> familyEntry : families.entrySet()) {
                String columnFamily = familyEntry.getKey();
                Map<String, String> qualifiers = familyEntry.getValue();
                for (Map.Entry<String, String> qualifierEntry : qualifiers.entrySet()) {
                    String qualifier = qualifierEntry.getKey();
                    String value = qualifierEntry.getValue();
                    edit.add(new KeyValue(Bytes.toBytes(row), Bytes.toBytes(columnFamily),
                            Bytes.toBytes(qualifier), Bytes.toBytes(value)));
                }
            }
        }
        return edit;
    }

    static private WAL.Entry[] createEntries() {
        WALKeyImpl key = new WALKeyImpl(Bytes.toBytes("encode_region_name"), TableName.valueOf("manga:fruit"),
                42, System.currentTimeMillis(), HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = createEdit(Fruit.fruits());

        WALKeyImpl key2 = new WALKeyImpl(Bytes.toBytes("encode_region_name"), TableName.valueOf("manga:fruit"),
                43, System.currentTimeMillis(), HConstants.DEFAULT_CLUSTER_ID);
        Map<String, Map<String, Map<String, String>>> map = new HashMap<>();
        Pair<String, Map<String, Map<String, String>>> p =
                Fruit.fruit(new Triple<>(107, "🍐", (float) 115));
        map.put(p.getFirst(), p.getSecond());
        WALEdit edit2 = createEdit(map);

        return new WAL.Entry[] {
                new WAL.Entry(key, edit),
                new WAL.Entry(key2, edit2)
        };
    }

    private void replicate(KeyValue kv) throws ServiceException {
        AdminProtos.ReplicateWALEntryRequest param = AdminProtos
                .ReplicateWALEntryRequest
                .newBuilder()
                .setReplicationClusterId(CLIENT_NAME)
                .build();
        HBaseRpcControllerImpl controller =
                new HBaseRpcControllerImpl(CellUtil.createCellScanner(ImmutableList.of(kv)));
        AdminProtos.ReplicateWALEntryResponse responseProto = admin.replicateWALEntry(controller, param);
        LOG.info(responseProto.toString());
    }

    private void replicate(WAL.Entry[] entries) throws IOException {
        ReplicationProtbufUtil.replicateWALEntry(
                admin,
                entries,
                CLIENT_NAME,
                new Path("hdfs://localhost:8020/hbase/data"),
                new Path("hdfs://localhost:8020/hbase/archive/data"),
                0);
        LOG.info("replicated");
    }

    public static void main(String[] args) throws IOException, ServiceException {
        ReplicateClt clt = new ReplicateClt("localhost", 8813);

        final byte[] CELL_BYTES = Bytes.toBytes("xyz");
        final KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);
//        clt.replicate(CELL);
        clt.replicate(createEntries());
    }
}