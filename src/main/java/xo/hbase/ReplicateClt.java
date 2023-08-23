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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ReplicateClt {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateClt.class);

    AdminProtos.AdminService.BlockingInterface admin;

    public ReplicateClt(String host, int port) throws IOException {
        Configuration conf = new Configuration();
        NioEventLoopGroup nio = new NioEventLoopGroup();
        NettyRpcClientConfigHelper.setEventLoopConfig(conf, nio, NioSocketChannel.class);
        AbstractRpcClient<?> client =
                new NettyRpcClient(conf, HConstants.CLUSTER_ID_DEFAULT, null, null);
        admin = ReplicateService.newBlockingStub(client, new InetSocketAddress(host, port));
    }

    private void replicate(KeyValue kv) throws ServiceException {
        AdminProtos.ReplicateWALEntryRequest param = AdminProtos.ReplicateWALEntryRequest.newBuilder().build();
        HBaseRpcControllerImpl controller =
                new HBaseRpcControllerImpl(CellUtil.createCellScanner(ImmutableList.of(kv)));
        AdminProtos.ReplicateWALEntryResponse responseProto = admin.replicateWALEntry(controller, param);
        LOG.info(responseProto.toString());
    }

    private void replicate(WAL.Entry entry) throws IOException {
        List<WAL.Entry> entries = Arrays.asList(entry);
        ReplicationProtbufUtil.replicateWALEntry(
                admin,
                (WAL.Entry[]) entries.toArray(),
                "",
                new Path("hdfs://localhost:8020/hbase/data"),
                new Path("hdfs://localhost:8020/hbase/archive/data"),
                0);
        LOG.info("replicated");
    }

    static private WALEdit createEdit(List<KeyValue> keyValues, boolean replay) {
        WALEdit edit = new WALEdit(keyValues.size(), replay);
        for (KeyValue keyValue : keyValues) {
            edit.add(keyValue);
        }
        return edit;
    }

    static private WAL.Entry createEntry() {
        WALKeyImpl key = new WALKeyImpl(Bytes.toBytes("encode_region_name"), TableName.valueOf("manga:fruit"),
                42, System.currentTimeMillis(), HConstants.DEFAULT_CLUSTER_ID);
        List<KeyValue> keyValues = new ArrayList<>();
        keyValues.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("name"),
                new byte[30]));
        keyValues.add(new KeyValue(Bytes.toBytes("107"), Bytes.toBytes("cf"), Bytes.toBytes("price"),
                new byte[30]));
        WALEdit edit = createEdit(keyValues, false);
        return new WAL.Entry(key, edit);
    }

    public static void main(String[] args) throws IOException, ServiceException {
        ReplicateClt clt = new ReplicateClt("localhost", 8813);

        final byte[] CELL_BYTES = Bytes.toBytes("xyz");
        final KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);
        clt.replicate(CELL);
        clt.replicate(createEntry());
    }
}