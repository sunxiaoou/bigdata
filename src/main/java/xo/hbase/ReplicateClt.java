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

    private static final NioEventLoopGroup NIO = new NioEventLoopGroup();
    private static final byte[] CELL_BYTES = Bytes.toBytes("xyz");
    private static final KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);

    private static WALEdit createEdit(List<KeyValue> keyValues, boolean replay) {
        WALEdit edit = new WALEdit(keyValues.size(), replay);
        for (KeyValue keyValue : keyValues) {
            edit.add(keyValue);
        }
        return edit;
    }

    private static WAL.Entry createEntry() {
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
        Configuration conf = new Configuration();
        NettyRpcClientConfigHelper.setEventLoopConfig(conf, NIO, NioSocketChannel.class);
        AbstractRpcClient<?> client =
                new NettyRpcClient(conf, HConstants.CLUSTER_ID_DEFAULT, null, null);
        AdminProtos.AdminService.BlockingInterface admin =
                ReplicateService.newBlockingStub(client, new InetSocketAddress("localhost", 8813));

//        AdminProtos.ReplicateWALEntryRequest param = AdminProtos.ReplicateWALEntryRequest.newBuilder().build();
//        HBaseRpcControllerImpl controller =
//                new HBaseRpcControllerImpl(CellUtil.createCellScanner(ImmutableList.of(CELL)));
//        AdminProtos.ReplicateWALEntryResponse responseProto = admin.replicateWALEntry(controller, param);
//        LOG.info(responseProto.toString());

        List<WAL.Entry> entries = Arrays.asList(createEntry());
        ReplicationProtbufUtil.replicateWALEntry(
                admin,
                (WAL.Entry[]) entries.toArray(),
                "",
                new Path("hdfs://localhost:8020/hbase/data"),
                new Path("hdfs://localhost:8020/hbase/archive/data"),
                0);
        LOG.info("replicated");
    }
}