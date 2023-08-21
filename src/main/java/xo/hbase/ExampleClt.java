package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcClientConfigHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;


public class ExampleClt {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleClt.class);

    private static final NioEventLoopGroup NIO = new NioEventLoopGroup();
    private static final byte[] CELL_BYTES = Bytes.toBytes("xyz");
    private static final KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);

    public static void main(String[] args) throws IOException, ServiceException {
        Configuration conf = new Configuration();
        NettyRpcClientConfigHelper.setEventLoopConfig(conf, NIO, NioSocketChannel.class);
        AbstractRpcClient<?> client =
                new NettyRpcClient(conf, HConstants.CLUSTER_ID_DEFAULT, null, null);
        ExampleProto.RowCountService.BlockingInterface stub =
                ExampleService.newBlockingStub(client, new InetSocketAddress("localhost", 8813));

        ExampleProto.CountRequest param = ExampleProto.CountRequest.newBuilder().build();
        HBaseRpcControllerImpl hBaseRpcController =
                new HBaseRpcControllerImpl(CellUtil.createCellScanner(ImmutableList.of(CELL)));
        ExampleProto.CountResponse responseProto = stub.getRowCount(hBaseRpcController, param);
        LOG.info("RowCount - " + responseProto.toString());
        responseProto = stub.getKeyValueCount(hBaseRpcController, param);
        LOG.info("KeyValueCount - " + responseProto.toString());
    }
}