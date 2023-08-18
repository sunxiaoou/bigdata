package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ipc.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;


public class RpcClt {
    private static final Logger LOG = LoggerFactory.getLogger(RpcTest.class);

    private static final NioEventLoopGroup NIO = new NioEventLoopGroup();
    private static final byte[] CELL_BYTES = Bytes.toBytes("xyz");
    private static final KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);

    public static void main(String[] args) throws IOException, ServiceException {
        Configuration conf = new Configuration();
        NettyRpcClientConfigHelper.setEventLoopConfig(conf, NIO, NioSocketChannel.class);
        AbstractRpcClient<?> client =
                new NettyRpcClient(conf, HConstants.CLUSTER_ID_DEFAULT, null, null);
        RpcServiceProtos.RpcService.BlockingInterface stub =
                RpcServiceBlockImpl.newBlockingStub(client, new InetSocketAddress("localhost", 8813));
        StringBuilder message = new StringBuilder(1200);
        for (int i = 0; i < 3; i ++) {
            message.append("hello.");
        }
        RpcProtos.EchoRequestProto param =
                RpcProtos.EchoRequestProto.newBuilder().setMessage(message.toString()).build();
        RpcProtos.EchoResponseProto responseProto =
                stub.echo(new HBaseRpcControllerImpl(CellUtil.createCellScanner(ImmutableList.of(CELL))), param);
        LOG.info("response:" + responseProto.toString());
    }
}