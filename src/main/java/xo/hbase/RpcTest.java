package xo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.ipc.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;


public class RpcTest {
    private static final Logger LOG = LoggerFactory.getLogger(RpcTest.class);

    private static final NioEventLoopGroup NIO = new NioEventLoopGroup();
    private static final Configuration CONF = HBaseConfiguration.create();
    private static final BlockingService SERVICE =
            RpcServiceProtos.RpcService.newReflectiveBlockingService(new RpcServiceBlockImpl());
    private static final byte[] CELL_BYTES = Bytes.toBytes("xyz");
    private static final KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);

    static RpcServer createRpcServer(Server server,
                                     String name,
                                     List<RpcServer.BlockingServiceAndInterface> services,
                                     InetSocketAddress bindAddress,
                                     Configuration conf,
                                     RpcScheduler scheduler) throws IOException {
        return new NettyRpcServer(server, name, services, bindAddress, conf, scheduler, true);
    }

    static NettyRpcClient createRpcClient(Configuration conf) {
        NettyRpcClientConfigHelper.setEventLoopConfig(conf, NIO, NioSocketChannel.class);
        return new NettyRpcClient(conf, HConstants.CLUSTER_ID_DEFAULT, null, null);
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration(CONF);
        conf.setInt(RpcServer.MAX_REQUEST_SIZE, 1000);
        RpcServer rpcServer = createRpcServer(null, "testRpcServer",
                Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
                new InetSocketAddress("localhost", 0), conf, new FifoRpcScheduler(conf, 1));
        try (AbstractRpcClient<?> client = createRpcClient(conf)) {
            rpcServer.start();
            RpcServiceProtos.RpcService.BlockingInterface stub =
                    RpcServiceBlockImpl.newBlockingStub(client, rpcServer.getListenerAddress());
            StringBuilder message = new StringBuilder(1200);
            for (int i = 0; i < 2; i ++) {
                message.append("hello.");
            }
            // set total RPC size bigger than 100 bytes
            RpcProtos.EchoRequestProto param =
                    RpcProtos.EchoRequestProto.newBuilder().setMessage(message.toString()).build();
            RpcProtos.EchoResponseProto responseProto =
                    stub.echo(new HBaseRpcControllerImpl(CellUtil.createCellScanner(ImmutableList.of(CELL))), param);
            LOG.info("response:" + responseProto.toString());
//            fail("RPC should have failed because it exceeds max request size");
        } catch (ServiceException e) {
            LOG.info("Caught expected exception: " + e);
//            assertTrue(e.toString(),
//                    StringUtils.stringifyException(e).contains("RequestTooBigException"));
        } finally {
            rpcServer.stop();
        }
    }
}
