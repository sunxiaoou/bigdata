package xo.hbase;


import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.security.User;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;


public class ExampleService implements ExampleProto.RowCountService.BlockingInterface {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleService.class);

    public static ExampleProto.RowCountService.BlockingInterface newBlockingStub(RpcClient client, InetSocketAddress addr)
            throws IOException {
        return newBlockingStub(client, addr, User.getCurrent());
    }

    public static ExampleProto.RowCountService.BlockingInterface newBlockingStub(RpcClient client, InetSocketAddress addr, User user)
            throws IOException {
        BlockingRpcChannel blockingRpcChannel = client.createBlockingRpcChannel(
                ServerName.valueOf(addr.getHostName(), addr.getPort(), System.currentTimeMillis()),
                user, 0);
        return ExampleProto.RowCountService.newBlockingStub(blockingRpcChannel);
    }

    @Override
    public ExampleProto.CountResponse getRowCount(RpcController controller, ExampleProto.CountRequest request) {
        LOG.info("getRowCount");
        return ExampleProto.CountResponse.newBuilder().setCount(42).build();
    }

    @Override
    public ExampleProto.CountResponse getKeyValueCount(RpcController controller, ExampleProto.CountRequest request) {
        LOG.info("getKeyValueCount");
        return ExampleProto.CountResponse.newBuilder().setCount(43).build();
    }
}
