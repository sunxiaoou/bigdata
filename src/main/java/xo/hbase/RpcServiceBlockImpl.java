package xo.hbase;

import xo.hbase.RpcServiceProtos.RpcService;
import xo.hbase.RpcServiceProtos.RpcService.BlockingInterface;
import xo.hbase.RpcServiceProtos.RpcService.Interface;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class RpcServiceBlockImpl implements BlockingInterface {

    public static BlockingInterface newBlockingStub(RpcClient client, InetSocketAddress addr)
            throws IOException {
        return newBlockingStub(client, addr, User.getCurrent());
    }

    public static BlockingInterface newBlockingStub(RpcClient client, InetSocketAddress addr, User user)
            throws IOException {
        BlockingRpcChannel blockingRpcChannel = client.createBlockingRpcChannel(
                ServerName.valueOf(addr.getHostName(), addr.getPort(), System.currentTimeMillis()),
                user, 0);
        return RpcService.newBlockingStub(blockingRpcChannel);
    }

    public static Interface newStub(RpcClient client, InetSocketAddress addr) throws IOException {
        RpcChannel rpcChannel = client.createRpcChannel(
                ServerName.valueOf(addr.getHostName(), addr.getPort(), System.currentTimeMillis()),
                User.getCurrent(), 0);
        return RpcService.newStub(rpcChannel);
    }

    @Override
    public RpcProtos.EmptyResponseProto ping(RpcController controller, RpcProtos.EmptyRequestProto request) {
        return RpcProtos.EmptyResponseProto.getDefaultInstance();
    }

    @Override
    public RpcProtos.EchoResponseProto echo(RpcController controller, RpcProtos.EchoRequestProto request)
            throws ServiceException {
        if (controller instanceof HBaseRpcController) {
            HBaseRpcController pcrc = (HBaseRpcController) controller;
            // If cells, scan them to check we are able to iterate what we were given and since this is an
            // echo, just put them back on the controller creating a new block. Tests our block building.
            CellScanner cellScanner = pcrc.cellScanner();
            List<Cell> list = null;
            if (cellScanner != null) {
                list = new ArrayList<>();
                try {
                    while (cellScanner.advance()) {
                        list.add(cellScanner.current());
                    }
                } catch (IOException e) {
                    throw new ServiceException(e);
                }
            }
            cellScanner = CellUtil.createCellScanner(list);
            pcrc.setCellScanner(cellScanner);
        }
        return RpcProtos.EchoResponseProto.newBuilder().setMessage(request.getMessage()).build();
    }

    @Override
    public RpcProtos.EmptyResponseProto error(RpcController controller, RpcProtos.EmptyRequestProto request)
            throws ServiceException {
        throw new ServiceException(new DoNotRetryIOException("server error!"));
    }

    @Override
    public RpcProtos.EmptyResponseProto pause(RpcController controller, RpcProtos.PauseRequestProto request) {
        Threads.sleepWithoutInterrupt(request.getMs());
        return RpcProtos.EmptyResponseProto.getDefaultInstance();
    }

    @Override
    public RpcProtos.AddrResponseProto addr(RpcController controller, RpcProtos.EmptyRequestProto request) {
        return RpcProtos.AddrResponseProto.newBuilder()
                .setAddr(RpcServer.getRemoteAddress().get().getHostAddress()).build();
    }
}
