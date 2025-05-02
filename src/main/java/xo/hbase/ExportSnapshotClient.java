package xo.hbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ExportSnapshotClient {
    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotClient.class);

    private final String host;
    private final int port;
    private final ObjectMapper mapper = new ObjectMapper();
    private final EventLoopGroup group = new NioEventLoopGroup();
    private Channel channel;

    public ExportSnapshotClient(String host, int port) throws InterruptedException {
        this.host = host;
        this.port = port;
        init();
    }

    private void init() throws InterruptedException {
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new StringDecoder(StandardCharsets.UTF_8));
                        p.addLast(new StringEncoder(StandardCharsets.UTF_8));
                    }
                });
        this.channel = b.connect(host, port).sync().channel();
    }

    public CompletableFuture<ExportResponse> exportSnapshot(ExportRequest request) throws Exception {
        CompletableFuture<ExportResponse> future = new CompletableFuture<>();

        channel.pipeline().addLast(new SimpleChannelInboundHandler<String>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
                ExportResponse resp = mapper.readValue(msg, ExportResponse.class);
                future.complete(resp);
                ctx.pipeline().remove(this);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                future.completeExceptionally(cause);
                ctx.close();
            }
        });

        String json = mapper.writeValueAsString(request);
        channel.writeAndFlush(json);
        return future;
    }

    public ExportResponse exportSnapshotSync(ExportRequest request, long timeoutMs) throws Exception {
        return exportSnapshot(request).get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
        group.shutdownGracefully();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            LOG.error("Usage: java ExportSnapshotClient <port>");
            return;
        }

        ExportSnapshotClient client = new ExportSnapshotClient("localhost", Integer.parseInt(args[0]));
        try {
            LOG.info(client.exportSnapshotSync(
                    new ExportRequest("manga:fruit",
                            "manga-fruit_250502",
                            "hdfs://ubuntu:8020/hbase"),
                    300000).message);
            LOG.info(client.exportSnapshotSync(
                    new ExportRequest("peTable",
                            "peTable_250502",
                            "hdfs://ubuntu:8020/hbase"),
                    300000).message);
        } catch (Exception e) {
            LOG.error("Error during export snapshot: ", e);
        } finally {
            client.close();
        }
    }
}

class ExportRequest {
    public String table;
    public String snapshot;
    public String copyFrom;

    public ExportRequest() {}

    public ExportRequest(String table, String snapshot, String copyFrom) {
        this.table = table;
        this.snapshot = snapshot;
        this.copyFrom = copyFrom;
    }
}