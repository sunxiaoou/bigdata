package xo.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ExportSnapshotClient {

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
        if (args.length != 5) {
            System.out.println("Usage: java ExportSnapshotClient <host> <port> <snapshot> <copyFrom> <copyTo>");
            return;
        }

        ExportSnapshotClient client = new ExportSnapshotClient(args[0], Integer.parseInt(args[1]));
        ExportRequest request = new ExportRequest(args[2], args[3], args[4]);
        try {
            ExportResponse resp = client.exportSnapshotSync(request, 300000); // 30s timeout
            System.out.println("Export result: " + (resp.success ? "✅ Success" : "❌ Failed") + " - " + resp.message);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}

class ExportRequest {
    public String snapshot;
    public String copyFrom;
    public String copyTo;

    public ExportRequest() {}

    public ExportRequest(String snapshot, String copyFrom, String copyTo) {
        this.snapshot = snapshot;
        this.copyFrom = copyFrom;
        this.copyTo = copyTo;
    }
}