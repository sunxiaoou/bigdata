package xo.netty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class ExportSnapshotClient {

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        if (args.length != 4) {
            System.out.println("Usage: java ExportSnapshotClient <port> <snapshot> <copyFrom> <copyTo>");
            return;
        }

        String host = "localhost";
        int port = Integer.parseInt(args[0]);
        ExportRequest request = new ExportRequest(args[1], args[2], args[3]);

        CountDownLatch latch = new CountDownLatch(1);
        ObjectMapper mapper = new ObjectMapper();

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new StringDecoder(StandardCharsets.UTF_8));
                            p.addLast(new StringEncoder(StandardCharsets.UTF_8));
                            p.addLast(new SimpleChannelInboundHandler<String>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
                                    ExportResponse resp = mapper.readValue(msg, ExportResponse.class);
                                    System.out.println("Export result: " + (resp.success ? "✅ Success" : "❌ Failed") + " - " + resp.message);
                                    latch.countDown();
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    cause.printStackTrace();
                                    latch.countDown();
                                    ctx.close();
                                }
                            });
                        }
                    });

            Channel ch = b.connect(host, port).sync().channel();
            String json = mapper.writeValueAsString(request);
            ch.writeAndFlush(json);
            latch.await();
            ch.close();
        } finally {
            group.shutdownGracefully();
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