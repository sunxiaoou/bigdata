package xo.hbase;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ExportSnapshotServer {
    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotServer.class);

    public static void start(int port) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new StringDecoder(StandardCharsets.UTF_8));
                            p.addLast(new StringEncoder(StandardCharsets.UTF_8));
                            p.addLast(new ExportSnapshotHandler());
                        }
                    });

            ChannelFuture f = b.bind(port).sync();
            LOG.info("ExportSnapshotServer started on port: {}", port);
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        start(31415);
    }
}

class ExportSnapshotHandler extends SimpleChannelInboundHandler<String> {
    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        LOG.info("Received: {}", msg);
        try {
            ExportRequest request = mapper.readValue(msg, ExportRequest.class);
            boolean success = ExportSnapshotTask.runExport(request);
            String message = "Export/Clone " + request.snapshot + (success ? " completed." : " failed.");
            ExportResponse response = new ExportResponse(success, message);
            if (success) {
                LOG.info(message);
            } else {
                LOG.error(message);
            }
            ctx.writeAndFlush(mapper.writeValueAsString(response));
        } catch (Exception e) {
            LOG.error("Exception: ", e);
            ExportResponse response = new ExportResponse(false, "Exception: " + e.getMessage());
            ctx.writeAndFlush(mapper.writeValueAsString(response));
        }
    }
}

class ExportResponse {
    public boolean success;
    public String message;

    public ExportResponse() {}

    public ExportResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }
}

class ExportSnapshotTask {
    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotTask.class);

    private static final ReplicateConfig config = ReplicateConfig.getInstance();
    private static final HBase db;
    private static final String copyTo;
    static {
        try {
            db = new HBase(config.getTargetHBaseConfPath(),
                    config.getTargetZookeeperPrincipal(),
                    config.getTargetHBasePrincipal(),
                    config.getTargetHBaseKeytab(), true);
            copyTo = db.getProperty("hbase.rootdir");
            LOG.info("ExportSnapshotTask initialized with copyTo: {}", copyTo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean runExport(ExportRequest req) {
        try {
            int rc = db.exportSnapshot(req.snapshot, req.copyFrom, copyTo);
            if (rc == 0) {
                db.cloneSnapshot(req.snapshot, req.table);
                LOG.info("snapshot({}) cloned to table({})", req.snapshot, req.table);
            }
            return rc == 0;
        } catch (Exception e) {
            LOG.error("ExportSnapshotTask: ", e);
            return false;
        }
    }
}