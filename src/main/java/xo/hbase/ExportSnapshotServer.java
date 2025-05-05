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
            long rowCount = ExportSnapshotTask.runExport(request);
            String message;
            if (rowCount >= 0) {
                message = String.format("Export/Clone (%s) to (%s) with (%d) rows.",
                        request.snapshot, request.table, rowCount);
            } else {
                message = String.format("Export/Clone (%s) to (%s) failed.", request.snapshot, request.table);
            }
            LOG.info(message);
            ctx.writeAndFlush(mapper.writeValueAsString(new ExportResponse(rowCount >= 0, message, rowCount)));
        } catch (Exception e) {
            LOG.error("Exception: ", e);
            ExportResponse response =
                    new ExportResponse(false, "Exception: " + e.getMessage(), -1);
            ctx.writeAndFlush(mapper.writeValueAsString(response));
        }
    }
}

class ExportResponse {
    public boolean success;
    public String message;
    public long rowCount;

    public ExportResponse() {}

    public ExportResponse(boolean success, String message, long rowCount) {
        this.success = success;
        this.message = message;
        this.rowCount = rowCount;
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

    public static long runExport(ExportRequest req) {
        try {
            if (0 == db.exportSnapshot(req.snapshot, req.copyFrom, copyTo)) {
                db.cloneSnapshot(req.snapshot, req.table);
                LOG.info("snapshot({}) cloned to table({})", req.snapshot, req.table);
            }
            return db.countTableRows(req.table);
        } catch (Exception e) {
            LOG.error("ExportSnapshotTask: ", e);
            return -1;
        }
    }
}