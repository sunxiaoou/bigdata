package xo.hbase;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExportSnapshotServer {
    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotServer.class);
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

//    public static void loginFromKeytab(String keytabPath, String principal) throws IOException {
//        System.setProperty("java.security.krb5.conf", "hb_mrs/krb5.conf");
//        Configuration conf = new Configuration();
//        UserGroupInformation.setConfiguration(conf);
//        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
//        System.out.println("Kerberos login successful as " + principal);
//    }

//    public static void startTicketRenewal() {
//        scheduler.scheduleAtFixedRate(() -> {
//            try {
//                UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
//                LOG.info("Kerberos ticket renewed successfully.");
//            } catch (Exception e) {
//                LOG.error("{}", e.getMessage());
//            }
//        }, 1, 1, TimeUnit.HOURS);
//    }

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
        // 1. Kerberos Login
//        loginFromKeytab("hb_mrs/loader_hive1.keytab", "loader_hive1@HADOOP.COM");

        // 2. Start Ticket Renewal Scheduler
//        startTicketRenewal();

        // 3. Start Netty Server
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
            String message = "Export " + request.snapshot + (success ? " completed." : " failed.");
            ExportResponse response = new ExportResponse(success, message);
            if (success) {
                LOG.info(message);
            } else {
                LOG.error(message);
            }
            if (success) {
                LOG.info("Export completed successfully.");
            } else {
                LOG.error("Export failed.");
            }
            ctx.writeAndFlush(mapper.writeValueAsString(response));
        } catch (Exception e) {
            LOG.error("Error processing request: {}", e.getMessage());
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

    private static final HBase db;
    static {
        try {
            db = new HBase("hb_mrs",
                    "zookeeper/hadoop.hadoop.com",
                    "loader_hive1@HADOOP.COM",
                    "hb_mrs/loader_hive1.keytab", true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean runExport(ExportRequest req) {
        try {
            int rc = db.exportSnapshot(req.snapshot, req.copyFrom, req.copyTo);
            if (rc == 0) {
                db.cloneSnapshot(req.snapshot, req.table);
                LOG.info("snapshot({}) cloned to table({})", req.snapshot, req.table);
            }
            return rc == 0;
        } catch (Exception e) {
            LOG.error("ExportSnapshotTask failed: {}", e.getMessage());
            return false;
        }
    }
}