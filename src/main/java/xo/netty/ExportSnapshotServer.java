package xo.netty;

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
import xo.hbase.HBase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExportSnapshotServer {
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public static void loginFromKeytab(String keytabPath, String principal) throws IOException {
        System.setProperty("java.security.krb5.conf", "hb_mrs/krb5.conf");
        Configuration conf = new Configuration();
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
        System.out.println("Kerberos login successful as " + principal);
    }

    public static void startTicketRenewal() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
                System.out.println("Kerberos ticket renewed successfully.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 1, 1, TimeUnit.HOURS);
    }

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
            System.out.println("ExportSnapshotServer started on port: " + port);
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. Kerberos Login
        loginFromKeytab("hb_mrs/loader_hive1.keytab", "loader_hive1@HADOOP.COM");

        // 2. Start Ticket Renewal Scheduler
        startTicketRenewal();

        // 3. Start Netty Server
        start(7412);
    }
}

class ExportSnapshotHandler extends SimpleChannelInboundHandler<String> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        System.out.println("Received: " + msg);
        try {
            ExportRequest request = mapper.readValue(msg, ExportRequest.class);
            boolean success = ExportSnapshotTask.runExport(request);
            ExportResponse response = new ExportResponse(success, success ? "Export completed." : "Export failed.");
            ctx.writeAndFlush(mapper.writeValueAsString(response));
        } catch (Exception e) {
            e.printStackTrace();
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
            System.out.println("ExportSnapshot completed with rc=" + rc);
            return rc == 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}