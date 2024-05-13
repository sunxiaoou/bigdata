package xo.netty.backup;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Objects;

public class BackupClient {
    private static final Logger LOG = LoggerFactory.getLogger(BackupClient.class);

    private final String host;
    private final int port;
    private final String command;
    private final String ruleJson;

    public BackupClient(String host, int port, String command, String ruleJson) {
        this.host = host;
        this.port = port;
        this.command = command;
        this.ruleJson = ruleJson;
    }

    public void start() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(new InetSocketAddress(host, port))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch)
                        throws Exception {
                        ch.pipeline().addLast(new LoggingHandler(LogLevel.DEBUG));
                        ch.pipeline().addLast(new BackupCltHandler(command, ruleJson));
                    }
                });
            ChannelFuture f = b.connect().sync();
            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully().sync();
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println(
                    String.format("Usage: %s host port command jsonFile", BackupClient.class.getSimpleName()));
            System.exit(1);
        }

        LOG.info(System.getProperty("user.dir"));
        String ruleJson = Utils.readFile(
                Objects.requireNonNull(BackupClient.class.getClassLoader().getResource(args[3])).getPath());
        if (ruleJson == null) {
            System.exit(-1);
        }
        new BackupClient(args[0], Integer.parseInt(args[1]), args[2], ruleJson).start();
    }
}